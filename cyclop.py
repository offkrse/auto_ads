#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import os
import time
import re
import random
import urllib.request
from urllib.parse import urlsplit, urlunsplit
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import logging
from logging.handlers import TimedRotatingFileHandler


import requests
from dateutil import tz
from filelock import FileLock
from dotenv import dotenv_values

# ============================ Пути/конфигурация ============================
VersionCyclop = "1.45"

GLOBAL_QUEUE_PATH = Path("/opt/auto_ads/data/global_queue.json")
USERS_ROOT = Path("/opt/auto_ads/users")
ENV_FILE = Path("/opt/auto_ads/.env")
LOGS_DIR = Path("/opt/auto_ads/logs")
LOG_FILE = LOGS_DIR / "cyclop.log"

API_BASE = os.getenv("VK_API_BASE", "https://ads.vk.com")

# Фиксированное смещение: от trigger_time ВСЕГДА вычитаем 4 часа
SERVER_SHIFT_HOURS = 4
MATCH_WINDOW_SECONDS = int("40")  # окно совпадения, сек
TARGET_SECOND = 20  # триггер в HH:MM:20

DEBUG_SAVE_PAYLOAD = os.getenv("DEBUG_SAVE_PAYLOAD", "0") == "1"
DEBUG_DRY_RUN = os.getenv("DEBUG_DRY_RUN", "0") == "1"
# Ретраи и таймауты
RETRY_MAX = int(os.getenv("RETRY_MAX", "7"))
RETRY_BACKOFF_BASE = float(os.getenv("RETRY_BACKOFF_BASE", "1.7"))
VK_HTTP_TIMEOUT = float(os.getenv("VK_HTTP_TIMEOUT", "60"))            # GET/прочее
VK_HTTP_TIMEOUT_POST = float(os.getenv("VK_HTTP_TIMEOUT_POST", "150")) # POST

CREO_STORAGE_ROOT = Path("/mnt/data/auto_ads_storage/video")
# Если сервер в UTC — дефолт уже UTC
LOCAL_TZ = tz.gettz(os.getenv("LOCAL_TZ", "UTC"))
UTC_TZ = tz.gettz("UTC")

BASE_DATE = datetime(2025, 7, 14)
BASE_NUMBER = 53
# ============================ Логирование ============================

def setup_logger() -> logging.Logger:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("auto_ads")

    # Если уже настроен — ничего не делаем
    if logger.handlers:
        return logger

    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logger.setLevel(level)

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Пишем ВСЕГДА в один файл без ротации
    file_handler = logging.FileHandler(str(LOG_FILE), encoding="utf-8")
    file_handler.setFormatter(fmt)
    file_handler.setLevel(level)

    # И дублируем в консоль (по желанию можно убрать)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(fmt)
    stream_handler.setLevel(level)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.propagate = False
    return logger

log = setup_logger()

# ============================ Пакеты / площадки ============================

def package_id_for_objective(obj: str) -> int:
    return {
        "socialengagement": 3127,
        "site_conversions": 3229,
    }.get(obj, 3127)

# Площадки (pads), разрешённые для пакета 3127 (примерный список)
PADS_FOR_PACKAGE: Dict[int, List[int]] = {
    3127: [102641, 1254386, 111756, 1265106, 1010345, 2243453],
}

# ======= ШАБЛОНЫ ДЛЯ НАЗВАНИЙ =======
AUD_TOKEN_RE = re.compile(r"\{\%([A-Z]+)((?:-[A-Z]+(?:\([^\)]*\))?)*)\%\}")

TARGET_CODES = {
    "socialengagement": ("СБ", "СообщениеБот"),
    "site_conversions": ("ПС", "ПереходСайт"),
    "leadads": ("ЛФ", "ЛидФорма"),
}

def _target_code(objective: str, long: bool = False) -> str:
    short, longv = TARGET_CODES.get(str(objective or "").strip(), ("", ""))
    return longv if long else short

def _gender_code(gender: str) -> str:
    g = (gender or "").replace(" ", "").lower()
    if g in ("male,female", "female,male"):
        return "МЖ"
    if g == "male":
        return "М"
    if g == "female":
        return "Ж"
    return ""

def _split_filter_spec(spec: str) -> List[Tuple[str, Optional[str]]]:
    # spec вроде "-CUT(2,10)-WS-IX(1)"; вернём [("CUT","2,10"),("WS",None),("IX","1")]
    out = []
    if not spec:
        return out
    parts = [p for p in spec.split("-") if p]
    for p in parts:
        if "(" in p and p.endswith(")"):
            name, args = p.split("(", 1)
            out.append((name.upper(), args[:-1]))
        else:
            out.append((p.upper(), None))
    return out

def _cut_1based(s: str, args: Optional[str]) -> str:
    if not s:
        return s
    if not args:
        return s
    try:
        if "," in args:
            i, j = args.split(",", 1)
            i = max(1, int(i.strip()))
            j = max(1, int(j.strip()))
            if i > j:
                i, j = j, i
            # 1-based inclusive → Python slice
            return s[i-1:j]
        else:
            n = max(0, int(args.strip()))
            return s[:n]
    except Exception:
        return s

def _apply_string_filters(s: str, filters: List[Tuple[str, Optional[str]]], *, wreg_words: Optional[List[str]] = None) -> str:
    out = s or ""
    for name, arg in filters:
        if name == "CUT":
            out = _cut_1based(out, arg)
        elif name == "WS":
            out = out.replace(" ", "")
        elif name == "WREG":
            out = _clean_wreg(out, wreg_words=wreg_words)
        # игнор остальных для строк
    return out.strip()

def _apply_list_filters(items: List[str], filters: List[Tuple[str, Optional[str]]]) -> List[str]:
    if not items:
        return []
    lst = [str(x or "") for x in items]

    # Прогоним CUT/WS ПОЭЛЕМЕНТНО (до IX)
    pre: List[str] = []
    for x in lst:
        x2 = x
        for name, arg in filters:
            if name == "CUT":
                x2 = _cut_1based(x2, arg)
            elif name == "WS":
                x2 = x2.replace(" ", "")
        pre.append(x2)

    # IX — компактируем «БАЗА + _число + (скобки)»
    do_ix = any(name == "IX" for name, _ in filters)
    if not do_ix:
        return pre

    # парсим IX(...) — пока значение (уровень) не используем, просто факт включения
    # сгруппируем по (base, paren_payload)
    pat = re.compile(r"^(.*?)(?:_?(\d+))?(?:\s*\(([^\)]*)\))?$")
    groups: Dict[str, Dict[str, Any]] = {}
    # key_base → {"nums": set(), "parens": set(), "base_text": "...", "examples": [...]}
    for x in pre:
        m = pat.match(x)
        if not m:
            # не распознали — оставим как есть отдельной группой
            k = ("__raw__", x)
            groups.setdefault(k, {"raw": []})["raw"].append(x)
            continue
        base = (m.group(1) or "").strip()
        num = m.group(2)
        par = (m.group(3) or "").strip()
        key = base  # группируем по «чистому» base
        g = groups.setdefault(key, {"nums": [], "parens": [], "base": base, "raw": []})
        if num and num.isdigit():
            if num not in g["nums"]:
                g["nums"].append(num)
        else:
            # нет номера — сохраняем «как есть» в raw для этой базы
            g["raw"].append(x)
        if par:
            if par not in g["parens"]:
                g["parens"].append(par)

    # собираем
    out: List[str] = []
    for key, g in groups.items():
        if key == ("__raw__",):
            out.extend(g["raw"])
            continue
        base = g.get("base", "")
        nums = g.get("nums", [])
        parens = g.get("parens", [])
        raws = g.get("raw", [])

        if nums:
            name = f"{base}{' ' if base and base[-1].isalnum() else ''}{','.join(nums)}"
        else:
            name = base.strip()
        if parens:
            name = f"{name} ({','.join(parens)})" if name else f"({','.join(parens)})"
        # добавим «сырые» элементы этой базы (если были без номера)
        acc = [name] if name else []
        acc.extend(raws)
        out.extend([x for x in acc if x])

    return out

def _clean_wreg(s: str, *, wreg_words: Optional[List[str]] = None) -> str:
    """
    Убираем «регулярные» служебные части:
    - короткие/длинные TARGET-коды в начале/конце;
    - фигурные маркеры вида {...} (на случай если кто-то оставил);
    - повисшие разделители ("-", "—", "_").
    Можно расширять через wreg_words — слова, которые надо вырезать.
    """
    out = s or ""
    # уберём развёрнутые маркеры {...}
    out = re.sub(r"\{[^\}]*\}", " ", out)

    # списки кодов
    short_codes = [v[0] for v in TARGET_CODES.values()]  # СБ/ПС/ЛФ
    long_codes  = [v[1] for v in TARGET_CODES.values()]  # СообщениеБот/...

    tokens = set(short_codes + long_codes + (wreg_words or []))
    # удалим токены как отдельные слова
    for t in sorted(tokens, key=len, reverse=True):
        out = re.sub(rf"(^|\s){re.escape(t)}(\s|$)", " ", out)

    # подчистим разделители, двойные пробелы
    out = re.sub(r"[\-–—_]{2,}", " ", out)
    out = re.sub(r"(^[\-–—_]+|[\-–—_]+$)", " ", out)
    out = re.sub(r"\s{2,}", " ", out)
    return out.strip()

def render_with_tokens(template: str,
                       *,
                       today_date,
                       objective: str = "",
                       age: str = "",
                       gender: str = "",
                       n: Optional[int] = None,
                       n_g: Optional[int] = None,
                       # контекст для «сложных» токенов
                       creo: str = "",
                       audience_names: Optional[List[str]] = None,
                       company_src: str = "",
                       group_src: str = "",
                       banner_src: str = "") -> str:
    """
    Универсальный рендер с поддержкой:
      {%DAY%}, {%N%}, {%N-G%}, {%TARGET%}, {%TARGET-L%}, {%AGE%}, {%GENDER%},
      {%CREO%},
      {%AUD%}[ -CUT(...) -WS -IX(...) ],
      {%COMPANY%}|{%GROUP%}|{%BANNER%} [ -CUT(...) -WS -WREG ].
    """
    if not template:
        return ""

    s = str(template)

    # простые подстановки (как раньше)
    s = s.replace("{%DAY%}", today_date.strftime("%d.%m"))
    s = s.replace("{%TARGET-L%}", _target_code(objective, long=True))
    s = s.replace("{%TARGET%}", _target_code(objective, long=False))
    s = s.replace("{%AGE%}", str(age or ""))
    s = s.replace("{%GENDER%}", _gender_code(gender))
    if "{%N%}" in s:
        s = s.replace("{%N%}", str(n if n is not None else 1))
    if "{%N-G%}" in s:
        s = s.replace("{%N-G%}", str(n_g if n_g is not None else 1))
    if "{%CREO%}" in s:
        s = s.replace("{%CREO%}", creo or "")

    # сложные токены с фильтрами
    def _repl(m: re.Match) -> str:
        name = (m.group(1) or "").upper()
        filt_raw = m.group(2) or ""
        filters = _split_filter_spec(filt_raw)

        if name == "AUD":
            items = list(audience_names or [])
            if not items:
                return ""
            items2 = _apply_list_filters(items, filters)
            return ", ".join([x for x in items2 if x])

        if name in ("COMPANY", "GROUP", "BANNER"):
            base = company_src if name == "COMPANY" else group_src if name == "GROUP" else banner_src
            # WREG должен знать слова для вырезания (например, текущие TARGET-коды)
            wreg_words = [_target_code(objective, long=False), _target_code(objective, long=True)]
            return _apply_string_filters(base, filters, wreg_words=wreg_words)

        # оставим нерешённые токены как есть (чтобы не ломать)
        return m.group(0)

    # прогоняем замену несколько раз, пока есть токены с фильтрами
    prev = None
    for _ in range(3):
        if s == prev:
            break
        prev = s
        s = AUD_TOKEN_RE.sub(_repl, s)

    return s.strip()

def render_name_tokens(
    template: str,
    *,
    today_date,          # date
    objective: str = "",
    age: str = "",
    gender: str = "",
    n: Optional[int] = None,      # {%N%}   — счётчик в пределах ГРУППЫ
    n_g: Optional[int] = None,    # {%N-G%} — счётчик в пределах КОМПАНИИ
) -> str:
    """
    Поддерживает плейсхолдеры:
      {%DAY%}       -> DD.MM (например, 30.11)
      {%N%}         -> локальная нумерация в группе
      {%N-G%}       -> глобальная нумерация в компании
      {%TARGET%}    -> СБ/ПС/ЛФ
      {%TARGET-L%}  -> СообщениеБот/ПереходСайт/ЛидФорма
      {%AGE%}       -> исходная строка age (например, "21-55")
      {%GENDER%}    -> МЖ/М/Ж
    """
    if not template:
        return ""

    s = str(template)

    # день
    s = s.replace("{%DAY%}", today_date.strftime("%d.%m"))

    # таргет
    s = s.replace("{%TARGET-L%}", _target_code(objective, long=True))
    s = s.replace("{%TARGET%}", _target_code(objective, long=False))

    # возраст/пол
    s = s.replace("{%AGE%}", str(age or ""))
    s = s.replace("{%GENDER%}", _gender_code(gender))

    # счётчики
    if "{%N%}" in s:
        s = s.replace("{%N%}", str(n if n is not None else 1))
    if "{%N-G%}" in s:
        s = s.replace("{%N-G%}", str(n_g if n_g is not None else 1))

    return s.strip()

# ============================ Утилиты ============================
class ApiHTTPError(Exception):
    def __init__(self, status: int, body: str, headers: Dict[str, str], url: str):
        super().__init__(f"HTTP {status} for {url}")
        self.status = status
        self.body = body
        self.headers = dict(headers or {})
        self.url = url

def _find_creative_meta(cabinet_id: str, media_id: int) -> Optional[Dict[str, Any]]:
    """
    Ищем файл вида:
      /mnt/data/auto_ads_storage/video/<cabinet_id>/<media_id>_*.json
    Возвращаем распарсенный JSON или None.
    """
    base_dir = CREO_STORAGE_ROOT / str(cabinet_id)
    if not base_dir.exists():
        return None

    prefix = f"{media_id}_"
    try:
        for p in base_dir.iterdir():
            if not p.is_file():
                continue
            if not p.name.startswith(prefix):
                continue
            if p.suffix != ".json":
                continue
            try:
                meta = load_json(p)
                log.info("Loaded creative meta for id=%s from %s", media_id, p)
                return meta
            except Exception as e:
                log.warning("Failed to read creative meta %s: %s", p, e)
                return None
    except Exception as e:
        log.warning("Iterdir failed for %s: %s", base_dir, e)

    return None

def sleep_to_next_tick(target_second: int = TARGET_SECOND, *, wake_early: float = 0.15) -> None:
    """
    Спим до ближайшего времени HH:MM:target_second (по LOCAL_TZ).
    wake_early — проснуться чуть раньше (в секундах), чтобы не проскочить из-за задержек.
    """
    now = datetime.now(LOCAL_TZ)

    next_tick = now.replace(second=target_second, microsecond=0)
    if now >= next_tick:
        next_tick += timedelta(minutes=1)

    sleep_s = (next_tick - now).total_seconds() - float(wake_early)
    # защита от отрицательных/слишком малых
    if sleep_s < 0.05:
        sleep_s = 0.05

    log.debug("Sleeping %.3fs until %s", sleep_s, next_tick.strftime("%Y-%m-%d %H:%M:%S %Z"))
    time.sleep(sleep_s)

def truncate_name(name: str, max_len: int = 200) -> str:
    """Обрезает название до max_len символов."""
    if not name:
        return name
    if len(name) <= max_len:
        return name
    return name[:max_len].rstrip()

def _build_priced_goal_company(company: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if (company.get("targetAction") or "") != "site_conversions":
        return None

    name = (company.get("siteAction") or "").strip()
    pixel = company.get("sitePixel")

    if not name or pixel is None or str(pixel).strip() == "":
        raise ValueError("site_conversions требует company.siteAction и company.sitePixel")

    # ВАЖНО: никаких None/null — только реально нужные поля
    return {
        "name": name,
        "source_id": int(pixel),
    }

def _build_priced_goal_group(company: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if (company.get("targetAction") or "") != "site_conversions":
        return None

    name = (company.get("siteAction") or "").strip()
    pixel = company.get("sitePixel")

    if not name or pixel is None or str(pixel).strip() == "":
        raise ValueError("site_conversions требует company.siteAction и company.sitePixel")

    return {
        "name": name,
        "source_id": int(pixel),
    }

def _add_group_with_optional_pads(payload_ad_groups: List[Dict[str, Any]],
                                 group_payload: Dict[str, Any],
                                 placements: List[int]) -> None:
    # pads должен быть ТОЛЬКО внутри targetings
    if placements:
        group_payload.setdefault("targetings", {})["pads"] = placements
    payload_ad_groups.append(group_payload)

def detect_image_media_kind(media_id: int, cabinet_id: Optional[str]) -> str:
    """
    По media_id и cabinet_id ищем JSON вида '<id>_*.json'
    и по полям width/height определяем ключ content.

    Правила:
      * квадрат 600x600 → "image_600x600"
      * формат 4:5 (например, 1080x1350) → "image_4_5"
      * иначе → "image_<width>x<height>" (запасной вариант)

    Если не нашли/не смогли — возвращаем стандартный image_600x600.
    """
    if not cabinet_id:
        return "image_600x600"

    meta = _find_creative_meta(str(cabinet_id), media_id)
    if not isinstance(meta, dict):
        return "image_600x600"

    width = meta.get("width")
    height = meta.get("height")

    # если вдруг наверху нет — пробуем брать из vk_response.variants.original
    if width is None or height is None:
        try:
            orig = (meta.get("vk_response") or {}).get("variants", {}).get("original") or {}
            width = width or orig.get("width")
            height = height or orig.get("height")
        except Exception:
            pass

    try:
        width = int(width)
        height = int(height)
    except Exception:
        return "image_600x600"

    if width <= 0 or height <= 0:
        return "image_600x600"

    # явно поддерживаем нужные нам форматы
    if width == 600 and height == 600:
        media_kind = "image_600x600"
    elif width == 607 and height == 1080:
        media_kind = "image_607x1080"
    # любое 4:5 (в том числе 1080x1350, 600x750 и т.п.)
    elif width * 5 == height * 4:
        media_kind = "image_4_5"
    else:
        # запасной вариант — как раньше
        media_kind = f"image_{width}x{height}"

    log.info(
        "Detected media_kind=%s for media_id=%s, cabinet_id=%s (width=%s, height=%s)",
        media_kind, media_id, cabinet_id, width, height
    )
    return media_kind

def _swap_image_600_to_1080(payload: Dict[str, Any]) -> int:
    """
    Во всех баннерах заменяет content.image_600x600 -> content.image_1080x1080, id переносим.
    Возвращает количество замен.
    """
    changed = 0
    try:
        for g in payload.get("ad_groups", []) or []:
            for b in g.get("banners", []) or []:
                content = (b.get("content") or {})
                if "image_600x600" in content and isinstance(content["image_600x600"], dict):
                    img_id = content["image_600x600"].get("id")
                    if img_id:
                        content.pop("image_600x600", None)
                        content["image_1080x1080"] = {"id": int(img_id)}
                        changed += 1
    except Exception:
        pass
    return changed

def save_text_blob(user_id: str, cabinet_id: str, name: str, text: str) -> Path:
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out_dir = USERS_ROOT / str(user_id) / "created_company" / str(cabinet_id)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{name}_{ts}.txt"
    with open(path, "w", encoding="utf-8") as f:
        f.write(text if text is not None else "")
    log.info("Saved text blob to %s (%d bytes)", path, len(text or ""))
    return path

def _dump_vk_validation(err_json: Dict[str, Any]) -> None:
    try:
        e = (err_json or {}).get("error") or {}
        fields = e.get("fields") or {}
        # campaigns → items (могут быть None)
        camps = (fields.get("campaigns") or {}).get("items") or []
        for ci, camp in enumerate(camps, start=1):
            if not isinstance(camp, dict):
                continue
            cb = (camp.get("fields") or {}).get("banners") or {}
            b_items = cb.get("items") or []
            for bi, b in enumerate(b_items, start=1):
                if not isinstance(b, dict):
                    log.error("VALIDATION: campaign[%d].banner[%d] is null/invalid", ci, bi)
                    continue
                bf = (b.get("fields") or {})
                for key in ("content", "textblocks", "targetings", "name"):
                    if key in bf:
                        node = bf.get(key)
                        log.error("VALIDATION: campaign[%d].banner[%d].%s -> %s",
                                  ci, bi, key, json.dumps(node, ensure_ascii=False))
                if b.get("code") or b.get("message"):
                    log.error("VALIDATION: campaign[%d].banner[%d] code=%s msg=%s",
                              ci, bi, b.get("code"), b.get("message"))
    except Exception as ex:
        log.error("VALIDATION: dump failed: %s", ex)

def compute_day_number(now_ref: datetime) -> int:
    """
    {день} = BASE_NUMBER + (сегодня - BASE_DATE).days
    Для «сегодня» берём серверное локальное время со смещением +4 часа,
    чтобы соответствовать логике триггера (не перескочить дату около полуночи).
    """
    # now_ref уже приходит как now_local
    adjusted = now_ref + timedelta(hours=SERVER_SHIFT_HOURS)
    return BASE_NUMBER + (adjusted.date() - BASE_DATE.date()).days
    
def expand_abstract_names(abs_names: List[str], day_number: int) -> List[str]:
    """Возвращает человеко-читаемые названия аудиторий из abstractAudiences,
    подставляя {день} -> day_number. Без запросов к API.
    """
    out: List[str] = []
    for raw in abs_names or []:
        name = str(raw).replace("{день}", str(day_number))
        if name:
            out.append(name)
    # уникализируем, сохраняя порядок
    return list(dict.fromkeys(out))

def resolve_abstract_audiences(tokens: List[str], names: List[str], day_number: int) -> List[int]:
    """
    Для каждого имени в names подставляем {день} -> day_number,
    зовём GET /api/v2/remarketing/segments.json?_name=<name>,
    берём items[].id (все найденные) и возвращаем список ID.
    """
    ids: List[int] = []
    for raw in names or []:
        name = str(raw).replace("{день}", str(day_number))
        from urllib.parse import quote
        endpoint = f"{API_BASE}/api/v2/remarketing/segments.json?_name={quote(name, safe='')}"
        try:
            resp = with_retries("GET", endpoint, tokens)
        except Exception as e:
            log.warning("abstractAudience '%s' lookup failed: %s", name, e)
            continue

        items = (resp or {}).get("items") or []
        found = [int(it["id"]) for it in items if isinstance(it, dict) and "id" in it]
        if found:
            log.info("abstractAudience '%s' -> segment_ids=%s", name, found)
            ids.extend(found)
        else:
            log.warning("abstractAudience '%s' not found", name)
    # уникализируем
    return list(dict.fromkeys(ids))

def save_debug_payload(user_id: str, cabinet_id: str, name: str, payload: Dict[str, Any]) -> None:
    if not DEBUG_SAVE_PAYLOAD:
        return
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out_dir = USERS_ROOT / str(user_id) / "created_company" / str(cabinet_id)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{name}_{ts}.payload.json"
    dump_json(path, payload)
    log.info("Saved debug payload to %s", path)
    
def load_tokens_from_envfile() -> None:
    """
    Загружаем ТОЛЬКО ключи VK_TOKEN_* из /opt/auto_ads/.env.
    Никакие другие переменные из .env в окружение не попадают.
    """
    if not ENV_FILE.exists():
        return
    try:
        values = dotenv_values(str(ENV_FILE))  # dict
        added = 0
        for k, v in (values or {}).items():
            if k and v and k.startswith("VK_TOKEN_"):
                os.environ[k] = v
                added += 1
        if added:
            log.debug("Loaded %d VK_TOKEN_* from %s", added, ENV_FILE)
    except Exception as e:
        log.warning("Failed to read tokens from %s: %s", ENV_FILE, e)

def load_telegram_bot_token() -> Optional[str]:
    """Загружает TELEGRAM_BOT_TOKEN из /opt/auto_ads/.env"""
    if not ENV_FILE.exists():
        return None
    try:
        values = dotenv_values(str(ENV_FILE))
        return values.get("TELEGRAM_BOT_TOKEN")
    except Exception as e:
        log.warning("Failed to read TELEGRAM_BOT_TOKEN from %s: %s", ENV_FILE, e)
        return None


def get_notification_settings(user_id: str, cabinet_id: str) -> Dict[str, Any]:
    """Читает настройки уведомлений из файла"""
    path = USERS_ROOT / str(user_id) / "settings" / str(cabinet_id) / "notifications.json"
    if not path.exists():
        return {"notifyOnError": True}  # по умолчанию включено
    try:
        return load_json(path)
    except Exception:
        return {"notifyOnError": True}


def send_telegram_notification(user_id: str, message: str) -> bool:
    """Отправляет уведомление в Telegram пользователю"""
    bot_token = load_telegram_bot_token()
    if not bot_token:
        log.warning("TELEGRAM_BOT_TOKEN not configured, skipping notification")
        return False
    
    try:
        # user_id в нашем случае = telegram_id
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": user_id,
            "text": message,
            "parse_mode": "HTML"
        }).encode("utf-8")
        
        req = urllib.request.Request(url, data=data, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status == 200:
                log.info("Telegram notification sent to user %s", user_id)
                return True
            else:
                log.warning("Telegram API returned status %s", resp.status)
                return False
    except Exception as e:
        log.warning("Failed to send Telegram notification: %s", e)
        return False


def notify_error_if_enabled(user_id: str, cabinet_id: str, preset_name: str, error_msg: str) -> None:
    """Отправляет уведомление об ошибке, если включено в настройках"""
    settings = get_notification_settings(user_id, cabinet_id)
    if not settings.get("notifyOnError", True):
        return
    
    message = (
        f"<b>Ошибка создания кампании: </b>"
        f"Пресет: {preset_name or 'Без имени'}\n"
    )
    send_telegram_notification(user_id, message)

def load_json(path: Path) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def dump_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def append_result_entry(user_id: str, cabinet_id: str, entry: Dict[str, Any]) -> None:
    """
    Добавляет запись в /opt/auto_ads/users/<user_id>/created_company/<cabinet_id>/created.json.
    Если файла нет — создаёт список с одной записью.
    """
    out_path = USERS_ROOT / str(user_id) / "created_company" / str(cabinet_id) / "created.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        try:
            data = load_json(out_path)
            if not isinstance(data, list):
                data = []
        except Exception:
            data = []
    else:
        data = []

    data.append(entry)
    dump_json(out_path, data)


def write_result_success(user_id: str, cabinet_id: str, preset_id: str, preset_name: str,
                         trigger_time: str, id_company: List[int]) -> None:
    entry = {
        "cabinet_id": str(cabinet_id),
        "date_time": datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S %Z"),
        "preset_id": str(preset_id),
        "preset_name": str(preset_name or ""),
        "trigger_time": str(trigger_time or ""),
        "status": "success",
        "text_error": "null",
        "code_error": "null",
        "id_company": id_company or [],
    }
    append_result_entry(user_id, cabinet_id, entry)


def write_result_error(user_id: str, cabinet_id: str, preset_id: str, preset_name: str,
                       trigger_time: str, human: str, tech: str) -> None:
    """
    Логирует ошибку в cyclop.log и отправляет уведомление в Telegram (если включено).
    НЕ записывает в created.json — только успехи туда попадают.
    """
    log.error(
        "PRESET ERROR | user=%s | cabinet=%s | preset=%s (%s) | trigger=%s | error=%s | tech=%s",
        user_id, cabinet_id, preset_id, preset_name, trigger_time, human, tech
    )
    
    # Отправляем уведомление в Telegram
    notify_error_if_enabled(user_id, cabinet_id, preset_name, human)

def extract_campaign_ids_from_resp(resp: Dict[str, Any]) -> List[int]:
    #Возвращаем список int без дублей (порядок сохраняем).
    ids: List[int] = []

    if isinstance(resp, dict):
        r = resp.get("response")
        if isinstance(r, dict):
            camps = r.get("campaigns")
            if isinstance(camps, list):
                ids.extend(int(x["id"]) for x in camps if isinstance(x, dict) and "id" in x)

        camps_flat = resp.get("campaigns")
        if isinstance(camps_flat, list):
            ids.extend(int(x["id"]) for x in camps_flat if isinstance(x, dict) and "id" in x)

    # уникализация
    return list(dict.fromkeys(ids))

def parse_hhmm(s: str) -> Tuple[int, int]:
    m = re.fullmatch(r"\s*(\d{1,2}):(\d{2})\s*", s or "")
    if not m:
        raise ValueError(f"Bad HH:MM: {s}")
    h = int(m.group(1))
    mi = int(m.group(2))
    if not (0 <= h < 24 and 0 <= mi < 60):
        raise ValueError(f"Out of range HH:MM: {s}")
    return h, mi

def compute_target_dt(trigger_hhmm: str, ref_now: datetime) -> datetime:
    """
    Берём часы/минуты из trigger_hhmm для СЕГОДНЯ в LOCAL_TZ (без смещений).
    """
    h, m = parse_hhmm(trigger_hhmm)
    return ref_now.replace(hour=h, minute=m, second=TARGET_SECOND, microsecond=0)

def check_trigger(trigger_hhmm: str, now_local: Optional[datetime] = None) -> Tuple[bool, Dict[str, str]]:
    if now_local is None:
        now_local = datetime.now(LOCAL_TZ)
    now_utc = datetime.now(UTC_TZ)

    # считаем в «сдвинутой» шкале: now(+4ч)
    adjusted_now = now_local + timedelta(hours=SERVER_SHIFT_HOURS)

    try:
        # ВАЖНО: target строим на ДАТЕ adjusted_now, чтобы при переходе через полночь дата тоже сместилась
        target = compute_target_dt(trigger_hhmm, adjusted_now)
    except Exception as e:
        log.error("Trigger parse error '%s': %s", trigger_hhmm, e)
        return False, {"error": str(e)}

    delta_sec = (adjusted_now - target).total_seconds()
    match = 0 <= delta_sec <= MATCH_WINDOW_SECONDS

    info = {
        "LOCAL_TZ": str(LOCAL_TZ),
        "TRIGGER": trigger_hhmm,
        "SERVER_SHIFT_HOURS": str(SERVER_SHIFT_HOURS),
        "NOW_LOCAL": now_local.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "NOW_UTC": now_utc.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "ADJUSTED_NOW": adjusted_now.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "TARGET_SHIFTED": target.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "DELTA_SEC": f"{delta_sec:.3f}",
        "WINDOW_SEC": str(MATCH_WINDOW_SECONDS),
        "MATCH": str(match),
    }

    # короткий лог
    sign = f"+{SERVER_SHIFT_HOURS}" if SERVER_SHIFT_HOURS >= 0 else str(SERVER_SHIFT_HOURS)
    log.info("trig=%s | %s | match=%s", trigger_hhmm, sign, match)
    return match, info

def as_int_list(maybe_csv_or_list) -> List[int]:
    if maybe_csv_or_list is None:
        return []
    if isinstance(maybe_csv_or_list, list):
        return [int(x) for x in maybe_csv_or_list]
    s = str(maybe_csv_or_list).strip()
    if not s:
        return []
    return [int(x) for x in s.split(",") if str(x).strip()]

def split_gender(gender_str: str) -> List[str]:
    if not gender_str:
        return []
    return [g.strip() for g in str(gender_str).split(",") if g.strip()]

def build_age_list(age_range_str: str) -> List[int]:
    m = re.fullmatch(r"\s*(\d{1,2})\s*-\s*(\d{1,2})\s*", age_range_str or "")
    ages = [0]
    if not m:
        return ages
    a = int(m.group(1)); b = int(m.group(2))
    if a > b: a, b = b, a
    ages.extend(list(range(a, b + 1)))
    return ages

def as_money_str(v) -> str:
    d = Decimal(str(v)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return f"{d:.2f}"

def compute_group_max_price(g: Dict[str, Any]) -> str:
    """
    bidStrategy:
      - "cap" -> max_price = "NNN.NN" из maxCpa
      - иначе -> "0.00"
    """
    strat = str(g.get("bidStrategy") or "min").strip().lower()
    if strat != "cap":
        return "0.00"

    v = g.get("maxCpa")
    if v is None or str(v).strip() == "":
        return "0.00"

    try:
        return as_money_str(v)
    except Exception:
        return "0.00"

def env_token(name: str) -> Optional[str]:
    return os.getenv(name)

def api_request(method: str, url: str, token: str, **kwargs) -> requests.Response:
    headers = kwargs.pop("headers", {})

    headers["Authorization"] = f"Bearer {token}"
    headers["Accept"] = "application/json"
    if method.upper() == "POST":
        headers.setdefault("Content-Type", "application/json; charset=utf-8")

    # таймауты отключены — не передаём параметр timeout вовсе
    log.debug("API %s %s | timeout=disabled", method, url)
    return requests.request(method, url, headers=headers, **kwargs)

def with_retries(method: str, url: str, tokens: List[str], **kwargs) -> Dict[str, Any]:
    last_error = None
    total_tokens = max(1, len(tokens))
    for attempt in range(1, RETRY_MAX + 1):
        token_idx = (attempt - 1) % total_tokens
        token_key_or_value = tokens[token_idx] if tokens else ""
        token_value = env_token(token_key_or_value) or token_key_or_value

        try:
            resp = api_request(method, url, token_value, **kwargs)
        except requests.RequestException as e:
            last_error = f"RequestException: {e}"
            base_sleep = RETRY_BACKOFF_BASE ** attempt
            jitter = random.uniform(0, 0.4 * base_sleep)
            sleep = min(60.0, base_sleep + jitter)
            log.warning("RequestException (attempt %s/%s): %s; sleep=%.2fs",
                        attempt, RETRY_MAX, e, sleep)
            time.sleep(sleep)
            continue

        # 429 — уважаем Retry-After
        if resp.status_code == 429:
            ra = resp.headers.get("Retry-After")
            try:
                sleep = float(ra)
            except Exception:
                base_sleep = RETRY_BACKOFF_BASE ** attempt
                jitter = random.uniform(0, 0.4 * base_sleep)
                sleep = min(60.0, base_sleep + jitter)
            body = resp.text or ""
            log.warning("HTTP 429 (attempt %s/%s). Retry-After=%.2fs | body_len=%d | body=%s",
                        attempt, RETRY_MAX, sleep, len(body), body)
            time.sleep(sleep)
            last_error = f"429: {body}"
            continue

        # 5xx — бэкофф
        if 500 <= resp.status_code < 600:
            body = resp.text or ""
            base_sleep = RETRY_BACKOFF_BASE ** attempt
            jitter = random.uniform(0, 0.4 * base_sleep)
            sleep = min(60.0, base_sleep + jitter)
            log.warning("HTTP %s (attempt %s/%s). Backoff %.2fs | body_len=%d | body=%s",
                        resp.status_code, attempt, RETRY_MAX, sleep, len(body), body)
            time.sleep(sleep)
            last_error = f"{resp.status_code}: {body}"
            continue

        # 4xx — без обрезок; validation/bad_request — кидаем сразу
        if 400 <= resp.status_code < 500:
            body = resp.text or ""
            # пробуем понять код ошибки
            try:
                err = resp.json()
                code = str(((err or {}).get("error") or {}).get("code") or "")
            except ValueError:
                err = None
                code = ""
            log.warning("HTTP %s 4xx on %s | body_len=%d | body=%s",
                        resp.status_code, url, len(body), body)
            if code in {"validation_failed", "bad_request"}:
                # НЕМЕДЛЕННО — никакого ретрая
                raise ApiHTTPError(resp.status_code, body, resp.headers, url)
            # иные 4xx — можно подретраить чуть-чуть
            base_sleep = RETRY_BACKOFF_BASE ** attempt
            jitter = random.uniform(0, 0.3 * base_sleep)
            sleep = min(30.0, base_sleep + jitter)
            time.sleep(sleep)
            last_error = f"{resp.status_code}: {body}"
            continue

        # 2xx — пробуем JSON; если не JSON — вернём raw
        try:
            j = resp.json()
            log.debug("API OK %s %s", resp.status_code, url)
            return j
        except ValueError:
            raw = {"raw": resp.text}
            log.debug("API OK (raw) %s %s | body_len=%d", resp.status_code, url, len(resp.text or ""))
            return raw

    # исчерпали попытки — кидаем с последним телом (без обрезки)
    raise ApiHTTPError(-1, last_error or "", {}, url)

def resolve_url_id(url_str: str, tokens: List[str]) -> int:
    from urllib.parse import quote
    q = quote(url_str, safe="")
    endpoint = f"{API_BASE}/api/v1/urls/?url={q}"
    payload = with_retries("GET", endpoint, tokens)
    if "id" not in payload:
        raise RuntimeError(f"No id in resolve_url response: {payload}")
    ad_id = int(payload["id"])
    log.info("Resolved URL '%s' -> id=%s", url_str, ad_id)
    return ad_id

def get_url_object_id(raw_url: str, objective: str, tokens: List[str]) -> int:
    """
    Возвращает id url-объекта для баннера/кампании.
    Для site_conversions используем /api/v2/urls.json (create_url_v2) и нормализацию.
    Для остальных целей — /api/v1/urls/?url=... (resolve_url_id).
    """
    if not raw_url or not str(raw_url).strip():
        raise ValueError("Empty URL")

    if objective == "site_conversions":
        norm = normalize_site_url(raw_url)
        return create_url_v2(norm, tokens)
    else:
        return resolve_url_id(raw_url, tokens)

def normalize_site_url(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return s

    # если только домен/без схемы
    if not re.match(r"^https?://", s, flags=re.I):
        s = "https://" + s.lstrip("/")

    parts = urlsplit(s)
    path = parts.path or "/"
    # ВАЖНО: для домена обеспечиваем слэш на конце (как в примере)
    if path == "/":
        path = "/"
    # если пользователь дал вообще без path - тоже "/"
    if not path:
        path = "/"

    norm = urlunsplit((parts.scheme, parts.netloc, path, parts.query, parts.fragment))

    # чтобы https://narod-zaem.ru -> https://narod-zaem.ru/
    if parts.netloc and (parts.path == "" or parts.path == "/"):
        norm = f"{parts.scheme}://{parts.netloc}/"
    if parts.netloc and norm == f"{parts.scheme}://{parts.netloc}":
        norm += "/"

    return norm

def create_url_v2(url_str: str, tokens: List[str]) -> int:
    endpoint = f"{API_BASE}/api/v2/urls.json"
    body = json.dumps({"url": url_str}, ensure_ascii=False).encode("utf-8")
    payload = with_retries("POST", endpoint, tokens, data=body)

    if isinstance(payload, dict):
        if "id" in payload:
            return int(payload["id"])
        # на всякий случай (если внезапно завернутый формат)
        if isinstance(payload.get("url"), dict) and "id" in payload["url"]:
            return int(payload["url"]["id"])

    raise RuntimeError(f"No id in /api/v2/urls.json response: {payload}")
    
# ============================ Баннер (строго 2 креатива) ============================

def pick_creative(ad: Dict[str, Any], cabinet_id: Optional[str] = None) -> Tuple[str, int]:
    """
    Возвращает (media_kind, media_id).

    Приоритет:
      * если есть imageIds → ('image_<width>x<height>', image_id),
        где размер читаем из /mnt/data/auto_ads_storage/video/<cabinet_id>/<id>_*.json;
        если не нашли — fallback на 'image_600x600';
      * иначе если есть videoIds → ('video_portrait_9_16_30s', video_id)
      * иначе ошибка.
    """
    imgs = ad.get("imageIds") or []
    vids = ad.get("videoIds") or []

    if isinstance(imgs, list) and imgs:
        try:
            img_id = int(imgs[0])
        except Exception:
            raise ValueError(f"imageIds[0] не число: {imgs[0]!r}")

        media_kind = detect_image_media_kind(img_id, cabinet_id)
        return media_kind, img_id

    if isinstance(vids, list) and vids:
        try:
            return "video_portrait_9_16_30s", int(vids[0])
        except Exception:
            raise ValueError(f"videoIds[0] не число: {vids[0]!r}")

    raise ValueError("Нет подходящего креатива: пустые imageIds и videoIds")

def make_banner_for_creative(url_id: int,
                             ad: Dict[str, Any],
                             *,
                             idx: int,
                             advertiser_info: str,
                             icon_id: int,
                             banner_name: str,
                             cta_text: str,
                             media_kind: str,
                             media_id: int,
                             objective: str = "") -> Dict[str, Any]:
    """
    Собирает баннер с переданным media_kind ('image_600x600' или 'video_portrait_9_16_30s')
    и media_id. Остальные поля — как раньше.
    """
    title = (ad.get("title") or "").strip()
    short = (ad.get("shortDescription") or "").strip()
    long_text = (ad.get("longDescription") or "").strip()
                                 
    if not advertiser_info:
        raise ValueError("Отсутствует advertiserInfo (about_company_115).")
    if not icon_id:
        raise ValueError("Отсутствует logoId (icon_256x256.id).")

    content = {"icon_256x256": {"id": int(icon_id)}}
    # Если это портретное видео 9:16 — кладём оба формата
    if media_kind == "video_portrait_9_16_30s":
        content["video_portrait_9_16_30s"] = {"id": int(media_id)}
        content["video_portrait_9_16_180s"] = {"id": int(media_id)}
    else:
        # Для картинок и любых других типов — как раньше
        content[media_kind] = {"id": int(media_id)}

    log.info("Banner #%d: icon_id=%s, %s=%s, name='%s', cta='%s'",
             idx, int(icon_id), media_kind, media_id, banner_name, cta_text)
    if objective == "site_conversions":
        textblocks = {
            "about_company_115": {"text": advertiser_info, "title": ""},
            "cta_sites_full": {"text": (cta_text or "visitSite"), "title": ""},
            "text_90": {"text": short, "title": ""},
            "text_long": {"text": long_text, "title": ""},
            "title_40_vkads": {"text": title, "title": ""},
        }
    else:
        textblocks = {
            "about_company_115": {"text": advertiser_info, "title": ""},
            "cta_community_vk": {"text": (cta_text or "visitSite"), "title": ""},
            "text_2000": {"text": short, "title": ""},
            "title_40_vkads": {"text": title, "title": ""},
        }
                                 
    return {
        "name": banner_name,
        "urls": {"primary": {"id": int(url_id)}},
        "content": content,
        "textblocks": textblocks
    }

# ============================ Построение payload ============================

def build_ad_plan_payload(preset: Dict[str, Any], ad_object_id: int, plan_index: int,
                          *, rendered_company_name: str = "",
                          rendered_group_names: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Собирает payload для POST /api/v2/ad_plans.json.
    Если переданы rendered_company_name / rendered_group_names — использует их,
    иначе рендерит по старой логике (для обратной совместимости).
    """
    company = preset["company"]
    groups = preset.get("groups", [])
    objective = company.get("targetAction", "socialengagement")

    today = (datetime.now(LOCAL_TZ) + timedelta(hours=SERVER_SHIFT_HOURS)).date()

    # Название компании
    if rendered_company_name:
        company_name = truncate_name(rendered_company_name, 200)
    else:
        company_name_tpl = (company.get("companyName") or "Авто кампания").strip() or "Авто кампания"
        company_name = truncate_name(
            render_name_tokens(company_name_tpl, today_date=today, objective=objective, n=1, n_g=1),
            200
        )

    package_id = package_id_for_objective(objective)
    start_date_str = today.isoformat()

    ad_groups_payload = []

    for g_idx, g in enumerate(groups, start=1):
        # Название группы
        if rendered_group_names and g_idx <= len(rendered_group_names):
            group_name = truncate_name(rendered_group_names[g_idx - 1], 200)
        else:
            group_name_tpl = (g.get("groupName") or f"Группа {g_idx}").strip()
            age_str = g.get("age", "")
            gender_str = g.get("gender", "")
            group_name = truncate_name(
                render_name_tokens(
                    group_name_tpl, today_date=today, objective=objective,
                    age=age_str, gender=gender_str, n=g_idx, n_g=g_idx
                ),
                200
            )

        age_str = g.get("age", "")
        gender_str = g.get("gender", "")
        placements = as_int_list(g.get("placements"))

        regions = as_int_list(g.get("regions"))
        genders = split_gender(gender_str)
        segments = as_int_list(g.get("audienceIds"))
        interests_list = as_int_list(g.get("interests"))
        age_list = build_age_list(age_str)

        targetings: Dict[str, Any] = {"geo": {"regions": regions}}
        if genders:
            targetings["sex"] = genders
        if segments:
            targetings["segments"] = segments
        if interests_list:
            targetings["interests"] = interests_list
        if age_list:
            targetings["age"] = {"age_list": age_list}
        if placements:
            targetings["pads"] = placements

        budget_day = int(g.get("budget") or 0)
        utm = g.get("utm") or "ref_source={{banner_id}}&ref={{campaign_id}}"

        banners_payload = [{"name": f"Объявление {g_idx}", "urls": {"primary": {"id": ad_object_id}}}]
        max_price = compute_group_max_price(g)

        group_payload = {
            "name": group_name,
            "targetings": targetings,
            "autobidding_mode": "max_goals",
            "budget_limit": None,
            "max_price": max_price,
            "budget_limit_day": budget_day,
            "date_start": start_date_str,
            "date_end": None,
            "age_restrictions": "18+",
            "package_id": package_id,
            "utm": utm,
            "banners": banners_payload,
        }

        pg_group = _build_priced_goal_group(company)
        if pg_group:
            group_payload["priced_goal"] = pg_group

        ad_groups_payload.append(group_payload)

    payload = {
        "name": company_name,
        "status": "active",
        "date_start": start_date_str,
        "date_end": None,
        "autobidding_mode": None,
        "budget_limit_day": None,
        "budget_limit": None,
        "max_price": 0,
        "objective": objective,
        "ad_object_id": ad_object_id,
        "ad_object_type": "url",
        "ad_groups": ad_groups_payload,
    }

    pg_company = _build_priced_goal_company(company)
    if pg_company:
        payload["priced_goal"] = pg_company

    return payload

# ============================ Создание плана ============================

def create_ad_plan(preset: Dict[str, Any], tokens: List[str], repeats: int,
                   user_id: str, cabinet_id: str,
                   preset_id: str, preset_name: str, trigger_time: str) -> List[Dict[str, Any]]:
    """
    При ошибке пишет файл error с понятным текстом и техническим кодом (append в created.json).
    При успехе — пишет success с массивом id_company из всех ответов.
    Возвращает список «сырого» ответа VK (для внутренней отладки).
    """
    company = preset["company"]
    url = company.get("url")
    if not url:
        write_result_error(user_id, cabinet_id, preset_id, preset_name, trigger_time,
                           "Отсутствует URL компании (company.url)", "Missing company.url")
        raise RuntimeError("Missing company.url")

    # company может содержать дефолты
    company_adv = (company.get("advertiserInfo") or "").strip()
    company_logo = company.get("logoId")

    company_name_tpl = (company.get("companyName") or "Авто кампания").strip() or "Авто кампания"
    objective = company.get("targetAction", "socialengagement")

    # Разрешаем URL -> id
    try:
        if objective == "site_conversions":
            norm_url = normalize_site_url(url)
            ad_object_id = create_url_v2(norm_url, tokens)
            log.info("site_conversions URL normalized: %s -> %s (ad_object_id=%s)", url, norm_url, ad_object_id)
        else:
            ad_object_id = resolve_url_id(url, tokens)
    except Exception as e:
        write_result_error(user_id, cabinet_id, preset_id, preset_name, trigger_time,
                           "Не удалось получить ad_object_id по URL", repr(e))
        raise

    # --- cache for URL -> id (чтобы не дергать API много раз) ---
    url_id_cache: Dict[str, int] = {}

    def cached_url_id(u: str) -> int:
        u = (u or "").strip()
        if not u:
            raise ValueError("Empty URL")
        key = normalize_site_url(u) if objective == "site_conversions" else u
        if key in url_id_cache:
            return url_id_cache[key]
        url_id_cache[key] = get_url_object_id(u, objective, tokens)
        return url_id_cache[key]

    # Копия пресета — будем расширять audienceIds из abstractAudiences
    preset_mut = json.loads(json.dumps(preset, ensure_ascii=False))
    groups = preset_mut.get("groups", []) or []
    ads = preset_mut.get("ads", []) or []

    if not groups:
        write_result_error(user_id, cabinet_id, preset_id, preset_name, trigger_time,
                           "В пресете отсутствуют группы", "groups is empty")
        raise RuntimeError("groups is empty")

    # 1) Обогащаем segments из abstractAudiences
    day_number = compute_day_number(datetime.now(LOCAL_TZ))
    for gi, g in enumerate(groups):
        abstract_names = g.get("abstractAudiences") or []
        if abstract_names:
            try:
                add_ids = resolve_abstract_audiences(tokens, abstract_names, day_number)
            except Exception as e:
                log.warning("resolve_abstract_audiences failed for group %d: %s", gi + 1, e)
                add_ids = []
            if add_ids:
                base_ids = g.get("audienceIds") or []
                merged = as_int_list(base_ids) + add_ids
                g["audienceIds"] = list(dict.fromkeys(merged))
                log.info("Group #%d segments extended by abstractAudiences: +%d id(s)", gi + 1, len(add_ids))

    # 2) Диагностика объявлений
    for i, ad in enumerate(ads):
        log.info("ads[%d] summary: adName=%r, button=%r, title=%r, videoIds=%r, logoId=%r, advertiserInfo=%r",
                 i, ad.get("adName"), ad.get("button"), ad.get("title"), ad.get("videoIds"),
                 ad.get("logoId") or company.get("logoId"),
                 ad.get("advertiserInfo") or company.get("advertiserInfo"))

    # === РЕНДЕРИНГ НАЗВАНИЙ С ПОЛНЫМИ ТОКЕНАМИ ===
    today = (datetime.now(LOCAL_TZ) + timedelta(hours=SERVER_SHIFT_HOURS)).date()
    day_number_for_names = compute_day_number(datetime.now(LOCAL_TZ))

    # Подготовим списки имён аудиторий для каждой группы
    group_audience_names: List[List[str]] = []
    for gi, g in enumerate(groups):
        aud_names = list(g.get("audienceNames") or [])
        abs_names = list(g.get("abstractAudiences") or [])
        if not aud_names and abs_names:
            aud_names = expand_abstract_names(abs_names, day_number_for_names)
        group_audience_names.append(aud_names)

    # Рендерим название компании с полными токенами
    first_group_auds = group_audience_names[0] if group_audience_names else []
    first_group = groups[0] if groups else {}
    first_ad = ads[0] if ads else {}
    first_creo = "Видео" if (first_ad.get("videoIds") or []) else "Статика"

    rendered_company_name = truncate_name(
        render_with_tokens(
            company_name_tpl,
            today_date=today,
            objective=objective,
            age=first_group.get("age", ""),
            gender=first_group.get("gender", ""),
            n=1,
            n_g=1,
            creo=first_creo,
            audience_names=first_group_auds,
            company_src=company_name_tpl,
            group_src=(first_group.get("groupName") or ""),
            banner_src=(first_ad.get("adName") or "")
        ),
        200
    )

    # Рендерим названия групп с полными токенами
    rendered_group_names: List[str] = []
    for gi, g in enumerate(groups):
        age_str = g.get("age", "")
        gender_str = g.get("gender", "")
        group_tpl = (g.get("groupName") or f"Группа {gi + 1}").strip()
        aud_names = group_audience_names[gi]

        ad = ads[gi] if gi < len(ads) else {}
        creo = "Видео" if (ad.get("videoIds") or []) else "Статика"

        g_name = truncate_name(
            render_with_tokens(
                group_tpl,
                today_date=today,
                objective=objective,
                age=age_str,
                gender=gender_str,
                n=gi + 1,
                n_g=gi + 1,
                creo=creo,
                audience_names=aud_names,
                company_src=company_name_tpl,
                group_src=group_tpl,
                banner_src=(ad.get("adName") or "")
            ),
            200
        )
        rendered_group_names.append(g_name)

    # 3) Баннеры 1:1 с группами + рендер имён с плейсхолдерами
    banners_by_group: List[Dict[str, Any]] = []
    company_counter = 0

    for gi in range(len(groups)):
        g = groups[gi]
        ad = ads[gi] if gi < len(ads) else None
        if not ad:
            write_result_error(user_id, cabinet_id, preset_id, preset_name, trigger_time,
                               f"Для группы #{gi + 1} отсутствует объявление в 'ads'",
                               f"ads[{gi}] is missing")
            raise RuntimeError(f"ads[{gi}] is missing")

        adv_info = (ad.get("advertiserInfo") or company_adv or "").strip()
        icon_id = ad.get("logoId") or company_logo
        if not adv_info:
            write_result_error(user_id, cabinet_id, preset_id, preset_name, trigger_time,
                               f"В объявлении #{gi + 1} отсутствует 'advertiserInfo' и не задан в company",
                               f"Missing ads[{gi}].advertiserInfo and company.advertiserInfo")
            raise RuntimeError("missing advertiserInfo")
        if not icon_id:
            write_result_error(user_id, cabinet_id, preset_id, preset_name, trigger_time,
                               f"В объявлении #{gi + 1} отсутствует 'logoId' и не задан в company",
                               f"Missing ads[{gi}].logoId and company.logoId")
            raise RuntimeError("missing logoId")

        # счётчики
        company_counter += 1

        # контексты
        age_str = g.get("age", "")
        gender_str = g.get("gender", "")
        aud_names = group_audience_names[gi]
        group_tpl = (g.get("groupName") or f"Группа {gi + 1}").strip()
        ad_tpl = (ad.get("adName") or f"Объявление {gi + 1}").strip()
        creo = "Видео" if (ad.get("videoIds") or []) else "Статика"

        # Рендерим название баннера с полными токенами
        banner_name = truncate_name(
            render_with_tokens(
                ad_tpl,
                today_date=today,
                objective=objective,
                age=age_str,
                gender=gender_str,
                n=1,
                n_g=company_counter,
                creo=creo,
                audience_names=aud_names,
                company_src=company_name_tpl,
                group_src=rendered_group_names[gi],
                banner_src=ad_tpl
            ),
            200
        )

        cta_text = (ad.get("button") or "visitSite").strip()

        # --- bannerUrl -> url_id для баннера (если нет — используем company.url) ---
        banner_url_raw = (
            (ad.get("bannerUrl") or "").strip()
            or (company.get("bannerUrl") or "").strip()
            or (preset.get("bannerUrl") or "").strip()
        )

        banner_url_id = ad_object_id
        if banner_url_raw:
            try:
                banner_url_id = cached_url_id(banner_url_raw)
                log.info("bannerUrl resolved: %s -> %s", banner_url_raw, banner_url_id)
            except Exception as e:
                write_result_error(
                    user_id, cabinet_id, preset_id, preset_name, trigger_time,
                    "Не удалось получить id по bannerUrl", repr(e)
                )
                raise

        try:
            media_kind, media_id = pick_creative(ad, cabinet_id=str(cabinet_id))

            banner = make_banner_for_creative(
                banner_url_id, ad, idx=gi + 1,
                advertiser_info=adv_info, icon_id=int(icon_id),
                banner_name=banner_name, cta_text=cta_text,
                media_kind=media_kind, media_id=media_id,
                objective=objective
            )
            banners_by_group.append(banner)
            log.info("Собран баннер #%d для группы '%s' (name='%s')",
                     gi + 1, rendered_group_names[gi], banner_name)
        except Exception as e:
            write_result_error(user_id, cabinet_id, preset_id, preset_name, trigger_time,
                               "Ошибка сборки баннера", repr(e))
            raise

    results = []
    endpoint = f"{API_BASE}/api/v2/ad_plans.json"

    for i in range(1, repeats + 1):
        base_payload = build_ad_plan_payload(
            preset_mut, ad_object_id, i,
            rendered_company_name=rendered_company_name,
            rendered_group_names=rendered_group_names
        )
        payload_try = json.loads(json.dumps(base_payload, ensure_ascii=False))

        # подставляем баннеры в группы
        ad_groups = payload_try.get("ad_groups", [])
        for gi, g in enumerate(ad_groups):
            g["banners"] = [banners_by_group[gi]]

        # sanity-лог
        for gi, g in enumerate(ad_groups, start=1):
            c = (g.get("banners") or [{}])[0].get("content") or {}
            ico = ((c.get("icon_256x256") or {}).get("id"))

            media_key = None
            media_id = None

            for k, v in c.items():
                if not isinstance(v, dict):
                    continue
                if k.startswith("image_") or k.startswith("video_"):
                    media_key = k
                    media_id = v.get("id")
                    break

            log.info(
                "Group %d will send icon_id=%s, %s=%s",
                gi, ico, media_key or "media", media_id
            )

        # DEBUG: сохранить payload (если включено)
        save_debug_payload(user_id, cabinet_id, f"ad_plan_{i}", payload_try)

        if DEBUG_DRY_RUN:
            log.warning("[DRY RUN] Skipping POST /api/v2/ad_plans.json (no request sent).")
            results.append({"request": payload_try, "response": {"response": {"campaigns": []}}})
            continue

        body_bytes = json.dumps(payload_try, ensure_ascii=False).encode("utf-8")
        log.info(
            "POST ad_plan (%d/%d): groups=%d, banners_per_group=1, payload=%.1f KB, timeout=disabled",
            i, repeats, len(ad_groups), len(body_bytes) / 1024.0
        )

        try:
            resp = with_retries("POST", endpoint, tokens, data=body_bytes)
            results.append({"request": payload_try, "response": resp})
            log.info("POST OK (%d/%d).", i, repeats)
        except ApiHTTPError as e:
            err_path = save_text_blob(user_id, cabinet_id, "vk_error_ad_plan_post", e.body)
            log.error("VK HTTP error %s on %s. Full body saved to: %s (len=%d)",
                      e.status, e.url, err_path, len(e.body or ""))

            try:
                err_json = json.loads(e.body)
                _dump_vk_validation(err_json)
            except Exception as ex:
                log.error("VALIDATION: non-JSON or parse failed: %s", ex)

            write_result_error(user_id, cabinet_id, preset_id, preset_name, trigger_time,
                               "Ошибка создания кампании в VK Ads", f"HTTP {e.status} {e.url}")
            raise

    # Собираем id кампаний со всех успешных ответов
    try:
        all_ids: List[int] = []
        for r in results:
            resp = r.get("response") or {}
            ids = extract_campaign_ids_from_resp(resp)
            if ids:
                all_ids.extend(ids)
        id_company = list(dict.fromkeys(all_ids))
    except Exception as e:
        write_result_error(user_id, cabinet_id, preset_id, preset_name, trigger_time,
                           "Не удалось распарсить ответ VK Ads", repr(e))
        raise

    write_result_success(user_id, cabinet_id, preset_id, preset_name, trigger_time, id_company)
    return results

# ============================ FAST ПЛАН ============================
def create_ad_plan_fast(preset: Dict[str, Any], tokens: List[str], repeats: int,
                        user_id: str, cabinet_id: str,
                        preset_id: str, preset_name: str, trigger_time: str) -> List[Dict[str, Any]]:
    """
    FAST: на каждый контейнер → отдельная группа; в каждой группе создаём баннер
    под КАЖДЫЙ креатив из ads[*].videoIds и ads[*].imageIds.
    Если контейнеров нет — используем аудитории самой группы.
    """
    company = preset["company"]
    url = company.get("url")
    if not url:
        write_result_error(user_id, cabinet_id, preset_id, preset_name, trigger_time,
                           "Отсутствует URL компании (company.url)", "Missing company.url")
        raise RuntimeError("Missing company.url")

    company_adv = (company.get("advertiserInfo") or "").strip()
    company_logo = company.get("logoId")
    company_name_tpl = (company.get("companyName") or "Авто кампания").strip() or "Авто кампания"
    objective = company.get("targetAction", "socialengagement")

    try:
        if objective == "site_conversions":
            norm_url = normalize_site_url(url)
            ad_object_id = create_url_v2(norm_url, tokens)
            log.info("site_conversions URL normalized: %s -> %s (ad_object_id=%s)", url, norm_url, ad_object_id)
        else:
            ad_object_id = resolve_url_id(url, tokens)
    except Exception as e:
        write_result_error(user_id, cabinet_id, preset_id, preset_name, trigger_time,
                           "Не удалось получить ad_object_id по URL", repr(e))
        raise

    # --- cache for URL -> id ---
    url_id_cache: Dict[str, int] = {}

    def cached_url_id(u: str) -> int:
        u = (u or "").strip()
        if not u:
            raise ValueError("Empty URL")
        key = normalize_site_url(u) if objective == "site_conversions" else u
        if key in url_id_cache:
            return url_id_cache[key]
        url_id_cache[key] = get_url_object_id(u, objective, tokens)
        return url_id_cache[key]

    preset_mut = json.loads(json.dumps(preset, ensure_ascii=False))
    groups = preset_mut.get("groups", []) or []
    ads = preset_mut.get("ads", []) or []
    if not groups:
        write_result_error(user_id, cabinet_id, preset_id, preset_name, trigger_time,
                           "В пресете отсутствуют группы", "groups is empty")
        raise RuntimeError("groups is empty")
    if not ads:
        write_result_error(user_id, cabinet_id, preset_id, preset_name, trigger_time,
                           "В пресете отсутствуют объявления", "ads is empty")
        raise RuntimeError("ads is empty")

    today = (datetime.now(LOCAL_TZ) + timedelta(hours=SERVER_SHIFT_HOURS)).date()
    day_number_for_names = compute_day_number(datetime.now(LOCAL_TZ))

    # === РЕНДЕРИМ НАЗВАНИЕ КОМПАНИИ С ПОЛНЫМИ ТОКЕНАМИ ===
    first_group = groups[0] if groups else {}
    first_ad = ads[0] if ads else {}
    first_creo = "Видео" if (first_ad.get("videoIds") or []) else "Статика"

    first_aud_names = list(first_group.get("audienceNames") or [])
    first_abs_names = list(first_group.get("abstractAudiences") or [])
    if not first_aud_names and first_abs_names:
        first_aud_names = expand_abstract_names(first_abs_names, day_number_for_names)

    rendered_company_name = truncate_name(
        render_with_tokens(
            company_name_tpl,
            today_date=today,
            objective=objective,
            age=first_group.get("age", ""),
            gender=first_group.get("gender", ""),
            n=1,
            n_g=1,
            creo=first_creo,
            audience_names=first_aud_names,
            company_src=company_name_tpl,
            group_src=(first_group.get("groupName") or ""),
            banner_src=(first_ad.get("adName") or "")
        ),
        200
    )

    base_payload = build_ad_plan_payload(preset_mut, ad_object_id, 1, rendered_company_name=rendered_company_name)
    payload_try = json.loads(json.dumps(base_payload, ensure_ascii=False))
    payload_try["ad_groups"] = []  # перезапишем полностью

    pkg_id = package_id_for_objective(objective)

    # по каждой группе fast-пресета
    for g_idx, g in enumerate(groups, start=1):
        group_tpl = (g.get("groupName") or f"Группа {g_idx}").strip()
        regions = as_int_list(g.get("regions"))
        genders = split_gender(g.get("gender", ""))
        age_str = g.get("age", "")
        age_list = build_age_list(age_str)
        placements = as_int_list(g.get("placements"))

        base_segments = as_int_list(g.get("audienceIds"))
        base_abstract = g.get("abstractAudiences") or []
        containers = g.get("containers") or []
        group_aud_names = g.get("audienceNames") or []

        # если контейнеров нет — один виртуальный контейнер из самой группы
        if not containers:
            containers = [{
                "id": "virt",
                "name": "Контейнер",
                "audienceIds": base_segments,
                "audienceNames": group_aud_names,
                "abstractAudiences": base_abstract,
            }]

        # каждый контейнер → отдельная группа
        for ci, cont in enumerate(containers, start=1):
            seg_ids = as_int_list(base_segments) + as_int_list(cont.get("audienceIds"))
            abs_names = (base_abstract or []) + (cont.get("abstractAudiences") or [])
            if abs_names:
                try:
                    day_number = compute_day_number(datetime.now(LOCAL_TZ))
                    seg_ids += resolve_abstract_audiences(tokens, abs_names, day_number)
                except Exception as e:
                    log.warning("FAST: resolve_abstract_audiences failed: %s", e)
            seg_ids = list(dict.fromkeys(int(x) for x in seg_ids))

            aud_names = list(group_aud_names or []) + list(cont.get("audienceNames") or [])
            if not aud_names and abs_names:
                aud_names = expand_abstract_names(abs_names, day_number_for_names)

            targetings: Dict[str, Any] = {"geo": {"regions": regions}}
            if genders:
                targetings["sex"] = genders
            if seg_ids:
                targetings["segments"] = seg_ids
            if age_list:
                targetings["age"] = {"age_list": age_list}
            if placements:
                targetings["pads"] = placements

            budget_day = int(g.get("budget") or 0)
            utm = g.get("utm") or "ref_source={{banner_id}}&ref={{campaign_id}}"
            max_price_for_group = compute_group_max_price(g)

            # КАЖДЫЙ креатив → отдельная группа с ОДНИМ баннером
            made_any = False

            for ai, ad in enumerate(ads):
                adv_info = (ad.get("advertiserInfo") or company_adv or "").strip()
                icon_id = ad.get("logoId") or company_logo
                if not adv_info or not icon_id:
                    write_result_error(user_id, cabinet_id, preset_id, preset_name, trigger_time,
                                       f"FAST: у ads[{ai}] нет advertiserInfo/logoId",
                                       f"fast missing fields in ads[{ai}]")
                    raise RuntimeError("fast missing fields")

                ad_tpl = (ad.get("adName") or f"Объявление {ai + 1}").strip()
                btn = (ad.get("button") or "visitSite").strip()

                # --- bannerUrl -> url_id для всех баннеров этого ad ---
                banner_url_raw = (
                    (ad.get("bannerUrl") or "").strip()
                    or (company.get("bannerUrl") or "").strip()
                    or (preset.get("bannerUrl") or "").strip()
                )

                ad_url_id_for_banner = ad_object_id
                if banner_url_raw:
                    try:
                        ad_url_id_for_banner = cached_url_id(banner_url_raw)
                        log.info("FAST bannerUrl resolved: %s -> %s", banner_url_raw, ad_url_id_for_banner)
                    except Exception as e:
                        write_result_error(
                            user_id, cabinet_id, preset_id, preset_name, trigger_time,
                            "Не удалось получить id по bannerUrl (FAST)", repr(e)
                        )
                        raise

                # --- сначала ВИДЕО ---
                for vid in (ad.get("videoIds") or []):
                    try:
                        media_id = int(vid)
                    except Exception:
                        continue

                    creo = "Видео"
                    group_seq = len(payload_try["ad_groups"]) + 1

                    # Рендерим название группы с полными токенами
                    g_name = truncate_name(
                        render_with_tokens(
                            group_tpl,
                            today_date=today,
                            objective=objective,
                            age=age_str,
                            gender=",".join(genders) if genders else "",
                            n=group_seq,
                            n_g=group_seq,
                            creo=creo,
                            audience_names=aud_names,
                            company_src=company_name_tpl,
                            group_src=group_tpl,
                            banner_src=ad_tpl
                        ),
                        200
                    )

                    # Рендерим название баннера с полными токенами
                    banner_name = truncate_name(
                        render_with_tokens(
                            ad_tpl,
                            today_date=today,
                            objective=objective,
                            age=age_str,
                            gender=",".join(genders) if genders else "",
                            n=1,
                            n_g=group_seq,
                            creo=creo,
                            audience_names=aud_names,
                            company_src=company_name_tpl,
                            group_src=g_name,
                            banner_src=ad_tpl
                        ),
                        200
                    )

                    banner = make_banner_for_creative(
                        ad_url_id_for_banner, ad, idx=1,
                        advertiser_info=adv_info, icon_id=int(icon_id),
                        banner_name=banner_name, cta_text=btn,
                        media_kind="video_portrait_9_16_30s", media_id=media_id, objective=objective
                    )

                    safe_g_name = (g_name or "").strip()
                    if not safe_g_name:
                        safe_g_name = f"Группа {len(payload_try['ad_groups']) + 1}"

                    if not isinstance(banner, dict):
                        log.error("FAST: banner is not a dict, skip group. ad_index=%s", ai)
                        continue

                    group_payload = {
                        "name": safe_g_name,
                        "targetings": json.loads(json.dumps(targetings)),
                        "max_price": max_price_for_group,
                        "autobidding_mode": "max_goals",
                        "budget_limit": None,
                        "budget_limit_day": budget_day,
                        "date_start": today.isoformat(),
                        "date_end": None,
                        "age_restrictions": "18+",
                        "package_id": pkg_id,
                        "utm": utm,
                        "banners": [banner],
                    }
                    pg_group = _build_priced_goal_group(company)
                    if pg_group:
                        group_payload["priced_goal"] = pg_group
                    _add_group_with_optional_pads(payload_try["ad_groups"], group_payload, placements)
                    made_any = True

                # --- затем КАРТИНКИ ---
                for img in (ad.get("imageIds") or []):
                    try:
                        media_id = int(img)
                    except Exception:
                        continue

                    media_kind = detect_image_media_kind(media_id, cabinet_id)
                    creo = "Статика"
                    group_seq = len(payload_try["ad_groups"]) + 1

                    # Рендерим название группы с полными токенами
                    g_name = truncate_name(
                        render_with_tokens(
                            group_tpl,
                            today_date=today,
                            objective=objective,
                            age=age_str,
                            gender=",".join(genders) if genders else "",
                            n=group_seq,
                            n_g=group_seq,
                            creo=creo,
                            audience_names=aud_names,
                            company_src=company_name_tpl,
                            group_src=group_tpl,
                            banner_src=ad_tpl
                        ),
                        200
                    )

                    # Рендерим название баннера с полными токенами
                    banner_name = truncate_name(
                        render_with_tokens(
                            ad_tpl,
                            today_date=today,
                            objective=objective,
                            age=age_str,
                            gender=",".join(genders) if genders else "",
                            n=1,
                            n_g=group_seq,
                            creo=creo,
                            audience_names=aud_names,
                            company_src=company_name_tpl,
                            group_src=g_name,
                            banner_src=ad_tpl
                        ),
                        200
                    )

                    banner = make_banner_for_creative(
                        ad_url_id_for_banner, ad, idx=1,
                        advertiser_info=adv_info, icon_id=int(icon_id),
                        banner_name=banner_name, cta_text=btn,
                        media_kind=media_kind, media_id=media_id, objective=objective
                    )

                    safe_g_name = (g_name or "").strip()
                    if not safe_g_name:
                        safe_g_name = f"Группа {len(payload_try['ad_groups']) + 1}"

                    if not isinstance(banner, dict):
                        log.error("FAST: banner is not a dict, skip group. ad_index=%s", ai)
                        continue

                    group_payload = {
                        "name": safe_g_name,
                        "targetings": json.loads(json.dumps(targetings)),
                        "max_price": max_price_for_group,
                        "autobidding_mode": "max_goals",
                        "budget_limit": None,
                        "budget_limit_day": budget_day,
                        "date_start": today.isoformat(),
                        "date_end": None,
                        "age_restrictions": "18+",
                        "package_id": pkg_id,
                        "utm": utm,
                        "banners": [banner],
                    }
                    pg_group = _build_priced_goal_group(company)
                    if pg_group:
                        group_payload["priced_goal"] = pg_group
                    _add_group_with_optional_pads(payload_try["ad_groups"], group_payload, placements)
                    made_any = True

            if not made_any:
                write_result_error(user_id, cabinet_id, preset_id, preset_name, trigger_time,
                                   "FAST: не собран ни один баннер (нет креативов)", "fast no creatives")
                raise RuntimeError("fast no creatives")

    results = []
    endpoint = f"{API_BASE}/api/v2/ad_plans.json"
    for i in range(1, repeats + 1):
        save_debug_payload(user_id, cabinet_id, f"ad_plan_fast_{i}", payload_try)
        if DEBUG_DRY_RUN:
            log.warning("[DRY RUN] Skipping POST /api/v2/ad_plans.json (no request sent).")
            results.append({"request": payload_try, "response": {"response": {"campaigns": []}}})
            continue

        body_bytes = json.dumps(payload_try, ensure_ascii=False).encode("utf-8")
        log.info(
            "FAST POST (%d/%d): groups=%d, total_banners=%d",
            i, repeats,
            len(payload_try.get("ad_groups", [])),
            sum(len(g.get("banners", [])) for g in payload_try.get("ad_groups", []))
        )

        try:
            resp = with_retries("POST", endpoint, tokens, data=body_bytes)
            results.append({"request": payload_try, "response": resp})
            log.info("FAST POST OK (%d/%d).", i, repeats)
        except ApiHTTPError as e:
            body_text = e.body or ""
            try:
                err_json = json.loads(body_text)
            except Exception:
                err_json = None

            need_swap = False
            if isinstance(err_json, dict):
                txt = json.dumps(err_json, ensure_ascii=False)
                need_swap = ("image_600x600" in txt) and ("bad_width" in txt)

            if need_swap:
                swaps = _swap_image_600_to_1080(payload_try)
                if swaps > 0:
                    save_debug_payload(user_id, cabinet_id, "ad_plan_fast_retry_1080", payload_try)
                    log.warning("FAST: detected bad_width for image_600x600, swapped to image_1080x1080 in %d banner(s). Retrying once...", swaps)
                    try:
                        body_bytes_retry = json.dumps(payload_try, ensure_ascii=False).encode("utf-8")
                        resp2 = with_retries("POST", endpoint, tokens, data=body_bytes_retry)
                        results.append({"request": payload_try, "response": resp2})
                        log.info("FAST POST OK on retry with image_1080x1080.")
                        continue
                    except ApiHTTPError as e2:
                        err_path = save_text_blob(user_id, cabinet_id, "vk_error_ad_plan_post_fast_retry", e2.body)
                        log.error("FAST retry failed: HTTP %s on %s. Saved to: %s (len=%d)",
                                  e2.status, e2.url, err_path, len(e2.body or ""))
                        try:
                            err_json2 = json.loads(e2.body)
                            _dump_vk_validation(err_json2)
                        except Exception as ex2:
                            log.error("FAST VALIDATION (retry): non-JSON or parse failed: %s", ex2)
                        write_result_error(user_id, cabinet_id, preset_id, preset_name, trigger_time,
                                           "Ошибка создания кампании (FAST, retry 1080)",
                                           f"HTTP {e2.status} {e2.url}")
                        raise

            err_path = save_text_blob(user_id, cabinet_id, "vk_error_ad_plan_post_fast", body_text)
            log.error("FAST VK HTTP error %s on %s. Full body saved to: %s (len=%d)",
                      e.status, e.url, err_path, len(body_text))
            try:
                err_json = json.loads(body_text)
                _dump_vk_validation(err_json)
            except Exception as ex:
                log.error("FAST VALIDATION: non-JSON or parse failed: %s", ex)
            write_result_error(user_id, cabinet_id, preset_id, preset_name, trigger_time,
                               "Ошибка создания кампании (FAST)", f"HTTP {e.status} {e.url}")
            raise

    try:
        all_ids: List[int] = []
        for r in results:
            resp = r.get("response") or {}
            ids = extract_campaign_ids_from_resp(resp)
            if ids:
                all_ids.extend(ids)
        id_company = list(dict.fromkeys(all_ids))
    except Exception as e:
        write_result_error(user_id, cabinet_id, preset_id, preset_name, trigger_time,
                           "Не удалось распарсить ответ VK Ads (FAST)", repr(e))
        raise

    write_result_success(user_id, cabinet_id, preset_id, preset_name, trigger_time, id_company)
    return results
# ============================ Основной цикл ============================

def process_queue_once() -> None:
    # Подтягиваем только VK_TOKEN_* из .env
    load_tokens_from_envfile()

    if not GLOBAL_QUEUE_PATH.exists():
        log.debug("Queue file does not exist: %s", GLOBAL_QUEUE_PATH)
        return

    lock = FileLock(str(GLOBAL_QUEUE_PATH) + ".lock")
    with lock:
        try:
            queue = load_json(GLOBAL_QUEUE_PATH)
        except Exception as e:
            log.warning("Cannot read queue: %s", e)
            return

    if not isinstance(queue, list):
        log.warning("Queue is not a list")
        return

    for item in queue:
        try:
            # статус пресета в очереди (по умолчанию считаем active)
            status = str(item.get("status", "active")).strip().lower()
            if status != "active":
                log.info("[SKIP] %s/%s preset=%s | status=%s",
                         item.get("user_id"), item.get("cabinet_id"),
                         item.get("preset_id"), status)
                continue

            user_id = str(item["user_id"])
            cabinet_id = str(item["cabinet_id"])
            preset_id = str(item["preset_id"])
            tokens = item.get("tokens") or []      # имена VK_TOKEN_* или сырые токены
            trigger_time = item.get("trigger_time") or item.get("time") or ""
            count_repeats = int(item.get("count_repeats") or 1)
            fast_flag = str(item.get("fast_preset", "")).strip().lower() == "true"

            now_local = datetime.now(LOCAL_TZ)
            match, info = check_trigger(trigger_time, now_local)
            
            match, info = check_trigger(trigger_time, now_local)
            if not match:
                log.info("[WAIT] %s/%s preset=%s | trigger=%s | target(shifted)=%s | now(+%sh)=%s | delta=%ss (window=%ss)",
                         user_id, cabinet_id, preset_id,
                         info.get("TRIGGER"), info.get("TARGET_SHIFTED"),
                         SERVER_SHIFT_HOURS, info.get("ADJUSTED_NOW"),
                         info.get("DELTA_SEC"), info.get("WINDOW_SEC"))
                continue

            preset_path = USERS_ROOT / user_id / "presets" / str(cabinet_id) / f"{preset_id}.json"
            if not preset_path.exists():
                log.error("Preset not found: %s", preset_path)
                write_result_error(user_id, cabinet_id, preset_id, "", trigger_time,
                                   "Не найден пресет", f"missing preset file: {preset_path}")
                continue

            preset = load_json(preset_path)
            preset_name = str((preset.get("company") or {}).get("presetName") or "")
            log.info("Processing %s/%s preset=%s repeats=%s", user_id, cabinet_id, preset_id, count_repeats)

            try:
                if fast_flag:
                    _ = create_ad_plan_fast(
                        preset, tokens, count_repeats, user_id, cabinet_id,
                        preset_id=preset_id, preset_name=preset_name, trigger_time=trigger_time
                    )
                else:
                    _ = create_ad_plan(
                        preset, tokens, count_repeats, user_id, cabinet_id,
                        preset_id=preset_id, preset_name=preset_name, trigger_time=trigger_time
                    )
            except Exception:
                # ошибка уже записана write_result_error внутри create_ad_plan
                continue

        except Exception as e:
            log.exception("Process item failed: %s", e)

def main_loop() -> None:
    load_tokens_from_envfile()
    now_local = datetime.now(LOCAL_TZ)
    now_utc = datetime.now(UTC_TZ)
    log.info(
        "auto_ads worker started. Tick each 60s. LOCAL_TZ=%s | now_local=%s | now_utc=%s | SHIFT=%sh | WINDOW=%ss",
        LOCAL_TZ,
        now_local.strftime("%Y-%m-%d %H:%M:%S %Z"),
        now_utc.strftime("%Y-%m-%d %H:%M:%S %Z"),
        SERVER_SHIFT_HOURS,
        MATCH_WINDOW_SECONDS
    )
    while True:
        try:
            process_queue_once()
        except Exception as e:
            log.exception("Fatal error: %s", e)
        sleep_to_next_tick(30, wake_early=0.15)

if __name__ == "__main__":
    main_loop()
