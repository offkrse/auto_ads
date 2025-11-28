#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import os
import time
import re
import random
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
VersionCyclop = "0.74"

GLOBAL_QUEUE_PATH = Path("/opt/auto_ads/data/global_queue.json")
USERS_ROOT = Path("/opt/auto_ads/users")
ENV_FILE = Path("/opt/auto_ads/.env")
LOGS_DIR = Path("/opt/auto_ads/logs")
LOG_FILE = LOGS_DIR / "auto_ads_worker.log"

API_BASE = os.getenv("VK_API_BASE", "https://ads.vk.com")

# Фиксированное смещение: от trigger_time ВСЕГДА вычитаем 4 часа
SERVER_SHIFT_HOURS = 4
MATCH_WINDOW_SECONDS = int(os.getenv("MATCH_WINDOW_SECONDS", "55"))  # окно совпадения, сек

DEBUG_SAVE_PAYLOAD = os.getenv("DEBUG_SAVE_PAYLOAD", "0") == "1"
DEBUG_DRY_RUN = os.getenv("DEBUG_DRY_RUN", "0") == "1"
# Ретраи и таймауты
RETRY_MAX = int(os.getenv("RETRY_MAX", "7"))
RETRY_BACKOFF_BASE = float(os.getenv("RETRY_BACKOFF_BASE", "1.7"))
VK_HTTP_TIMEOUT = float(os.getenv("VK_HTTP_TIMEOUT", "60"))            # GET/прочее
VK_HTTP_TIMEOUT_POST = float(os.getenv("VK_HTTP_TIMEOUT_POST", "150")) # POST

# Если сервер в UTC — дефолт уже UTC
LOCAL_TZ = tz.gettz(os.getenv("LOCAL_TZ", "UTC"))
UTC_TZ = tz.gettz("UTC")

BASE_DATE = datetime(2025, 7, 14)
BASE_NUMBER = 53
# ============================ Логирование ============================

def setup_logger() -> logging.Logger:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("auto_ads")
    if logger.handlers:
        return logger

    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logger.setLevel(level)

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    file_handler = TimedRotatingFileHandler(
        filename=str(LOG_FILE),
        when="midnight",
        interval=1,
        backupCount=14,
        encoding="utf-8",
        utc=False,
    )
    file_handler.setFormatter(fmt)
    file_handler.setLevel(level)

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
    return {"socialengagement": 3127}.get(obj, 3127)

# Площадки (pads), разрешённые для пакета 3127 (примерный список)
PADS_FOR_PACKAGE: Dict[int, List[int]] = {
    3127: [102641, 1254386, 111756, 1265106, 1010345, 2243453],
}

# ============================ Утилиты ============================
def compute_day_number(now_ref: datetime) -> int:
    """
    {день} = BASE_NUMBER + (сегодня - BASE_DATE).days
    Для «сегодня» берём серверное локальное время со смещением +4 часа,
    чтобы соответствовать логике триггера (не перескочить дату около полуночи).
    """
    # now_ref уже приходит как now_local
    adjusted = now_ref + timedelta(hours=SERVER_SHIFT_HOURS)
    return BASE_NUMBER + (adjusted.date() - BASE_DATE.date()).days

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

def load_json(path: Path) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def dump_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def write_result_success(user_id: str, cabinet_id: str, campaign_ids: List[int]) -> None:
    out_path = USERS_ROOT / str(user_id) / "created_company" / str(cabinet_id) / "created.json"
    payload = [{
        "cabinet_id": str(cabinet_id),
        "status": "success",
        "text_error": "null",
        "code_error": "null",
        "id_company": campaign_ids,
    }]
    dump_json(out_path, payload)

def write_result_error(user_id: str, cabinet_id: str, human: str, tech: str) -> None:
    out_path = USERS_ROOT / str(user_id) / "created_company" / str(cabinet_id) / "created.json"
    payload = [{
        "cabinet_id": str(cabinet_id),
        "status": "error",
        "text_error": human,
        "code_error": tech,
    }]
    dump_json(out_path, payload)

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
    return ref_now.replace(hour=h, minute=m, second=0, microsecond=0)

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
            log.warning("RequestException (attempt %s/%s): %s; sleep=%.2fs", attempt, RETRY_MAX, e, sleep)
            time.sleep(sleep)
            continue

        if resp.status_code == 429 or 500 <= resp.status_code < 600:
            last_error = f"{resp.status_code}: {resp.text[:200]}"
            base_sleep = RETRY_BACKOFF_BASE ** attempt
            jitter = random.uniform(0, 0.4 * base_sleep)
            sleep = min(60.0, base_sleep + jitter)
            log.warning("API %s (attempt %s/%s). Backoff %.2fs. Body: %s", resp.status_code, attempt, RETRY_MAX, sleep, resp.text[:300])
            time.sleep(sleep)
            continue

        if not (200 <= resp.status_code < 300):
            last_error = f"{resp.status_code}: {resp.text[:500]}"
            base_sleep = RETRY_BACKOFF_BASE ** attempt
            jitter = random.uniform(0, 0.3 * base_sleep)
            sleep = min(30.0, base_sleep + jitter)
            log.warning("Non-2xx %s (attempt %s/%s). Backoff %.2fs. Body: %s", resp.status_code, attempt, RETRY_MAX, sleep, resp.text[:300])
            time.sleep(sleep)
            continue

        try:
            j = resp.json()
            log.debug("API OK %s %s", resp.status_code, url)
            return j
        except ValueError:
            raw = {"raw": resp.text}
            log.debug("API OK (raw response) %s %s", resp.status_code, url)
            return raw

    raise RuntimeError(f"API failed after retries: {last_error}")

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

# ============================ Баннер (строго 2 креатива) ============================

def make_banner_for_ad(company_name: str, ad_object_id: int, ad: Dict[str, Any],
                       idx: int, advertiser_info: str, icon_id: Optional[int]) -> Dict[str, Any]:
    """
    Строго 2 креатива: icon_256x256 (из company.logoId) + video_portrait_9_16_30s (из ads[i].videoIds[0])
    Тексты:
      - about_company_115: из company.advertiserInfo
      - cta_community_vk: visitSite
      - text_2000: ad.shortDescription
      - title_40_vkads: ad.title
    """
    title = (ad.get("title") or "").strip()
    short = (ad.get("shortDescription") or "").strip()
    vids = ad.get("videoIds") or []
    video_id = vids[0] if vids else None

    if not advertiser_info:
        raise ValueError("Отсутствует advertiserInfo в company (для about_company_115).")
    if not icon_id:
        raise ValueError("Отсутствует logoId в company (для icon_256x256.id).")
    if not video_id:
        raise ValueError(f"У объявления ads[{idx-1}] отсутствует videoIds[0].")

    return {
        "name": f"Объявление {idx}",
        "urls": {"primary": {"id": ad_object_id}},
        "content": {
            "icon_256x256": {"id": int(icon_id)},
            "video_portrait_9_16_30s": {"id": int(video_id)},
        },
        "textblocks": {
            "about_company_115": {"text": advertiser_info, "title": ""},
            "cta_community_vk": {"text": "visitSite", "title": ""},
            "text_2000": {"text": short, "title": ""},
            "title_40_vkads": {"text": title, "title": ""},
        }
    }

# ============================ Построение payload ============================

def build_ad_plan_payload(preset: Dict[str, Any], ad_object_id: int, plan_index: int) -> Dict[str, Any]:
    company = preset["company"]
    groups = preset.get("groups", [])
    company_name = (company.get("companyName") or "Авто кампания").strip() or "Авто кампания"
    objective = company.get("targetAction", "socialengagement")
    package_id = package_id_for_objective(objective)

    now_local = datetime.now(LOCAL_TZ) + timedelta(hours=SERVER_SHIFT_HOURS)
    start_date_str = now_local.date().isoformat()

    ad_groups_payload = []

    for g_idx, g in enumerate(groups, start=1):
        group_name = f"Группа {g_idx}"
        regions = as_int_list(g.get("regions"))
        genders = split_gender(g.get("gender", ""))
        segments = as_int_list(g.get("audienceIds"))
        # interests — УДАЛЕНЫ
        age_list = build_age_list(g.get("age", ""))

        targetings: Dict[str, Any] = {"geo": {"regions": regions}}
        if genders:
            targetings["sex"] = genders
        if segments:
            targetings["segments"] = segments
        if age_list:
            targetings["age"] = {"age_list": age_list}

        pads_vals = PADS_FOR_PACKAGE.get(package_id)
        if pads_vals:
            targetings["pads"] = pads_vals

        budget_day = int(g.get("budget") or 0)
        utm = g.get("utm") or "ref_source={{banner_id}}&ref={{campaign_id}}"

        # баннер заполним позже (в create_ad_plan)
        banners_payload = [{"name": f"Объявление {g_idx}", "urls": {"primary": {"id": ad_object_id}}}]

        ad_groups_payload.append({
            "name": group_name,
            "targetings": targetings,
            "max_price": 0,
            # ГРУППА: как просили
            "autobidding_mode": "max_goals",
            "budget_limit": None,
            "budget_limit_day": budget_day,
            "date_start": start_date_str,
            "date_end": None,
            "age_restrictions": "18+",
            "package_id": package_id,
            "utm": utm,
            "banners": banners_payload,
        })

    # КОМПАНИЯ: три поля — строго null
    payload = {
        "name": f"{company_name}",
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
    return payload

# ============================ Создание плана ============================

def create_ad_plan(preset: Dict[str, Any], tokens: List[str], repeats: int,
                   user_id: str, cabinet_id: str) -> List[Dict[str, Any]]:
    """
    При ошибке пишет файл error с понятным текстом и техническим кодом.
    При успехе — пишет success с массивом id_company из response.campaigns[].id
    Возвращает список «сырого» ответа VK Ads (для внутренней отладки).
    """
    # --- локальные хелперы для abstractAudiences ---
    BASE_DATE = datetime(2025, 7, 14)
    BASE_NUMBER = 53

    def _compute_day_number(now_ref: datetime) -> int:
        adjusted = now_ref + timedelta(hours=SERVER_SHIFT_HOURS)
        return BASE_NUMBER + (adjusted.date() - BASE_DATE.date()).days

    def _resolve_abstract_audiences(_tokens: List[str], names: List[str], day_number: int) -> List[int]:
        ids: List[int] = []
        for raw in names or []:
            name = str(raw).replace("{день}", str(day_number))
            from urllib.parse import quote
            endpoint = f"{API_BASE}/api/v2/remarketing/segments.json?_name={quote(name, safe='')}"
            try:
                resp = with_retries("GET", endpoint, _tokens)
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
        return list(dict.fromkeys(ids))
    # --- конец хелперов ---

    company = preset["company"]
    url = company.get("url")
    if not url:
        human = "Отсутствует URL компании (company.url)"
        tech = "Missing company.url"
        write_result_error(user_id, cabinet_id, human, tech)
        raise RuntimeError(tech)

    # допустим company может содержать дефолты, но это не обязательно
    company_adv = (company.get("advertiserInfo") or "").strip()
    company_logo = company.get("logoId")

    company_name = (company.get("companyName") or "Авто кампания").strip() or "Авто кампания"

    try:
        ad_object_id = resolve_url_id(url, tokens)
    except Exception as e:
        write_result_error(user_id, cabinet_id, "Не удалось получить ad_object_id по URL", repr(e))
        raise

    # Работаем с копией пресета (обогащаем audienceIds из abstractAudiences)
    preset_mut = json.loads(json.dumps(preset, ensure_ascii=False))
    groups = preset_mut.get("groups", []) or []
    ads = preset_mut.get("ads", []) or []

    if not groups:
        write_result_error(user_id, cabinet_id, "В пресете отсутствуют группы", "groups is empty")
        raise RuntimeError("groups is empty")

    # 1) Обогащаем segments из abstractAudiences
    day_number = _compute_day_number(datetime.now(LOCAL_TZ))
    for gi, g in enumerate(groups):
        abstract_names = g.get("abstractAudiences") or []
        if abstract_names:
            add_ids = _resolve_abstract_audiences(tokens, abstract_names, day_number)
            if add_ids:
                base_ids = g.get("audienceIds") or []
                merged = as_int_list(base_ids) + add_ids
                g["audienceIds"] = list(dict.fromkeys(merged))
                log.info("Group #%d segments extended by abstractAudiences: +%d id(s)", gi+1, len(add_ids))

    # 2) Баннеры 1:1 с группами. Для каждого объявления берём advertiserInfo/logoId:
    banners_by_group: List[Dict[str, Any]] = []
    for gi in range(len(groups)):
        ad = ads[gi] if gi < len(ads) else None
        if not ad:
            human = f"Для группы #{gi+1} отсутствует объявление в 'ads'"
            tech = f"ads[{gi}] is missing"
            write_result_error(user_id, cabinet_id, human, tech)
            raise RuntimeError(tech)

        # приоритет: ad.* → company.*
        adv_info = (ad.get("advertiserInfo") or company_adv or "").strip()
        icon_id = ad.get("logoId") or company_logo

        if not adv_info:
            human = f"В объявлении #{gi+1} отсутствует 'advertiserInfo' и не задан в company"
            tech = f"Missing ads[{gi}].advertiserInfo and company.advertiserInfo"
            write_result_error(user_id, cabinet_id, human, tech)
            raise RuntimeError(tech)

        if not icon_id:
            human = f"В объявлении #{gi+1} отсутствует 'logoId' и не задан в company"
            tech = f"Missing ads[{gi}].logoId and company.logoId"
            write_result_error(user_id, cabinet_id, human, tech)
            raise RuntimeError(tech)

        try:
            banner = make_banner_for_ad(company_name, ad_object_id, ad, gi + 1, adv_info, int(icon_id))
            banners_by_group.append(banner)
            log.info("Собран баннер #%d из ads[%d]", gi+1, gi)
        except Exception as e:
            write_result_error(user_id, cabinet_id, "Ошибка сборки баннера", repr(e))
            raise

    results = []
    endpoint = f"{API_BASE}/api/v2/ad_plans.json"

    for i in range(1, repeats + 1):
        base_payload = build_ad_plan_payload(preset_mut, ad_object_id, i)
        payload_try = json.loads(json.dumps(base_payload, ensure_ascii=False))

        # подставляем баннеры в группы
        ad_groups = payload_try.get("ad_groups", [])
        for gi, g in enumerate(ad_groups):
            g["banners"] = [banners_by_group[gi]]

        # DEBUG: сохранить payload (если включено)
        save_debug_payload(user_id, cabinet_id, f"ad_plan_{i}", payload_try)

        if DEBUG_DRY_RUN:
            log.warning("[DRY RUN] Skipping POST /api/v2/ad_plans.json (no request sent).")
            results.append({"request": payload_try, "response": {"response": {"campaigns": []}}})
            continue

        body_bytes = json.dumps(payload_try, ensure_ascii=False).encode("utf-8")
        log.info(
            "POST ad_plan (%d/%d): groups=%d, banners_per_group=1, payload=%.1f KB, timeout=disabled",
            i, repeats, len(ad_groups), len(body_bytes)/1024.0
        )

        try:
            resp = with_retries("POST", endpoint, tokens, data=body_bytes)
            results.append({"request": payload_try, "response": resp})
            log.info("POST OK (%d/%d).", i, repeats)
            try:
                log.debug("VK response:\n%s", json.dumps(resp, ensure_ascii=False, indent=2))
            except Exception:
                log.debug("VK response (raw): %s", str(resp)[:800])
        except Exception as e:
            write_result_error(user_id, cabinet_id, "Ошибка создания кампании в VK Ads", repr(e))
            raise

    # Успех: вытаскиваем id кампаний
    try:
        last = results[-1]["response"] if results else {}
        campaigns = (last.get("response") or {}).get("campaigns") or []
        campaign_ids = [int(x.get("id")) for x in campaigns if isinstance(x, dict) and "id" in x]
    except Exception as e:
        write_result_error(user_id, cabinet_id, "Не удалось распарсить ответ VK Ads", repr(e))
        raise

    write_result_success(user_id, cabinet_id, campaign_ids)
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

    now_local = datetime.now(LOCAL_TZ)
    for item in queue:
        try:
            user_id = str(item["user_id"])
            cabinet_id = str(item["cabinet_id"])
            preset_id = item["preset_id"]
            tokens = item.get("tokens") or []      # имена VK_TOKEN_* или сырые токены
            trigger_time = item.get("trigger_time") or item.get("time") or ""
            count_repeats = int(item.get("count_repeats") or 1)

            match, info = check_trigger(trigger_time, now_local)
            if not match:
                log.info("[WAIT] %s/%s preset=%s | trigger=%s | target=%s | now(+%sh)=%s | delta=%ss (window=%ss)",
                     user_id, cabinet_id, preset_id,
                     info.get("TRIGGER"), info.get("TARGET_LOCAL"),
                     SERVER_SHIFT_HOURS, info.get("ADJUSTED_NOW"), info.get("DELTA_SEC"), info.get("WINDOW_SEC"))
                continue

            preset_path = USERS_ROOT / user_id / "presets" / str(cabinet_id) / f"{preset_id}.json"
            if not preset_path.exists():
                log.error("Preset not found: %s", preset_path)
                write_result_error(user_id, cabinet_id, "Не найден пресет", f"missing preset file: {preset_path}")
                continue

            preset = load_json(preset_path)
            log.info("Processing %s/%s preset=%s repeats=%s", user_id, cabinet_id, preset_id, count_repeats)

            try:
                _ = create_ad_plan(preset, tokens, count_repeats, user_id, cabinet_id)
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
        time.sleep(60)

if __name__ == "__main__":
    main_loop()
