#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import os
import time
import re
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
VersionCyclop = "0.4 unstable"

GLOBAL_QUEUE_PATH = Path("/opt/auto_ads/data/global_queue.json")
USERS_ROOT = Path("/opt/auto_ads/users")
ENV_FILE = Path("/opt/auto_ads/.env")
LOGS_DIR = Path("/opt/auto_ads/logs")
LOG_FILE = LOGS_DIR / "auto_ads_worker.log"

API_BASE = os.getenv("VK_API_BASE", "https://ads.vk.com")
TRIGGER_EXTRA_HOURS = -4       # -4 часа от trigger_time
MATCH_WINDOW_SECONDS = int(os.getenv("MATCH_WINDOW_SECONDS", "55"))      # окно совпадения, сек
RETRY_MAX = int(os.getenv("RETRY_MAX", "6"))                              # попытки при 429/5xx
RETRY_BACKOFF_BASE = float(os.getenv("RETRY_BACKOFF_BASE", "1.5"))        # экспоненциальный бэкофф

# ====== Креативы и тексты по умолчанию (можно переопределить через env) ======
DEFAULT_ABOUT = (
    "ООО «БСМЕДИА» 443080, Россия, г. Самара, Московское шоссе, д. 43, оф. 706 ОГРН 1216300005536"
)
ABOUT_COMPANY_TEXT = (os.getenv("ABOUT_COMPANY_TEXT") or "").strip() or DEFAULT_ABOUT

ICON_IMAGE_ID = int(os.getenv("ICON_IMAGE_ID", "98308610"))  # иконка из ТЗ
VIDEO_ID = int(os.getenv("VIDEO_ID", "79401418"))            # видео из ТЗ

# Если сервер в UTC — дефолт уже UTC
LOCAL_TZ = tz.gettz(os.getenv("LOCAL_TZ", "UTC"))
UTC_TZ = tz.gettz("UTC")

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

# ============================ Константы пакетов/площадок ============================

def package_id_for_objective(obj: str) -> int:
    return {"socialengagement": 3127}.get(obj, 3127)

# Площадки (pads), разрешённые для пакета 3127
PADS_FOR_PACKAGE: Dict[int, List[int]] = {
    3127: [102641, 1254386, 111756, 1265106, 1010345, 2243453],
}

# ============================ Утилиты ============================

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
    Берём часы/минуты из trigger_hhmm для СЕГОДНЯ в LOCAL_TZ и добавляем TRIGGER_EXTRA_HOURS.
    """
    h, m = parse_hhmm(trigger_hhmm)
    base = ref_now.replace(hour=h, minute=m, second=0, microsecond=0)
    return base + timedelta(hours=TRIGGER_EXTRA_HOURS)

def check_trigger(trigger_hhmm: str, now_local: Optional[datetime] = None) -> Tuple[bool, Dict[str, str]]:
    """
    Возвращает (match, info_dict) и логирует подробности сравнения времени.
    """
    if now_local is None:
        now_local = datetime.now(LOCAL_TZ)
    now_utc = datetime.now(UTC_TZ)

    try:
        target = compute_target_dt(trigger_hhmm, now_local)
    except Exception as e:
        log.error("Trigger parse error '%s': %s", trigger_hhmm, e)
        return False, {"error": str(e)}

    delta_sec = (now_local - target).total_seconds()
    match = 0 <= delta_sec <= MATCH_WINDOW_SECONDS

    info = {
        "LOCAL_TZ": str(LOCAL_TZ),
        "TRIGGER": trigger_hhmm,
        "TRIGGER_EXTRA_HOURS": str(TRIGGER_EXTRA_HOURS),
        "NOW_LOCAL": now_local.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "NOW_UTC": now_utc.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "TARGET_LOCAL": target.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "DELTA_SEC": f"{delta_sec:.3f}",
        "WINDOW_SEC": str(MATCH_WINDOW_SECONDS),
        "MATCH": str(match),
    }

    log.info(
        "TimeCheck | tz=%s | trig=%s | extra=%sh | now_local=%s | now_utc=%s | target=%s | delta=%.3fs | window=%ss | match=%s",
        info["LOCAL_TZ"], info["TRIGGER"], info["TRIGGER_EXTRA_HOURS"],
        info["NOW_LOCAL"], info["NOW_UTC"], info["TARGET_LOCAL"],
        float(info["DELTA_SEC"]), info["WINDOW_SEC"], info["MATCH"]
    )
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
    log.debug("API %s %s", method, url)
    return requests.request(method, url, headers=headers, timeout=30, **kwargs)

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
            sleep = RETRY_BACKOFF_BASE ** attempt
            log.warning("RequestException (attempt %s): %s; sleep=%.2fs", attempt, e, sleep)
            time.sleep(sleep)
            continue

        if resp.status_code == 429 or 500 <= resp.status_code < 600:
            last_error = f"{resp.status_code}: {resp.text[:200]}"
            sleep = RETRY_BACKOFF_BASE ** attempt
            log.warning("API %s (attempt %s). Backoff %.2fs. Body: %s", resp.status_code, attempt, sleep, resp.text[:300])
            time.sleep(sleep)
            continue

        if not (200 <= resp.status_code < 300):
            last_error = f"{resp.status_code}: {resp.text[:500]}"
            sleep = min(60, RETRY_BACKOFF_BASE ** attempt)
            log.warning("Non-2xx %s (attempt %s). Backoff %.2fs. Body: %s", resp.status_code, attempt, sleep, resp.text[:300])
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

def make_banner_variants(company_name: str, ad_object_id: int, ad: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    ЕДИНСТВЕННЫЙ допустимый вариант:
    - content: icon_256x256 + video_portrait_9_16_30s
    - textblocks: about_company_115, cta_community_vk, text_2000, title_40_vkads
    Если чего-то нет — кидаем ошибку (чтобы не отправлять лишние/неверные шаблоны).
    """
    # Тексты
    short = (ad.get("shortDescription") or "").strip()
    title = (ad.get("title") or "").strip()

    # Видео: берём из ad.videoIds[0], если нет — используем VIDEO_ID из окружения/дефолта
    video_ids = ad.get("videoIds") or []
    video_id = int(video_ids[0]) if video_ids else int(VIDEO_ID)
    icon_id = int(ICON_IMAGE_ID)

    if not icon_id or not video_id:
        raise RuntimeError("Нужны оба креатива: ICON_IMAGE_ID и VIDEO_ID (или ad.videoIds).")

    banner = {
        "name": f"{company_name} - баннер 1",
        "urls": {"primary": {"id": ad_object_id}},
        "content": {
            "icon_256x256": {"id": icon_id},
            "video_portrait_9_16_30s": {"id": video_id},
        },
        "textblocks": {
            "about_company_115": {"text": ABOUT_COMPANY_TEXT, "title": ""},
            "cta_community_vk": {"text": "visitSite", "title": ""},
            "text_2000": {"text": short, "title": ""},
            "title_40_vkads": {"text": title, "title": ""},
        }
    }

    return [banner]  # только 1 вариант

def make_banner_for_ad(company_name: str, ad_object_id: int, ad: Dict[str, Any]) -> Dict[str, Any]:
    """
    Строго 2 креатива: icon_256x256 + video_portrait_9_16_30s
    Тексты: about_company_115, cta_community_vk, text_2000, title_40_vkads
    """
    short = (ad.get("shortDescription") or "").strip()
    title = (ad.get("title") or "").strip()

    video_ids = ad.get("videoIds") or []
    video_id = int(video_ids[0]) if video_ids else int(VIDEO_ID)
    icon_id = int(ICON_IMAGE_ID)
    if not icon_id or not video_id:
        raise RuntimeError("Нужны оба креатива: ICON_IMAGE_ID и VIDEO_ID (или ad.videoIds).")

    return {
        "name": f"{company_name} - баннер 1",
        "urls": {"primary": {"id": ad_object_id}},
        "content": {
            "icon_256x256": {"id": icon_id},
            "video_portrait_9_16_30s": {"id": video_id},
        },
        "textblocks": {
            "about_company_115": {"text": ABOUT_COMPANY_TEXT, "title": ""},
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

    # стартовая дата = сегодня +/- EXTRA (как у вас было)
    now_local = datetime.now(LOCAL_TZ) + timedelta(hours=TRIGGER_EXTRA_HOURS)
    start_date_str = now_local.date().isoformat()

    ad_groups_payload = []

    for g_idx, g in enumerate(groups, start=1):
        group_name = f"{company_name} - группа {g_idx}"
        regions = as_int_list(g.get("regions"))
        genders = split_gender(g.get("gender", ""))
        segments = as_int_list(g.get("audienceIds"))
        # interests удалены
        age_list = build_age_list(g.get("age", ""))

        targetings: Dict[str, Any] = {"geo": {"regions": regions}}
        if genders:
            targetings["sex"] = genders
        if segments:
            targetings["segments"] = segments
        if age_list:
            targetings["age"] = {"age_list": age_list}

        # фиксируем pads для пакета
        pads_vals = PADS_FOR_PACKAGE.get(package_id)
        if pads_vals:
            targetings["pads"] = pads_vals

        budget_day = int(g.get("budget") or 0)
        utm = g.get("utm") or "ref_source={{banner_id}}&ref={{campaign_id}}"

        # баннер заполнится позже (в create_ad_plan)
        banners_payload = [{"name": f"{company_name} - баннер 1", "urls": {"primary": {"id": ad_object_id}}}]

        ad_groups_payload.append({
            "name": group_name,
            "targetings": targetings,
            "max_price": 0,
            "budget_limit": None,
            "budget_limit_day": budget_day,  # у групп оставляем как было
            "date_start": start_date_str,
            "date_end": None,
            "age_restrictions": "18+",
            "package_id": package_id,
            "utm": utm,
            "banners": banners_payload,
        })

    # ⬇️ На уровне КОМПАНИИ делаем строго null для трёх полей
    payload = {
        "name": f"{company_name}",
        "status": "active",
        "date_start": start_date_str,
        "date_end": None,
        "autobidding_mode": None,   # <-- null
        "budget_limit_day": None,   # <-- null
        "budget_limit": None,       # <-- null
        "max_price": 0,
        "objective": objective,
        "ad_object_id": ad_object_id,
        "ad_object_type": "url",
        "ad_groups": ad_groups_payload,
    }
    return payload

# ============================ Создание плана (строго с нужным шаблоном) ============================

def create_ad_plan(preset: Dict[str, Any], tokens: List[str], repeats: int) -> List[Dict[str, Any]]:
    company = preset["company"]
    url = company.get("url")
    if not url:
        raise RuntimeError("В пресете нет company.url")

    company_name = (company.get("companyName") or "Авто кампания").strip() or "Авто кампания"
    ad_object_id = resolve_url_id(url, tokens)

    ads = preset.get("ads", [])
    groups = preset.get("groups", []) or []
    groups_count = len(groups)

    if groups_count == 0:
        raise RuntimeError("В пресете пустой список groups — нечего создавать.")

    # Готовим баннеры по принципу: баннер i = ads[i]
    # Если ads меньше, чем groups — для оставшихся групп берём ПОСЛЕДНИЙ баннер.
    # Если ads пустой — делаем один баннер из дефолтов (ICON_IMAGE_ID/VIDEO_ID) и используем его для всех групп.
    banners_by_group: List[Dict[str, Any]] = []
    if not ads:
        log.warning("В пресете пустой ads — используем дефолтный баннер для всех групп.")
        default_ad = {"title": "", "shortDescription": "", "videoIds": []}
        default_banner = make_banner_for_ad(company_name, ad_object_id, default_ad)
        banners_by_group = [default_banner for _ in range(groups_count)]
    else:
        prepared_banners: List[Dict[str, Any]] = []
        for idx, ad in enumerate(ads, start=1):
            b = make_banner_for_ad(company_name, ad_object_id, ad)
            prepared_banners.append(b)
            log.info("Собран баннер #%d из ads[%d]", idx, idx-1)

        for gi in range(groups_count):
            if gi < len(prepared_banners):
                banners_by_group.append(prepared_banners[gi])
            else:
                banners_by_group.append(prepared_banners[-1])  # берём последний

    results = []
    for i in range(1, repeats + 1):
        base_payload = build_ad_plan_payload(preset, ad_object_id, i)

        # Подставляем баннер для КАЖДОЙ группы по индексу
        payload_try = json.loads(json.dumps(base_payload, ensure_ascii=False))
        ad_groups = payload_try.get("ad_groups", [])
        for gi, g in enumerate(ad_groups):
            g["banners"] = [banners_by_group[gi]]
            # Страховка: если вдруг нет UTM — поставим дефолт, чтобы везде он был
            g["utm"] = g.get("utm") or "ref_source={{banner_id}}&ref={{campaign_id}}"

        endpoint = f"{API_BASE}/api/v2/ad_plans.json"
        resp = with_retries(
            "POST",
            endpoint,
            tokens,
            data=json.dumps(payload_try, ensure_ascii=False).encode("utf-8")
        )
        results.append({"request": payload_try, "response": resp, "variant_used": "per-group icon+video"})
        log.info("Ad plan created (%d/%d) with per-group banner mapping (ads[i] -> groups[i]).", i, repeats)

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
            user_id = item["user_id"]
            cabinet_id = item["cabinet_id"]
            preset_id = item["preset_id"]
            tokens = item.get("tokens") or []      # имена VK_TOKEN_* или сырые токены
            trigger_time = item.get("trigger_time") or item.get("time") or ""
            count_repeats = int(item.get("count_repeats") or 1)

            match, info = check_trigger(trigger_time, now_local)
            if not match:
                log.info("[WAIT] %s/%s preset=%s | trigger=%s | target=%s | now=%s | delta=%ss (window=%ss)",
                         user_id, cabinet_id, preset_id,
                         info.get("TRIGGER"), info.get("TARGET_LOCAL"),
                         info.get("NOW_LOCAL"), info.get("DELTA_SEC"), info.get("WINDOW_SEC"))
                continue

            preset_path = USERS_ROOT / str(user_id) / "presets" / str(cabinet_id) / f"{preset_id}.json"
            if not preset_path.exists():
                log.error("Preset not found: %s", preset_path)
                continue

            preset = load_json(preset_path)
            log.info("Processing %s/%s preset=%s repeats=%s", user_id, cabinet_id, preset_id, count_repeats)
            results = create_ad_plan(preset, tokens, count_repeats)

            out_path = USERS_ROOT / str(user_id) / "created_company" / str(cabinet_id) / "created.json"
            dump_json(out_path, results)
            log.info("Saved %d result(s) to %s", len(results), out_path)

        except Exception as e:
            log.exception("Process item failed: %s", e)

def main_loop() -> None:
    load_tokens_from_envfile()
    now_local = datetime.now(LOCAL_TZ)
    now_utc = datetime.now(UTC_TZ)
    log.info(
        "auto_ads worker started. Tick each 60s. LOCAL_TZ=%s | now_local=%s | now_utc=%s | EXTRA=%sh | WINDOW=%ss",
        LOCAL_TZ,
        now_local.strftime("%Y-%m-%d %H:%M:%S %Z"),
        now_utc.strftime("%Y-%m-%d %H:%M:%S %Z"),
        TRIGGER_EXTRA_HOURS,
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
