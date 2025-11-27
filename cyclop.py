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
from dotenv import load_dotenv

# ============================ Пути/конфигурация ============================

GLOBAL_QUEUE_PATH = Path("/opt/auto_ads/data/global_queue.json")
USERS_ROOT = Path("/opt/auto_ads/users")
ENV_FILE = Path("/opt/auto_ads/.env")
LOGS_DIR = Path("/opt/auto_ads/logs")
LOG_FILE = LOGS_DIR / "auto_ads_worker.log"

API_BASE = os.getenv("VK_API_BASE", "https://ads.vk.com")
TRIGGER_EXTRA_HOURS = int(os.getenv("TRIGGER_EXTRA_HOURS", "4"))        # +4 часа к trigger_time
MATCH_WINDOW_SECONDS = int(os.getenv("MATCH_WINDOW_SECONDS", "55"))      # окно совпадения, сек
RETRY_MAX = int(os.getenv("RETRY_MAX", "6"))                              # попытки при 429/5xx
RETRY_BACKOFF_BASE = float(os.getenv("RETRY_BACKOFF_BASE", "1.5"))        # экспоненциальный бэкофф

ABOUT_COMPANY_TEXT = (os.getenv("ABOUT_COMPANY_TEXT") or "").strip() or None
ICON_IMAGE_ID = os.getenv("ICON_IMAGE_ID")  # можно не задавать

LOCAL_TZ = tz.gettz(os.getenv("LOCAL_TZ", "Europe/Moscow"))

# ============================ Логирование ============================

def setup_logger() -> logging.Logger:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("auto_ads")
    if logger.handlers:
        return logger  # уже сконфигурирован

    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logger.setLevel(level)

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Ротация по дням, хранить 14 файлов
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

    # Дублируем в stdout (удобно для systemd/journal)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(fmt)
    stream_handler.setLevel(level)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.propagate = False
    return logger

log = setup_logger()

# =====================================================================

def load_env() -> None:
    """Подгружаем .env (override=True, чтобы обновлять значения)."""
    if ENV_FILE.exists():
        load_dotenv(dotenv_path=str(ENV_FILE), override=True)
        log.debug("Env loaded from %s", ENV_FILE)

def load_json(path: Path) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def dump_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def within_now(target: datetime, now: Optional[datetime] = None) -> bool:
    """now попадает в [target, target + MATCH_WINDOW_SECONDS]."""
    if now is None:
        now = datetime.now(LOCAL_TZ)
    delta = (now - target).total_seconds()
    return 0 <= delta <= MATCH_WINDOW_SECONDS

def parse_hhmm(s: str) -> Tuple[int, int]:
    m = re.fullmatch(r"\s*(\d{1,2}):(\d{2})\s*", s or "")
    if not m:
        raise ValueError(f"Bad HH:MM: {s}")
    h = int(m.group(1))
    mi = int(m.group(2))
    if not (0 <= h < 24 and 0 <= mi < 60):
        raise ValueError(f"Out of range HH:MM: {s}")
    return h, mi

def build_age_list(age_range_str: str) -> List[int]:
    """Из '21-55' -> [0,21,22,...,55]. Если формат странный — вернём [0]."""
    m = re.fullmatch(r"\s*(\d{1,2})\s*-\s*(\d{1,2})\s*", age_range_str or "")
    ages = [0]
    if not m:
        return ages
    a = int(m.group(1))
    b = int(m.group(2))
    if a > b:
        a, b = b, a
    ages.extend(list(range(a, b + 1)))
    return ages

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

def env_token(name: str) -> Optional[str]:
    """Берём значение токена из окружения (которое поддерживает .env)."""
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
    """
    Повтор с экспоненциальным бэкоффом при 429/5xx.
    Перебираем токены циклически. Если в tokens передали уже "сырой" токен, тоже сработает.
    """
    last_error = None
    total_tokens = max(1, len(tokens))
    for attempt in range(1, RETRY_MAX + 1):
        token_idx = (attempt - 1) % total_tokens
        token_key_or_value = tokens[token_idx] if tokens else ""
        token_value = env_token(token_key_or_value) or token_key_or_value  # либо по имени из .env, либо строка как есть

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

def package_id_for_objective(obj: str) -> int:
    return {
        "socialengagement": 3127,
    }.get(obj, 3127)

def build_ad_plan_payload(
    preset: Dict[str, Any],
    ad_object_id: int,
    plan_index: int
) -> Dict[str, Any]:
    """
    Формируем тело POST /api/v2/ad_plans.json
    plan_index начинается с 1 (можно расширить, если захотите нумеровать имена).
    """
    company = preset["company"]
    groups = preset.get("groups", [])
    ads = preset.get("ads", [])

    company_name = (company.get("companyName") or "Авто кампания").strip() or "Авто кампания"
    objective = company.get("targetAction", "socialengagement")

    # дата старта = сегодня (+4ч), формат YYYY-MM-DD
    now_local = datetime.now(LOCAL_TZ) + timedelta(hours=TRIGGER_EXTRA_HOURS)
    start_date_str = now_local.date().isoformat()

    ad_groups_payload = []
    for g_idx, g in enumerate(groups, start=1):
        group_name = f"{company_name} - группа {g_idx}"
        regions = as_int_list(g.get("regions"))
        genders = split_gender(g.get("gender", ""))
        segments = as_int_list(g.get("audienceIds"))
        interests_raw = str(g.get("interests") or "").strip()
        interests = as_int_list(interests_raw) if interests_raw else []
        age_list = build_age_list(g.get("age", ""))

        targetings: Dict[str, Any] = {"geo": {"regions": regions}}
        if genders:
            targetings["sex"] = genders
        if segments:
            targetings["segments"] = segments
        if interests:
            targetings["interests"] = interests
        if age_list:
            targetings["age"] = {"age_list": age_list}

        budget_day = int(g.get("budget") or 0)
        utm = g.get("utm") or ""

        # баннеры — по числу объявлений
        banners_payload = []
        for a_idx, ad in enumerate(ads, start=1):
            banner_name = f"{company_name} - баннер {a_idx}"
            content: Dict[str, Any] = {}
            textblocks: Dict[str, Any] = {}

            # Медиаконтент
            if ICON_IMAGE_ID:
                content["icon_256x256"] = {"id": int(ICON_IMAGE_ID)}
            video_ids = ad.get("videoIds") or []
            if video_ids:
                content["video_portrait_9_16_30s"] = {"id": int(video_ids[0])}

            # Тексты
            short = ad.get("shortDescription") or ""
            title = ad.get("title") or ""

            if ABOUT_COMPANY_TEXT:
                textblocks["about_company_115"] = {"text": ABOUT_COMPANY_TEXT, "title": ""}
            textblocks["cta_community_vk"] = {"text": "visitSite", "title": ""}
            textblocks["text_2000"] = {"text": short, "title": ""}
            textblocks["title_40_vkads"] = {"text": title, "title": ""}

            banners_payload.append({
                "name": banner_name,
                "urls": {"primary": {"id": ad_object_id}},
                "content": content,
                "textblocks": textblocks,
            })

        if not banners_payload:
            banners_payload.append({
                "name": f"{company_name} - баннер 1",
                "urls": {"primary": {"id": ad_object_id}},
            })

        ad_groups_payload.append({
            "name": group_name,
            "targetings": targetings,
            "max_price": 0,
            "budget_limit": None,
            "budget_limit_day": budget_day,
            "date_start": start_date_str,
            "date_end": None,
            "age_restrictions": "18+",
            "package_id": package_id_for_objective(objective),
            "utm": utm,
            "banners": banners_payload,
        })

    payload = {
        "name": f"{company_name}",
        "status": "active",
        "date_start": start_date_str,
        "date_end": None,
        "autobidding_mode": "max_goals",
        "budget_limit_day": None,
        "budget_limit": None,
        "max_price": 0,
        "objective": objective,
        "ad_object_id": ad_object_id,
        "ad_object_type": "url",
        "ad_groups": ad_groups_payload,
    }
    return payload

def create_ad_plan(preset: Dict[str, Any], tokens: List[str], repeats: int) -> List[Dict[str, Any]]:
    """
    Возвращает список ответов API по числу созданий (count_repeats).
    """
    company = preset["company"]
    url = company.get("url")
    if not url:
        raise RuntimeError("В пресете нет company.url")

    ad_object_id = resolve_url_id(url, tokens)
    results = []
    for i in range(1, repeats + 1):
        payload = build_ad_plan_payload(preset, ad_object_id, i)
        endpoint = f"{API_BASE}/api/v2/ad_plans.json"
        resp = with_retries("POST", endpoint, tokens, data=json.dumps(payload, ensure_ascii=False).encode("utf-8"))
        results.append({"request": payload, "response": resp})
        log.info("Ad plan created (%d/%d) for url_id=%s", i, repeats, ad_object_id)
    return results

def should_fire_now(trigger_hhmm: str, now: Optional[datetime] = None) -> bool:
    """
    Условие запуска: текущая минута совпадает с (trigger_time + TRIGGER_EXTRA_HOURS).
    """
    if now is None:
        now = datetime.now(LOCAL_TZ)
    h, m = parse_hhmm(trigger_hhmm)
    target = now.replace(hour=h, minute=m, second=0, microsecond=0) + timedelta(hours=TRIGGER_EXTRA_HOURS)
    return within_now(target, now)

def process_queue_once() -> None:
    # Подхватим актуальные переменные окружения из .env
    load_env()

    if not GLOBAL_QUEUE_PATH.exists():
        log.debug("Queue file does not exist: %s", GLOBAL_QUEUE_PATH)
        return

    # Лочим файл, чтобы не было гонок при параллельных запусках
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

    now = datetime.now(LOCAL_TZ)
    for item in queue:
        try:
            user_id = item["user_id"]
            cabinet_id = item["cabinet_id"]
            preset_id = item["preset_id"]
            tokens = item.get("tokens") or []  # имена переменных из .env или сырые токены
            trigger_time = item.get("trigger_time") or item.get("time") or ""
            count_repeats = int(item.get("count_repeats") or 1)

            if not trigger_time:
                log.info("[SKIP] %s/%s: no trigger_time", user_id, cabinet_id)
                continue

            if not should_fire_now(trigger_time, now):
                log.debug("[WAIT] %s/%s: now not in trigger window (%s + %sh)", user_id, cabinet_id, trigger_time, TRIGGER_EXTRA_HOURS)
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
    load_env()
    log.info("auto_ads worker started. Tick each 60s.")
    while True:
        try:
            process_queue_once()
        except Exception as e:
            log.exception("Fatal error: %s", e)
        time.sleep(60)

if __name__ == "__main__":
    main_loop()
