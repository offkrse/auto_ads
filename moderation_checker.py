#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт проверки модерации кампаний VK Ads.

При запуске:
1. Просматривает /opt/auto_ads/data/check_moderation/
2. Проверяет статус кампаний через VK API
3. Если BANNED:
   - Записывает информацию о бане в sets.json (поле moderation)
   - Меняет хэш видео
   - Меняет текст (замена символов)
   - Создаёт one-shot пресет для автоматического пересоздания
4. Если ACTIVE:
   - Проверяет issues групп на NO_ALLOWED_BANNERS
   - Если есть NO_ALLOWED_BANNERS - обрабатываем как бан (rehash + создаём пресет в one_add_groups)
   - Если нет NO_ALLOWED_BANNERS - записываем APPROVED и удаляем файл
"""

import json
import os
import random
import re
import shutil
import subprocess
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import logging
from logging.handlers import RotatingFileHandler

import requests
from dateutil import tz
from filelock import FileLock
from dotenv import dotenv_values

# ============================ Конфигурация ============================

VERSION = "1.16"

CHECK_MODERATION_DIR = Path("/opt/auto_ads/data/check_moderation")
ONE_SHOT_PRESETS_DIR = Path("/opt/auto_ads/data/one_shot_presets")
ONE_ADD_GROUPS_DIR = Path("/opt/auto_ads/data/one_add_groups")
USERS_ROOT = Path("/opt/auto_ads/users")
ENV_FILE = Path("/opt/auto_ads/.env")
LOGS_DIR = Path("/opt/auto_ads/logs")
CREO_STORAGE_ROOT = Path("/mnt/data/auto_ads_storage/video")

API_BASE = os.getenv("VK_API_BASE", "https://ads.vk.com")
LOCAL_TZ = tz.gettz(os.getenv("LOCAL_TZ", "UTC"))

# Создаём директории
CHECK_MODERATION_DIR.mkdir(parents=True, exist_ok=True)
ONE_SHOT_PRESETS_DIR.mkdir(parents=True, exist_ok=True)
ONE_ADD_GROUPS_DIR.mkdir(parents=True, exist_ok=True)

# Дефолтные символы для замены (используются если не заданы в textset)
DEFAULT_SHORT_TEXT_SWAP = "🌟"
DEFAULT_SHORT_TEXT_SYMBOLS = "🌟;🔥;🏅;🚀;🥇;🌠;🎯;🎁"
DEFAULT_LONG_TEXT_SWAP = "🌟"
DEFAULT_LONG_TEXT_SYMBOLS = "🌟;🔥;🏅;🚀;🥇;🌠;🎯;🎁"

# Сдвиг времени для one-shot пресетов (часов от текущего времени)
ONE_SHOT_TIME_OFFSET_HOURS = 7

# Ретраи и таймауты
RETRY_MAX = 3
VK_HTTP_TIMEOUT = 60

# ============================ Логирование ============================

def setup_logger() -> logging.Logger:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("moderation_checker")
    
    if logger.handlers:
        return logger
    
    level = logging.INFO
    logger.setLevel(level)
    
    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Основной лог
    log_file = LOGS_DIR / "moderation_checker.log"
    file_handler = RotatingFileHandler(
        str(log_file), maxBytes=10*1024*1024, backupCount=5, encoding="utf-8"
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

# ============================ Утилиты ============================

_TOKENS: Dict[str, str] = {}

def load_tokens_from_envfile() -> None:
    global _TOKENS
    if ENV_FILE.exists():
        env_vals = dotenv_values(str(ENV_FILE))
        for k, v in env_vals.items():
            if k.startswith("VK_TOKEN_") and v:
                _TOKENS[k] = v

def get_real_token(token_name: str) -> Optional[str]:
    if token_name in _TOKENS:
        return _TOKENS[token_name]
    return os.getenv(token_name)

def load_json(path: Path) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def dump_json(path: Path, data: Any) -> None:
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)

def atomic_write_json(path: Path, data: Any) -> None:
    dump_json(path, data)

def get_cabinet_token(user_id: str, cabinet_id: str) -> Optional[str]:
    """Получает реальный токен для кабинета."""
    # Файл пользователя: /opt/auto_ads/users/<user_id>/<user_id>.json
    user_file = USERS_ROOT / str(user_id) / f"{user_id}.json"
    if not user_file.exists():
        log.error("User file not found: %s", user_file)
        return None
    try:
        user_data = load_json(user_file)
        cabinets = user_data.get("cabinets", [])
        for cab in cabinets:
            if str(cab.get("id")) == str(cabinet_id):
                token_name = cab.get("token")
                if token_name:
                    real_token = get_real_token(token_name)
                    if real_token:
                        return real_token
                    else:
                        log.error("Token %s not found in env", token_name)
                else:
                    log.error("No token name for cabinet %s", cabinet_id)
        log.error("Cabinet %s not found in user file", cabinet_id)
    except Exception as e:
        log.error("Failed to get cabinet token: %s", e)
    return None

# ============================ VK API ============================

def vk_api_get(endpoint: str, token: str, params: Optional[Dict] = None) -> Dict:
    """GET запрос к VK API."""
    url = f"{API_BASE}{endpoint}"
    headers = {"Authorization": f"Bearer {token}"}
    
    for attempt in range(RETRY_MAX):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=VK_HTTP_TIMEOUT)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code in (429, 500, 502, 503, 504):
                delay = (2 ** attempt) + random.uniform(0.1, 0.5)
                log.warning("VK API %s returned %s, retry %d/%d after %.2fs",
                           endpoint, resp.status_code, attempt+1, RETRY_MAX, delay)
                time.sleep(delay)
                continue
            log.error("VK API %s returned %s: %s", endpoint, resp.status_code, resp.text[:500])
            return {}
        except Exception as e:
            log.error("VK API %s exception: %s", endpoint, e)
            if attempt < RETRY_MAX - 1:
                time.sleep(2 ** attempt)
    return {}

def check_campaign_status(token: str, campaign_id: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Проверяет статус кампании.
    Возвращает кортеж (status, major_status).
    
    Пример ответа:
    {
      "vkads_status": {
        "codes": ["BANNED"],
        "major_status": "BANNED",
        "status": "ACTIVE"
      }
    }
    """
    params = {
        "_id__in": campaign_id,
        "fields": "id,name,vkads_status"
    }
    data = vk_api_get("/api/v2/ad_plans.json", token, params)
    
    items = data.get("items", [])
    if not items:
        return None, None
    
    item = items[0]
    vkads_status = item.get("vkads_status", {})
    status = vkads_status.get("status", "")
    major_status = vkads_status.get("major_status", "")
    
    log.info("Campaign %s status: %s, major_status: %s", campaign_id, status, major_status)
    return status, major_status

def get_ad_groups_issues(token: str, group_ids: List[str]) -> Dict[str, List[Dict]]:
    """
    Получает issues для групп объявлений.
    Возвращает dict: {group_id: [issues]}
    """
    if not group_ids:
        return {}
    
    params = {
        "_id__in": ",".join(group_ids),
        "fields": "id,name,issues"
    }
    data = vk_api_get("/api/v2/ad_groups.json", token, params)
    
    result = {}
    for item in data.get("items", []):
        group_id = str(item.get("id", ""))
        issues = item.get("issues", [])
        result[group_id] = issues
    
    return result

def get_ad_group_details(token: str, group_id: str) -> Optional[Dict]:
    """
    Получает детали группы: targetings и banners.
    """
    params = {
        "_id__in": group_id,
        "fields": "id,name,targetings,banners"
    }
    data = vk_api_get("/api/v2/ad_groups.json", token, params)
    
    items = data.get("items", [])
    if items:
        return items[0]
    return None

def get_banner_content(token: str, banner_id: str) -> Optional[Dict]:
    """
    Получает content баннера.
    """
    params = {
        "_id__in": banner_id,
        "fields": "id,name,content"
    }
    data = vk_api_get("/api/v2/banners.json", token, params)
    
    items = data.get("items", [])
    if items:
        return items[0]
    return None

def extract_media_id_from_content(content: Dict) -> Tuple[Optional[str], str]:
    """
    Извлекает video_id или image_id из content баннера.
    Возвращает (media_id, media_type) где media_type = 'video' или 'image'
    """
    if not content:
        return None, ""
    
    # Сначала ищем video_portrait_*
    for key, value in content.items():
        if key.startswith("video_portrait_") and isinstance(value, dict):
            media_id = value.get("id")
            if media_id:
                return str(media_id), "video"
    
    # Затем video_*
    for key, value in content.items():
        if key.startswith("video_") and isinstance(value, dict):
            media_id = value.get("id")
            if media_id:
                return str(media_id), "video"
    
    # Затем image_*
    for key, value in content.items():
        if key.startswith("image_") and isinstance(value, dict):
            media_id = value.get("id")
            if media_id:
                return str(media_id), "image"
    
    return None, ""

def extract_segments_from_targetings(targetings: Dict) -> List[int]:
    """Извлекает segments из targetings."""
    return targetings.get("segments", [])

# ============================ Работа с креативами ============================

def get_sets_path(user_id: str, cabinet_id: str) -> Path:
    return USERS_ROOT / str(user_id) / "creatives" / str(cabinet_id) / "sets.json"

def load_sets(user_id: str, cabinet_id: str) -> List[Dict]:
    path = get_sets_path(user_id, cabinet_id)
    if not path.exists():
        return []
    try:
        return load_json(path)
    except Exception as e:
        log.error("Failed to load sets.json: %s", e)
        return []

def save_sets(user_id: str, cabinet_id: str, sets: List[Dict]) -> None:
    path = get_sets_path(user_id, cabinet_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    lock = FileLock(str(path) + ".lock")
    with lock:
        atomic_write_json(path, sets)

def find_video_in_sets(sets: List[Dict], video_id: str, cabinet_id: str) -> Optional[Dict]:
    """Находит видео в sets.json по id."""
    for s in sets:
        for item in s.get("items", []):
            # Проверяем vkByCabinet
            vk_by_cabinet = item.get("vkByCabinet", {})
            if str(vk_by_cabinet.get(str(cabinet_id))) == str(video_id):
                return item
            # Проверяем id напрямую
            if str(item.get("id")) == str(video_id):
                return item
    return None

def update_moderation_status(
    sets: List[Dict],
    video_id: str,
    cabinet_id: str,
    objective: str,
    status: str,
    textset_id: str,
    text_short: str,
    text_long: str,
    original_video_id: str = ""
) -> bool:
    """
    Обновляет статус модерации для видео.
    Формат: moderation: [{objective: [{video_id, original_video_id, status, textset_id, text_short, text_long, timestamp}]}]
    """
    if not original_video_id:
        original_video_id = video_id
    
    timestamp = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
    
    for s in sets:
        for item in s.get("items", []):
            # Проверяем vkByCabinet
            vk_by_cabinet = item.get("vkByCabinet", {})
            item_match = (
                str(vk_by_cabinet.get(str(cabinet_id))) == str(video_id) or
                str(item.get("id")) == str(video_id) or
                str(vk_by_cabinet.get(str(cabinet_id))) == str(original_video_id) or
                str(item.get("id")) == str(original_video_id)
            )
            
            if item_match:
                # Инициализируем moderation если нет
                if "moderation" not in item:
                    item["moderation"] = [
                        {"leadads": []},
                        {"site_conversions": []},
                        {"socialengagement": []}
                    ]
                
                # Находим нужный objective
                for mod_entry in item["moderation"]:
                    if objective in mod_entry:
                        mod_list = mod_entry[objective]
                        # Добавляем запись
                        mod_list.append({
                            "video_id": str(video_id),
                            "original_video_id": str(original_video_id),
                            "status": status,
                            "textset_id": str(textset_id),
                            "text_short": text_short,
                            "text_long": text_long,
                            "timestamp": timestamp
                        })
                        return True
    return False

def get_used_texts(sets: List[Dict], video_id: str, cabinet_id: str, objective: str) -> List[Tuple[str, str]]:
    """Возвращает список уже использованных текстов (short, long) для видео."""
    used = []
    for s in sets:
        for item in s.get("items", []):
            vk_by_cabinet = item.get("vkByCabinet", {})
            item_match = (
                str(vk_by_cabinet.get(str(cabinet_id))) == str(video_id) or
                str(item.get("id")) == str(video_id)
            )
            
            if item_match and "moderation" in item:
                for mod_entry in item["moderation"]:
                    if objective in mod_entry:
                        for record in mod_entry[objective]:
                            used.append((
                                record.get("text_short", ""),
                                record.get("text_long", "")
                            ))
    return used

# ============================ Замена текста ============================

def get_next_symbol(current_text: str, swap_char: str, symbols_str: str, used_texts: List[str]) -> str:
    """
    Заменяет swap_char на следующий доступный символ из symbols_str.
    Проверяет что получившийся текст не использовался ранее.
    """
    symbols = [s.strip() for s in symbols_str.split(";") if s.strip()]
    
    for symbol in symbols:
        new_text = current_text.replace(swap_char, symbol, 1)
        if new_text not in used_texts:
            return new_text
    
    # Если все символы использованы, добавляем случайный в конец
    random_symbol = random.choice(symbols)
    return current_text + random_symbol

def swap_text_symbols(
    short_desc: str,
    long_desc: str,
    used_texts: List[Tuple[str, str]],
    textset: Optional[Dict] = None
) -> Tuple[str, str]:
    """
    Заменяет символы в текстах, избегая уже использованных комбинаций.
    Берёт настройки символов из textset, если они заданы.
    """
    # Получаем настройки из textset или используем дефолтные
    if textset:
        short_swap = textset.get("short_text_swap", DEFAULT_SHORT_TEXT_SWAP)
        short_symbols = textset.get("short_text_symbols", DEFAULT_SHORT_TEXT_SYMBOLS)
        long_swap = textset.get("long_text_swap", DEFAULT_LONG_TEXT_SWAP)
        long_symbols = textset.get("long_text_symbols", DEFAULT_LONG_TEXT_SYMBOLS)
    else:
        short_swap = DEFAULT_SHORT_TEXT_SWAP
        short_symbols = DEFAULT_SHORT_TEXT_SYMBOLS
        long_swap = DEFAULT_LONG_TEXT_SWAP
        long_symbols = DEFAULT_LONG_TEXT_SYMBOLS
    
    used_shorts = [t[0] for t in used_texts]
    used_longs = [t[1] for t in used_texts]
    
    new_short = get_next_symbol(short_desc, short_swap, short_symbols, used_shorts)
    new_long = get_next_symbol(long_desc, long_swap, long_symbols, used_longs)
    
    return new_short, new_long

# ============================ Смена хэша видео ============================

def cabinet_storage(cabinet_id: str) -> Path:
    return CREO_STORAGE_ROOT / str(cabinet_id)

def find_local_video_id_by_vk_id(sets: List[Dict], vk_video_id: str, cabinet_id: str) -> Optional[str]:
    """
    Находит локальный ID видео по VK ID из sets.json.
    
    В sets.json видео хранится так:
    {
        "id": "id_abc123",          // локальный ID
        "vkByCabinet": {
            "21799870": "102924861"  // cabinet_id -> VK ID
        }
    }
    """
    for s in sets:
        for item in s.get("items", []):
            vk_by_cabinet = item.get("vkByCabinet", {})
            if str(vk_by_cabinet.get(str(cabinet_id))) == str(vk_video_id):
                local_id = item.get("id")
                if local_id:
                    return str(local_id)
    return None


# Кэш для уже обработанных video_id в текущем запуске
# {old_video_id: new_video_id}
_rehash_cache: Dict[str, str] = {}


def clear_rehash_cache() -> None:
    """Очищает кэш rehash (вызывать в начале обработки)."""
    global _rehash_cache
    _rehash_cache = {}


def rehash_video(
    user_id: str,
    cabinet_id: str,
    video_id: str,
    token: str
) -> Optional[Dict]:
    """
    Создаёт копию видео с новым хэшом и загружает в VK.
    
    Логика:
    1. Проверяем кэш - если video_id уже обрабатывался, возвращаем закэшированный результат
    2. Ищем файл: /mnt/data/auto_ads_storage/video/<cabinet_id>/<video_id>_<name>.<ext>
    3. Создаём временный файл: temp_<random_id>_<name>.<ext> в той же директории
    4. Ремуксим через ffmpeg (меняет хэш)
    5. Загружаем в VK
    6. Удаляем временный файл
    7. Сохраняем результат в кэш
    
    Возвращает информацию о новом видео или None при ошибке.
    """
    global _rehash_cache
    
    # Проверяем кэш
    if video_id in _rehash_cache:
        cached_new_id = _rehash_cache[video_id]
        log.info("Using cached rehash result: %s -> %s", video_id, cached_new_id)
        return {
            "old_vk_id": video_id,
            "new_vk_id": cached_new_id,
            "vk_response": {},
            "from_cache": True
        }
    
    storage = cabinet_storage(cabinet_id)
    
    # Файлы на диске называются {vk_id}_{original_name}
    video_file = None
    
    for f in storage.glob(f"{video_id}_*"):
        if f.is_file() and not f.name.endswith(".json") and not f.name.endswith(".jpg"):
            video_file = f
            break
    
    if not video_file:
        log.error("Video file not found for video_id=%s in %s", video_id, storage)
        # Выводим список файлов для отладки
        try:
            files = list(storage.glob("*"))[:20]
            log.error("Available files in storage: %s", [f.name for f in files])
        except:
            pass
        return None
    
    log.info("Found video file: %s", video_file)
    
    # Читаем мету
    base_no_ext = video_file.stem
    meta_path = storage / f"{base_no_ext}.json"
    
    if meta_path.exists():
        try:
            meta = load_json(meta_path)
        except Exception as e:
            log.error("Failed to read meta for %s: %s", video_file, e)
            meta = {}
    else:
        meta = {}
    
    width = int(meta.get("width") or 720)
    height = int(meta.get("height") or 1280)
    
    # Получаем original_name (часть после vk_id_)
    original_name = video_file.name.split("_", 1)[1] if "_" in video_file.name else video_file.name
    
    # Создаём временный файл В ТОЙ ЖЕ ДИРЕКТОРИИ
    random_id = random.randint(100000, 999999)
    temp_filename = f"temp_{random_id}_{original_name}"
    temp_path = storage / temp_filename
    
    try:
        # Ремультиплекс через ffmpeg для изменения хэша
        log.info("Remuxing video to %s", temp_path)
        proc = subprocess.run(
            [
                "ffmpeg", "-y",
                "-i", str(video_file),
                "-c", "copy",
                "-map_metadata", "-1",  # убираем метаданные для изменения хэша
                str(temp_path),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if proc.returncode != 0:
            log.error("ffmpeg remux failed: %s", proc.stderr[:500])
            return None
        
        if not temp_path.exists():
            log.error("Temp file was not created: %s", temp_path)
            return None
        
        log.info("Temp file created: %s (size=%d)", temp_path, temp_path.stat().st_size)
        
        # Загружаем в VK
        headers = {"Authorization": f"Bearer {token}"}
        vk_url = f"{API_BASE}/api/v2/content/video.json"
        
        with open(temp_path, "rb") as fh:
            files = {
                "file": (original_name, fh, "video/mp4"),
                "data": (None, json.dumps({"width": width, "height": height}), "application/json"),
            }
            resp = requests.post(vk_url, headers=headers, files=files, timeout=180)
        
        if resp.status_code != 200:
            log.error("VK upload failed: %s %s", resp.status_code, resp.text[:300])
            return None
        
        resp_json = resp.json()
        log.info("VK upload response: %s", json.dumps(resp_json, ensure_ascii=False)[:500])
        new_vk_id = str(resp_json.get("id") or "").strip()
        
        if not new_vk_id:
            log.error("VK did not return id in response: %s", resp_json)
            return None
        
        log.info("Video rehashed: %s -> %s", video_id, new_vk_id)
        
        # Сохраняем в кэш
        _rehash_cache[video_id] = new_vk_id
        
        return {
            "old_vk_id": video_id,
            "new_vk_id": new_vk_id,
            "vk_response": resp_json,
        }
        
    except Exception as e:
        log.error("rehash_video exception: %s", e)
        return None
    finally:
        # Удаляем временный файл
        try:
            if temp_path.exists():
                temp_path.unlink()
                log.info("Deleted temp file: %s", temp_path)
        except Exception as e:
            log.warning("Failed to delete temp file %s: %s", temp_path, e)

# ============================ Textsets ============================

def get_textsets_path(user_id: str, cabinet_id: str) -> Path:
    return USERS_ROOT / str(user_id) / "presets" / str(cabinet_id) / "textsets.json"

def load_textsets(user_id: str, cabinet_id: str) -> List[Dict]:
    path = get_textsets_path(user_id, cabinet_id)
    if not path.exists():
        return []
    try:
        return load_json(path)
    except:
        return []

def find_textset(textsets: List[Dict], textset_id: str) -> Optional[Dict]:
    for ts in textsets:
        if ts.get("id") == textset_id:
            return ts
    return None

# ============================ One-shot пресеты ============================

def create_one_shot_preset(
    user_id: str,
    cabinet_id: str,
    preset_id: str,
    original_preset: Dict,
    new_video_id: str,
    old_video_id: str,
    original_video_id: str,
    new_short: str,
    new_long: str,
    textset_id: str
) -> Optional[Path]:
    """
    Создаёт one-shot пресет с изменённым видео и текстом.
    Время устанавливается на +7 часов от текущего.
    """
    try:
        # Копируем пресет
        new_preset = json.loads(json.dumps(original_preset, ensure_ascii=False))
        
        # Добавляем user_id и cabinet_id для cyclop
        new_preset["_user_id"] = str(user_id)
        new_preset["_cabinet_id"] = str(cabinet_id)
        
        # Добавляем информацию об оригинальном видео
        new_preset["_moderation_info"] = {
            "original_video_id": original_video_id,
            "old_video_id": old_video_id,
            "new_video_id": new_video_id,
        }
        
        # Обновляем время
        trigger_time = datetime.now(LOCAL_TZ) + timedelta(hours=ONE_SHOT_TIME_OFFSET_HOURS)
        new_preset["company"]["time"] = trigger_time.strftime("%H:%M")
        
        # Обновляем видео и текст во всех объявлениях
        for ad in new_preset.get("ads", []):
            # Заменяем video_id
            video_ids = ad.get("videoIds", [])
            if old_video_id in video_ids:
                ad["videoIds"] = [new_video_id if v == old_video_id else v for v in video_ids]
            
            # Заменяем текст если textset совпадает
            if ad.get("textSetId") == textset_id or not textset_id:
                ad["shortDescription"] = new_short
                ad["longDescription"] = new_long
        
        # Сохраняем
        random_id = random.randint(100000, 999999)
        filename = f"os_preset_{random_id}.json"
        filepath = ONE_SHOT_PRESETS_DIR / filename
        
        dump_json(filepath, new_preset)
        
        log.info("Created one-shot preset: %s", filepath)
        return filepath
        
    except Exception as e:
        log.error("Failed to create one-shot preset: %s", e)
        return None

def create_add_group_preset(
    user_id: str,
    cabinet_id: str,
    preset_id: str,
    original_preset: Dict,
    new_video_id: str,
    old_video_id: str,
    original_video_id: str,
    new_short: str,
    new_long: str,
    textset_id: str,
    segments: List[int],
    ad_plan_id: str = ""
) -> Optional[Path]:
    """
    Создаёт пресет для добавления группы с обновлёнными видео и сегментами.
    Сохраняется в /opt/auto_ads/data/one_add_groups/
    """
    try:
        # Копируем пресет
        new_preset = json.loads(json.dumps(original_preset, ensure_ascii=False))
        
        # Добавляем user_id и cabinet_id для cyclop
        new_preset["_user_id"] = str(user_id)
        new_preset["_cabinet_id"] = str(cabinet_id)
        
        # Добавляем информацию для добавления группы
        new_preset["_moderation_info"] = {
            "original_video_id": original_video_id,
            "old_video_id": old_video_id,
            "new_video_id": new_video_id,
            "segments": segments,
            "ad_plan_id": ad_plan_id,
        }
        
        # Обновляем время
        trigger_time = datetime.now(LOCAL_TZ) + timedelta(hours=ONE_SHOT_TIME_OFFSET_HOURS)
        new_preset["company"]["time"] = trigger_time.strftime("%H:%M")
        
        # Обновляем видео и текст во всех объявлениях
        for ad in new_preset.get("ads", []):
            # Заменяем video_id на новый
            ad["videoIds"] = [new_video_id]
            
            # Заменяем текст если textset совпадает
            if ad.get("textSetId") == textset_id or not textset_id:
                ad["shortDescription"] = new_short
                ad["longDescription"] = new_long
        
        # Обновляем сегменты в группах
        if segments:
            for group in new_preset.get("groups", []):
                group["audienceIds"] = segments
        
        # Сохраняем
        random_id = random.randint(100000, 999999)
        filename = f"add_group_{random_id}.json"
        filepath = ONE_ADD_GROUPS_DIR / filename
        
        dump_json(filepath, new_preset)
        
        log.info("Created add-group preset: %s (ad_plan_id=%s)", filepath, ad_plan_id)
        return filepath
        
    except Exception as e:
        log.error("Failed to create add-group preset: %s", e)
        return None

# ============================ Обработка забаненных групп ============================

def process_banned_group(
    token: str,
    user_id: str,
    cabinet_id: str,
    preset_id: str,
    preset: Dict,
    group_id: str,
    ad_data: Dict,
    sets: List[Dict],
    objective: str,
    is_no_allowed_banners: bool = False,
    company_id: str = ""
) -> bool:
    """
    Обрабатывает забаненную группу или группу с NO_ALLOWED_BANNERS.
    
    Возвращает True если обработка успешна.
    """
    video_id = ad_data.get("video_id", "")
    original_video_id = ad_data.get("original_video_id", video_id)
    textset_id = ad_data.get("textset_id", "")
    short_desc = ad_data.get("short_description", "")
    long_desc = ad_data.get("long_description", "")
    segments = []
    
    # Загружаем textset для получения настроек символов
    textsets = load_textsets(user_id, cabinet_id)
    textset = find_textset(textsets, textset_id) if textset_id else None
    
    # Если NO_ALLOWED_BANNERS - получаем данные из VK API
    if is_no_allowed_banners:
        log.info("Processing NO_ALLOWED_BANNERS for group %s", group_id)
        
        # Получаем детали группы
        group_details = get_ad_group_details(token, group_id)
        if not group_details:
            log.error("Could not get details for group %s", group_id)
            return False
        
        # Извлекаем segments
        targetings = group_details.get("targetings", {})
        segments = extract_segments_from_targetings(targetings)
        log.info("Group %s segments: %s", group_id, segments)
        
        # Получаем баннеры
        banners = group_details.get("banners", [])
        if not banners:
            log.error("No banners in group %s", group_id)
            return False
        
        # Берём первый баннер
        banner_info = banners[0]
        banner_id = str(banner_info.get("id", ""))
        
        if banner_id:
            # Получаем content баннера
            banner_data = get_banner_content(token, banner_id)
            if banner_data:
                content = banner_data.get("content", {})
                media_id, media_type = extract_media_id_from_content(content)
                
                if media_id:
                    if media_type == "video":
                        video_id = media_id
                        if not original_video_id:
                            original_video_id = media_id
                    log.info("Extracted %s id: %s from banner %s", media_type, media_id, banner_id)
    
    if not video_id:
        log.warning("No video_id for group %s, skipping", group_id)
        return False
    
    log.info("Processing banned content: video_id=%s, original=%s", video_id, original_video_id)
    
    # Получаем уже использованные тексты
    used_texts = get_used_texts(sets, original_video_id, cabinet_id, objective)
    
    # Записываем статус BANNED
    update_moderation_status(
        sets, video_id, cabinet_id, objective,
        "BANNED", textset_id, short_desc, long_desc, original_video_id
    )
    
    # Меняем хэш видео
    rehash_result = rehash_video(user_id, cabinet_id, video_id, token)
    
    if rehash_result:
        new_video_id = rehash_result["new_vk_id"]
        
        # Меняем текст (передаём textset для получения настроек символов)
        new_short, new_long = swap_text_symbols(short_desc, long_desc, used_texts, textset)
        
        # Создаём пресет в зависимости от типа проблемы
        if is_no_allowed_banners:
            # Создаём пресет для добавления группы с сегментами
            create_add_group_preset(
                user_id, cabinet_id, preset_id, preset,
                new_video_id, video_id, original_video_id,
                new_short, new_long, textset_id, segments,
                ad_plan_id=company_id
            )
        else:
            # Создаём one-shot пресет
            create_one_shot_preset(
                user_id, cabinet_id, preset_id, preset,
                new_video_id, video_id, original_video_id,
                new_short, new_long, textset_id
            )
        
        return True
    else:
        log.error("Failed to rehash video %s", video_id)
        return False

# ============================ Основная логика ============================

def process_moderation_file(filepath: Path) -> bool:
    """
    Обрабатывает один файл из check_moderation.
    Возвращает True если файл можно удалить (обработан или устарел).
    """
    try:
        data = load_json(filepath)
    except Exception as e:
        log.error("Failed to read %s: %s", filepath, e)
        return True  # Удаляем битый файл
    
    user_id = data.get("user_id")
    cabinet_id = data.get("cabinet_id")
    preset_id = data.get("preset_id")
    preset = data.get("preset", {})
    company_ids = data.get("company_ids", [])
    ad_groups_ids = data.get("ad_groups_ids", [])
    
    if not user_id or not cabinet_id or not company_ids:
        log.warning("Invalid data in %s", filepath)
        return True
    
    # Получаем токен
    token = get_cabinet_token(user_id, cabinet_id)
    if not token:
        log.error("No token for user %s cabinet %s", user_id, cabinet_id)
        return False  # Не удаляем, попробуем позже
    
    # Очищаем кэш rehash для этого файла (чтобы одинаковые video_id в одном файле 
    # использовали один и тот же новый video_id)
    clear_rehash_cache()
    
    objective = preset.get("company", {}).get("targetAction", "socialengagement")
    
    # Загружаем sets.json
    sets = load_sets(user_id, cabinet_id)
    
    should_delete = True  # По умолчанию удаляем после обработки
    
    # Проверяем каждую кампанию
    for company_id in company_ids:
        status, major_status = check_campaign_status(token, company_id)
        
        if status is None:
            log.warning("Could not get status for campaign %s", company_id)
            should_delete = False  # Не удаляем, попробуем позже
            continue
        
        # Кампания полностью забанена
        if status == "BANNED":
            log.info("Campaign %s is BANNED (status=BANNED)", company_id)
            
            # Обрабатываем каждую группу объявлений
            for ag_info in ad_groups_ids:
                for ag_id, ad_data in ag_info.items():
                    process_banned_group(
                        token, user_id, cabinet_id, preset_id, preset,
                        ag_id, ad_data, sets, objective,
                        is_no_allowed_banners=False,
                        company_id=company_id
                    )
        
        # major_status=BANNED но status не BANNED - проверяем каждую группу
        elif major_status == "BANNED":
            log.info("Campaign %s has major_status=BANNED, checking each group", company_id)
            
            # Получаем все group_ids
            group_ids = []
            for ag_info in ad_groups_ids:
                for ag_id in ag_info.keys():
                    group_ids.append(ag_id)
            
            if not group_ids:
                log.warning("No group_ids found for campaign %s", company_id)
                continue
            
            # Проверяем issues групп
            issues_by_group = get_ad_groups_issues(token, group_ids)
            
            groups_with_problems = []
            
            for ag_id, issues in issues_by_group.items():
                for issue in issues:
                    if issue.get("code") == "NO_ALLOWED_BANNERS":
                        groups_with_problems.append(ag_id)
                        log.info("Group %s has NO_ALLOWED_BANNERS", ag_id)
            
            if groups_with_problems:
                # Обрабатываем группы с проблемами
                for ag_info in ad_groups_ids:
                    for ag_id, ad_data in ag_info.items():
                        if ag_id in groups_with_problems:
                            process_banned_group(
                                token, user_id, cabinet_id, preset_id, preset,
                                ag_id, ad_data, sets, objective,
                                is_no_allowed_banners=True,
                                company_id=company_id
                            )
                # Не удаляем файл, так как есть проблемы
                should_delete = False
            else:
                log.warning("Campaign %s has major_status=BANNED but no groups with NO_ALLOWED_BANNERS", company_id)
                should_delete = False
        
        elif status == "ACTIVE":
            log.info("Campaign %s is ACTIVE, checking groups for NO_ALLOWED_BANNERS", company_id)
            
            # Получаем все group_ids
            group_ids = []
            for ag_info in ad_groups_ids:
                for ag_id in ag_info.keys():
                    group_ids.append(ag_id)
            
            if not group_ids:
                log.warning("No group_ids found for campaign %s", company_id)
                continue
            
            # Проверяем issues групп
            issues_by_group = get_ad_groups_issues(token, group_ids)
            
            has_no_allowed_banners = False
            groups_with_problems = []
            
            for ag_id, issues in issues_by_group.items():
                for issue in issues:
                    if issue.get("code") == "NO_ALLOWED_BANNERS":
                        has_no_allowed_banners = True
                        groups_with_problems.append(ag_id)
                        log.info("Group %s has NO_ALLOWED_BANNERS", ag_id)
            
            if has_no_allowed_banners:
                # Обрабатываем группы с проблемами
                for ag_info in ad_groups_ids:
                    for ag_id, ad_data in ag_info.items():
                        if ag_id in groups_with_problems:
                            process_banned_group(
                                token, user_id, cabinet_id, preset_id, preset,
                                ag_id, ad_data, sets, objective,
                                is_no_allowed_banners=True,
                                company_id=company_id
                            )
                # Не удаляем файл, так как есть проблемы
                should_delete = False
            else:
                # Все группы прошли модерацию - записываем APPROVED
                log.info("All groups in campaign %s passed moderation", company_id)
                
                for ag_info in ad_groups_ids:
                    for ag_id, ad_data in ag_info.items():
                        video_id = ad_data.get("video_id", "")
                        original_video_id = ad_data.get("original_video_id", video_id)
                        textset_id = ad_data.get("textset_id", "")
                        short_desc = ad_data.get("short_description", "")
                        long_desc = ad_data.get("long_description", "")
                        
                        if video_id:
                            update_moderation_status(
                                sets, video_id, cabinet_id, objective,
                                "APPROVED", textset_id, short_desc, long_desc, original_video_id
                            )
        else:
            # Другой статус (PENDING и т.д.) - не удаляем, проверим позже
            log.info("Campaign %s has status %s, will check later", company_id, status)
            should_delete = False
    
    # Сохраняем обновлённый sets.json
    save_sets(user_id, cabinet_id, sets)
    
    return should_delete

def process_all_moderation_files() -> None:
    """Обрабатывает все файлы в check_moderation."""
    if not CHECK_MODERATION_DIR.exists():
        log.debug("Check moderation dir does not exist")
        return
    
    files = list(CHECK_MODERATION_DIR.glob("company_*.json"))
    log.info("Found %d moderation files to process", len(files))
    
    for filepath in files:
        log.info("Processing: %s", filepath.name)
        try:
            should_delete = process_moderation_file(filepath)
            if should_delete:
                filepath.unlink()
                log.info("Deleted processed file: %s", filepath.name)
        except Exception as e:
            log.exception("Error processing %s: %s", filepath.name, e)

def main() -> None:
    """Точка входа."""
    load_tokens_from_envfile()
    log.info("Moderation checker v%s started", VERSION)
    
    process_all_moderation_files()
    
    log.info("Moderation checker finished")

if __name__ == "__main__":
    main()
