from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Depends, APIRouter, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from datetime import datetime
from urllib.parse import quote, parse_qsl
from pathlib import Path
from io import BytesIO
from PIL import Image
import hmac, hashlib
import requests
import subprocess
import tempfile
import json
import shutil
import uvicorn
import os
import fcntl
import errno
import uuid

app = FastAPI()

VersionApp = "0.79"
BASE_DIR = Path("/opt/auto_ads")
USERS_DIR = BASE_DIR / "users"
USERS_DIR.mkdir(parents=True, exist_ok=True)

LOG_DIR = Path("/opt/auto_ads/logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
GLOBAL_LOG = LOG_DIR / "global_error.log"

LOGO_STORAGE_DIR = Path("/mnt/data/auto_ads_storage/logo")
LOGO_STORAGE_DIR.mkdir(parents=True, exist_ok=True)

DATA_DIR = Path("/opt/auto_ads/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)
GLOBAL_QUEUE_FILE = DATA_DIR / "global_queue.json"

STORAGE_DIR = Path("/mnt/data/auto_ads_storage/video")
STORAGE_DIR.mkdir(parents=True, exist_ok=True)

INTERESTS_FILE = DATA_DIR / "interests.json"
REGIONS_FILE   = DATA_DIR / "regions.json"

FRONTEND_DIR = BASE_DIR / "frontend"

# === env ===
from dotenv import load_dotenv
load_dotenv("/opt/auto_ads/.env")

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
# -------------------------------------
#   HELPERS
# -------------------------------------
def _safe_unlink(p: Path):
    try:
        if p.exists():
            p.unlink()
    except Exception as e:
        log_error(f"safe_unlink failed for {p}: {repr(e)}")

def next_display_name(storage: Path, original: str) -> str:
    """
    Возвращает имя для отображения (с автонумерацией " (2)", " (3)", ...)
    на основании файлов, уже лежащих в storage. Смотрим на оригинальные
    имена (часть после первого "vk_id_"), чтобы избежать дублей в UI.
    """
    base, ext = os.path.splitext(original)
    # Собираем все использованные display-имена в этой папке
    used: set[str] = set()
    for f in storage.glob("*"):
        if not f.is_file():
            continue
        name = f.name
        # У нас файлы имеют вид "<vkid>_Оригинал.ext" или "<vkid>_Оригинал (n).ext"
        if "_" in name:
            disp = name.split("_", 1)[1]
            used.add(disp)
    # Если такого имени ещё нет — возвращаем как есть
    candidate = f"{base}{ext}"
    if candidate not in used:
        return candidate
    # Иначе подбираем с суффиксом (2), (3), ...
    n = 2
    while True:
        candidate = f"{base} ({n}){ext}"
        if candidate not in used:
            return candidate
        n += 1

def check_telegram_init_data(init_data: str) -> dict:
    """
    Возвращает dict с данными, если подпись валидна. Иначе бросает HTTPException(401).
    """
    if not init_data or not BOT_TOKEN:
        raise HTTPException(401, "Missing Telegram auth")

    # Раскладываем initData в пары
    data = dict(parse_qsl(init_data, keep_blank_values=True))
    hash_recv = data.pop("hash", None)
    if not hash_recv:
        raise HTTPException(401, "Bad Telegram auth")

    # Строим data_check_string
    pairs = [f"{k}={v}" for k, v in sorted(data.items())]
    data_check_string = "\n".join(pairs)

    secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
    hash_calc = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()

    if not hmac.compare_digest(hash_calc, hash_recv):
        raise HTTPException(401, "Invalid Telegram signature")

    # Можно ещё проверить свежесть auth_date (например ±1 день)
    return data  # тут, например, есть user, auth_date и пр.

def require_tg_user(
    init_data: str = Query(None, alias="init_data"),
    request: Request = None
):
    # допускаем и query (?init_data=...), и заголовок, который шлёт фронт
    if init_data is None and request is not None:
        # Starlette приводит имена к lower-case
        init_data = request.headers.get("x-tg-init-data") or request.headers.get("x-telegram-init")
    data = check_telegram_init_data(init_data)
    return data

def read_history_file(user_id: str, cabinet_id: str):
  p = USERS_DIR / user_id / "created_company" / cabinet_id / "created.json"
  if not p.exists():
    return []
  try:
    raw = p.read_text(encoding="utf-8").strip()
    if not raw:
      return []
    data = json.loads(raw)
    if isinstance(data, dict) and "items" in data and isinstance(data["items"], list):
      return data["items"]
    if isinstance(data, list):
      return data
    return []
  except Exception as e:
    # можно логировать e
    return []

def logo_storage(cabinet_id: str) -> Path:
    p = LOGO_STORAGE_DIR / str(cabinet_id)
    p.mkdir(parents=True, exist_ok=True)
    return p

def logo_meta_path(user_id: str, cabinet_id: str) -> Path:
    return USERS_DIR / user_id / "creatives" / cabinet_id / "logo.json"


def log_error(msg: str):
    try:
        with open(GLOBAL_LOG, "a", encoding="utf-8") as fh:
            ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
            fh.write(f"[{ts}] {msg}\n")
    except Exception:
        pass  # последняя линия обороны — лог просто пропускаем

class file_lock:
    """Простейшая advisory-блокировка на файле (Unix)."""
    def __init__(self, path: Path):
        self._path = path
        self._fh = None
    def __enter__(self):
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self._path, "a+")  # создаём, если нет
        while True:
            try:
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX)
                break
            except OSError as e:
                if e.errno in (errno.EINTR, errno.EAGAIN):
                    continue
                raise
        return self._fh
    def __exit__(self, exc_type, exc, tb):
        try:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        finally:
            self._fh.close()

def atomic_write_json(dst: Path, obj: dict | list):
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False, indent=2)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, dst)  # атомарная замена

def _replace_media_id_in_presets_for_cab(user_id: str, cabinet_id: str, mapping: dict[str, str]):
    """
    Заменяет старые id на новые ТОЛЬКО в пресетах cabinet_id.
    mapping: {old_id: new_id}
    """
    if not mapping:
        return
    pdir = USERS_DIR / user_id / "presets" / str(cabinet_id)
    if not pdir.exists():
        return
    for f in pdir.glob("*.json"):
        try:
            with open(f, "r", encoding="utf-8") as fh:
                preset = json.load(fh)
        except Exception as e:
            log_error(f"_replace_media_id_in_presets_for_cab: read error {f}: {repr(e)}")
            continue
        if not isinstance(preset, dict):
            continue
        ads = preset.get("ads")
        if not isinstance(ads, list):
            continue

        changed = False
        for ad in ads:
            imgs = ad.get("imageIds")
            if isinstance(imgs, list):
                new_imgs = [mapping.get(str(x), x) for x in imgs]
                if new_imgs != imgs:
                    ad["imageIds"] = new_imgs
                    changed = True
            vids = ad.get("videoIds")
            if isinstance(vids, list):
                new_vids = [mapping.get(str(x), x) for x in vids]
                if new_vids != vids:
                    ad["videoIds"] = new_vids
                    changed = True
        if changed:
            try:
                atomic_write_json(f, preset)
            except Exception as e:
                log_error(f"_replace_media_id_in_presets_for_cab: write error {f}: {repr(e)}")


def _drop_media_id_in_presets_for_cab(user_id: str, cabinet_id: str, media_id: str):
    """
    Удаляет media_id из imageIds/videoIds во всех preset_*.json данного cabinet_id.
    """
    pdir = USERS_DIR / user_id / "presets" / str(cabinet_id)
    if not pdir.exists():
        return
    for f in pdir.glob("*.json"):
        try:
            with open(f, "r", encoding="utf-8") as fh:
                preset = json.load(fh)
        except Exception as e:
            log_error(f"_drop_media_id_in_presets_for_cab: read error {f}: {repr(e)}")
            continue
        if not isinstance(preset, dict):
            continue
        ads = preset.get("ads")
        if not isinstance(ads, list):
            continue

        changed = False
        for ad in ads:
            imgs = ad.get("imageIds")
            if isinstance(imgs, list):
                new_imgs = [x for x in imgs if str(x) != str(media_id)]
                if new_imgs != imgs:
                    ad["imageIds"] = new_imgs
                    changed = True
            vids = ad.get("videoIds")
            if isinstance(vids, list):
                new_vids = [x for x in vids if str(x) != str(media_id)]
                if new_vids != vids:
                    ad["videoIds"] = new_vids
                    changed = True
        if changed:
            try:
                atomic_write_json(f, preset)
            except Exception as e:
                log_error(f"_drop_media_id_in_presets_for_cab: write error {f}: {repr(e)}")

def _update_presets_image_ids(user_id: str, mapping: dict[str, str]):
    """
    Проходит по ВСЕМ пресетам пользователя и заменяет значения в imageIds
    согласно mapping {old_vk_id: new_vk_id}.
    """
    if not mapping:
        return

    data = ensure_user_structure(user_id)
    cabinets = data.get("cabinets", []) or []

    for cab in cabinets:
        cab_id = str(cab.get("id"))
        pdir = USERS_DIR / user_id / "presets" / cab_id
        if not pdir.exists():
            continue

        for f in pdir.glob("*.json"):
            try:
                with open(f, "r", encoding="utf-8") as fh:
                    preset = json.load(fh)
            except Exception as e:
                log_error(f"_update_presets_image_ids: read error {f}: {repr(e)}")
                continue

            if not isinstance(preset, dict):
                continue

            ads = preset.get("ads")
            if not isinstance(ads, list):
                continue

            changed = False
            for ad in ads:
                imgs = ad.get("imageIds")
                if not isinstance(imgs, list):
                    continue
                for i, img_id in enumerate(imgs):
                    s_id = str(img_id)
                    if s_id in mapping:
                        imgs[i] = mapping[s_id]
                        changed = True

            if changed:
                try:
                    atomic_write_json(f, preset)
                except Exception as e:
                    log_error(f"_update_presets_image_ids: write error {f}: {repr(e)}")

def _update_presets_video_ids(user_id: str, mapping: dict[str, str]):
    """
    Проходит по ВСЕМ пресетам пользователя и заменяет значения в videoIds
    согласно mapping {old_vk_id: new_vk_id}.
    """
    if not mapping:
        return

    data = ensure_user_structure(user_id)
    cabinets = data.get("cabinets", []) or []

    for cab in cabinets:
        cab_id = str(cab.get("id"))
        pdir = USERS_DIR / user_id / "presets" / cab_id
        if not pdir.exists():
            continue

        for f in pdir.glob("*.json"):
            try:
                with open(f, "r", encoding="utf-8") as fh:
                    preset = json.load(fh)
            except Exception as e:
                log_error(f"_update_presets_video_ids: read error {f}: {repr(e)}")
                continue

            if not isinstance(preset, dict):
                continue

            ads = preset.get("ads")
            if not isinstance(ads, list):
                continue

            changed = False
            for ad in ads:
                vids = ad.get("videoIds")
                if not isinstance(vids, list):
                    continue
                for i, vid in enumerate(vids):
                    svid = str(vid)
                    if svid in mapping:
                        vids[i] = mapping[svid]
                        changed = True

            if changed:
                try:
                    atomic_write_json(f, preset)
                except Exception as e:
                    log_error(f"_update_presets_video_ids: write error {f}: {repr(e)}")

VIDEO_EXTS = (".mp4", ".mov", ".m4v", ".webm", ".avi", ".mkv")

def _rehash_one_file(user_id: str, cabinet_id: str, fname: str) -> dict:
    """
    Пере-заливает один файл (картинка/видео) в VK так, чтобы получился НОВЫЙ id.
    1) Берём старый файл + мету.
    2) Делаем "слегка изменённую" копию во временный файл (для image — пересохраняем через PIL).
    3) Заливаем временный файл в VK.
    4) Создаём новый локальный файл с new_vk_id_тем_же_отображаемым_именем.
    5) Удаляем старый файл/мету/превью.
    6) Возвращаем {old_vk_id, new_vk_id, final_name, meta}.
    """
    storage = cabinet_storage(cabinet_id)
    file_path = storage / fname
    if not file_path.exists():
        raise HTTPException(404, f"File {fname} not found")

    base_no_ext, ext = os.path.splitext(fname)
    meta_path = storage / f"{base_no_ext}.json"

    if not meta_path.exists():
        raise HTTPException(404, f"Meta for {fname} not found")

    try:
        with open(meta_path, "r", encoding="utf-8") as fh:
            meta = json.load(fh)
    except Exception as e:
        log_error(f"_rehash_one_file: meta read error {meta_path}: {repr(e)}")
        raise HTTPException(500, "Broken meta")

    old_vk_id = str(meta.get("vk_id") or meta.get("vk_response", {}).get("id") or "").strip()
    if not old_vk_id:
        # запасной вариант — берём из имени файла до первого "_"
        old_vk_id = fname.split("_", 1)[0]

    is_video = (meta.get("type") == "video") or ext.lower() in (".mp4", ".mov", ".m4v", ".webm", ".avi", ".mkv")
    width  = int(meta.get("width")  or 720)
    height = int(meta.get("height") or 1280)

    # часть после vk_id_ — "отображаемое имя"
    display_name = fname.split("_", 1)[1] if "_" in fname else fname

    # === 1. Делаем временный файл с МОДИФИЦИРОВАННЫМ содержимым ===
    with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
        tmp_path = Path(tmp.name)

    try:
        if not is_video:
            # КАРТИНКА: пересохраняем, чтобы изменились байты
            try:
                img = Image.open(file_path)
                # сохраним в том же формате, если возможно
                fmt = img.format or ("JPEG" if ext.lower() in (".jpg", ".jpeg") else "PNG")
                img.save(tmp_path, format=fmt)
            except Exception as e:
                log_error(f"_rehash_one_file: image resave failed {file_path} -> {tmp_path}: {repr(e)}")
                # fall-back — просто копируем (в худшем случае VK всё равно вернёт тот же id)
                shutil.copy(file_path, tmp_path)
        else:
            # ВИДЕО: делаем ремультиплекс/копию через ffmpeg (байты точно поменяются)
            try:
                proc = subprocess.run(
                    [
                        "ffmpeg", "-y",
                        "-i", str(file_path),
                        "-c", "copy",
                        str(tmp_path),
                    ],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                if proc.returncode != 0:
                    log_error(f"_rehash_one_file: ffmpeg copy failed {file_path}: {proc.stderr[:400]}")
                    # fall-back — тупо копируем
                    shutil.copy(file_path, tmp_path)
            except Exception as e:
                log_error(f"_rehash_one_file: ffmpeg exception {file_path}: {repr(e)}")
                shutil.copy(file_path, tmp_path)

        # === 2. Заливаем ВРЕМЕННЫЙ файл в VK ===

        data = ensure_user_structure(user_id)
        cab = next((c for c in data["cabinets"] if str(c["id"]) == str(cabinet_id)), None)
        if not cab or not cab.get("token"):
            raise HTTPException(400, "Invalid cabinet or missing token")
        token_name = cab["token"]
        real_token = os.getenv(token_name)
        if not real_token:
            raise HTTPException(500, f"Token {token_name} not found in environment")

        headers = {"Authorization": f"Bearer {real_token}"}
        vk_url = "https://ads.vk.com/api/v2/content/video.json" if is_video else "https://ads.vk.com/api/v2/content/static.json"

        with open(tmp_path, "rb") as fh:
            files = {
                "file": (display_name, fh, "video/mp4" if is_video else "image/jpeg"),
                "data": (None, json.dumps({"width": width, "height": height}), "application/json"),
            }
            resp = requests.post(vk_url, headers=headers, files=files, timeout=60)

        if resp.status_code != 200:
            log_error(f"_rehash_one_file: VK error {vk_url} => {resp.status_code} {resp.text[:300]}")
            raise HTTPException(502, "VK upload error")

        resp_json = resp.json()
        new_vk_id = str(resp_json.get("id") or "").strip()
        if not new_vk_id:
            raise HTTPException(500, "VK did not return id")

        if new_vk_id == old_vk_id:
            log_error(f"_rehash_one_file: VK returned SAME id ({old_vk_id}) for {file_path}")
            # формально это уже "плохая" ситуация, можно:
            # - либо всё равно продолжать (просто обновили meta),
            # - либо бросать ошибку.
            # Я предлагаю ПРОДОЛЖАТЬ, чтобы не ломать UX.

        # === 3. Создаём новый локальный файл под new_vk_id_ТотЖеНейм ===

        final_name = f"{new_vk_id}_{display_name}"
        final_path = storage / final_name

        try:
            shutil.copy(tmp_path, final_path)
        except Exception as e:
            log_error(f"_rehash_one_file: copy tmp -> final error {tmp_path} -> {final_path}: {repr(e)}")
            raise HTTPException(500, "Internal copy error")

        # === 4. Удаляем старый файл и старые превью/мету ===
        _safe_unlink(file_path)
        _safe_unlink(meta_path)
        # превью могут быть в двух вариантах
        _safe_unlink(storage / (fname + ".jpg"))
        _safe_unlink(storage / f"{base_no_ext}.jpg")

        # === 5. Генерируем новое превью для видео ===
        thumb_url = None
        if is_video:
            try:
                thumb_name = f"{final_name}.jpg"
                thumb_path = storage / thumb_name
                proc = subprocess.run(
                    [
                        "ffmpeg", "-y",
                        "-ss", "1",
                        "-i", str(final_path),
                        "-vframes", "1",
                        "-vf", "scale=360:-1",
                        str(thumb_path),
                    ],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                if proc.returncode == 0 and thumb_path.exists():
                    thumb_url = f"/auto_ads/video/{cabinet_id}/{thumb_name}"
                else:
                    log_error(f"_rehash_one_file: ffmpeg thumb failed for {final_path}: {proc.stderr[:400]}")
            except Exception as e:
                log_error(f"_rehash_one_file: thumb exception for {final_path}: {repr(e)}")

        # === 6. Пишем новую мету ===
        new_base, _ = os.path.splitext(final_name)
        new_meta_path = storage / f"{new_base}.json"
        new_meta = {
            "vk_response": resp_json,
            "cabinet_id": str(cabinet_id),
            "vk_id": new_vk_id,
            "display_name": display_name,
            "stored_file": f"/auto_ads/video/{cabinet_id}/{final_name}",
            "thumb_url": thumb_url,
            "content_type": meta.get("content_type") or ("video/mp4" if is_video else "image/jpeg"),
            "width": width,
            "height": height,
            "uploaded_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "type": "video" if is_video else "image",
        }
        atomic_write_json(new_meta_path, new_meta)
        
        file_url = f"/auto_ads/video/{cabinet_id}/{final_name}"
        
        return {
            "old_vk_id": old_vk_id,
            "new_vk_id": new_vk_id,
            "final_name": final_name,
            "cabinet_id": str(cabinet_id),
            "url": file_url,
            "thumb_url": thumb_url,
            "meta": new_meta,
        }

    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass

def remove_from_global_queue(user_id: str, cabinet_id: str, preset_id: str):
    """
    Удаляет запись(и) по ключу (user_id, cabinet_id, preset_id) из /opt/auto_ads/data/global_queue.json
    с файловой блокировкой и атомарной записью.
    """
    GLOBAL_QUEUE_FILE.parent.mkdir(parents=True, exist_ok=True)
    lock_path = GLOBAL_QUEUE_FILE.with_suffix(".lock")
    with file_lock(lock_path):
        # читаем текущее содержимое
        if GLOBAL_QUEUE_FILE.exists():
            try:
                with open(GLOBAL_QUEUE_FILE, "r", encoding="utf-8") as fh:
                    data = json.load(fh) or []
            except Exception as e:
                log_error(f"remove_from_global_queue: broken json, reset. err={repr(e)}")
                data = []
        else:
            data = []
        # фильтруем
        uid, cid, pid = str(user_id), str(cabinet_id), str(preset_id)
        data = [
            it for it in (data if isinstance(data, list) else [])
            if not (
                str(it.get("user_id","")) == uid and
                str(it.get("cabinet_id","")) == cid and
                str(it.get("preset_id","")) == pid
            )
        ]
        # атомарно пишем
        atomic_write_json(GLOBAL_QUEUE_FILE, data)

def upsert_global_queue(item: dict):
    """
    Безопасно вставляет/обновляет запись в /opt/auto_ads/data/global_queue.json,
    ключом считаем (user_id, cabinet_id, preset_id).
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    GLOBAL_QUEUE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = GLOBAL_QUEUE_FILE.with_suffix(".json.tmp")

    # читаем + лочим основной файл
    with open(GLOBAL_QUEUE_FILE, "a+", encoding="utf-8") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            f.seek(0)
            raw = f.read()
            try:
                data = json.loads(raw) if raw.strip() else []
            except Exception as e:
                log_error(f"global_queue: broken JSON, resetting. err={repr(e)}")
                data = []

            if not isinstance(data, list):
                data = []

            # ключи
            uid = str(item.get("user_id", ""))
            cid = str(item.get("cabinet_id", ""))
            pid = str(item.get("preset_id", ""))

            # ищем существующую запись
            idx = -1
            for i, it in enumerate(data):
                if (str(it.get("user_id","")) == uid and
                    str(it.get("cabinet_id","")) == cid and
                    str(it.get("preset_id","")) == pid):
                    idx = i
                    break

            if idx >= 0:
                data[idx] = item  # обновляем
            else:
                data.append(item) # добавляем

            # пишем атомарно
            with open(tmp_path, "w", encoding="utf-8") as tf:
                json.dump(data, tf, ensure_ascii=False, indent=2)
                tf.flush()
                os.fsync(tf.fileno())
            os.replace(tmp_path, GLOBAL_QUEUE_FILE)
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)

def read_global_queue() -> list[dict]:
    """Безопасно читает список из GLOBAL_QUEUE_FILE."""
    try:
        if not GLOBAL_QUEUE_FILE.exists():
            return []
        with open(GLOBAL_QUEUE_FILE, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, list) else []
    except Exception as e:
        log_error(f"read_global_queue failed: {repr(e)}")
        return []

def update_status_in_global_queue(user_id: str, cabinet_id: str, preset_id: str, status: str):
    """
    Меняет только поле status у записи (user_id, cabinet_id, preset_id).
    Если записи нет — создаёт минимальную с этим статусом.
    """
    status = "active" if status != "deactive" else "deactive"
  
    GLOBAL_QUEUE_FILE.parent.mkdir(parents=True, exist_ok=True)
    # Блокировка тем же способом, как в upsert_global_queue
    with open(GLOBAL_QUEUE_FILE, "a+", encoding="utf-8") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            f.seek(0)
            raw = f.read()
            try:
                data = json.loads(raw) if raw.strip() else []
            except Exception:
                data = []

            if not isinstance(data, list):
                data = []

            uid, cid, pid = str(user_id), str(cabinet_id), str(preset_id)

            idx = -1
            for i, it in enumerate(data):
                if (str(it.get("user_id","")) == uid and
                    str(it.get("cabinet_id","")) == cid and
                    str(it.get("preset_id","")) == pid):
                    idx = i
                    break

            if idx >= 0:
                # обновляем только статус
                it = dict(data[idx])
                it["status"] = status
                data[idx] = it
            else:
                # создаём минимальную запись, чтобы статус сохранился
                data.append({
                    "user_id": uid,
                    "cabinet_id": cid,
                    "preset_id": pid,
                    "status": status,
                    "date_time": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                })

            tmp_path = GLOBAL_QUEUE_FILE.with_suffix(".json.tmp")
            with open(tmp_path, "w", encoding="utf-8") as tf:
                json.dump(data, tf, ensure_ascii=False, indent=2)
                tf.flush()
                os.fsync(tf.fileno())
            os.replace(tmp_path, GLOBAL_QUEUE_FILE)
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)

def abstract_audiences_path(user_id: str, cabinet_id: str) -> Path:
    p = USERS_DIR / user_id / "audiences" / str(cabinet_id)
    p.mkdir(parents=True, exist_ok=True)
    return p / "abstract.json"

def default_abstract_audiences() -> list[dict]:
    names = [
        "LAL Б0 ({день})",
        "LAL Б1 ({день})",
        "LAL ББ ({день})",
        "LAL ББ ДОП_2 ({день})",
        "LAL ББ ДОП_3 ({день})",
        "LAL КР 1 ({день})",
        "LAL КР 2 ({день})",
        "LAL КР ДОП_3 ({день})",
        "LAL КР ДОП_4 ({день})",
        "LAL КР ДОП_5 ({день})",
        "LAL КР ДОП_6 ({день})",
        "LAL КР ДОП_8 ({день})",
        "LAL КР ДОП_9 ({день})",
        "LAL КР ДОП_10 ({день})",
    ]
    return [{"name": n} for n in names]

def textsets_path(user_id: str, cabinet_id: str) -> Path:
    return USERS_DIR / user_id / "presets" / cabinet_id / "textsets.json"

def cabinet_storage(cabinet_id: str) -> Path:
    path = STORAGE_DIR / str(cabinet_id)
    path.mkdir(parents=True, exist_ok=True)
    return path
    
def ensure_user_structure(user_id: str):
    """Создает структуру и единый файл <id>.json, если его нет."""

    user_dir = USERS_DIR / user_id
    user_dir.mkdir(parents=True, exist_ok=True)

    # Основные директории
    (user_dir / "presets").mkdir(exist_ok=True)
    (user_dir / "creatives").mkdir(exist_ok=True)
    (user_dir / "audiences").mkdir(exist_ok=True)

    # Файл user.json
    info_file = user_dir / f"{user_id}.json"

    # Если первый вход — создаём дефолтный файл
    if not info_file.exists():
        data = {
            "user_id": user_id,
            "cabinets": [  # Всегда создаём кабинет ALL
                {"id": "all", "name": "Все кабинеты", "token": ""}
            ],
            "selected_cabinet_id": "all"
        }
        with open(info_file, "w") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    # Загружаем файл
    with open(info_file, "r") as f:
        data = json.load(f)

    # Гарантируем наличие кабинета all
    if not any(c["id"] == "all" for c in data["cabinets"]):
        data["cabinets"].insert(0, {"id": "all", "name": "Все кабинеты", "token": ""})

    # Создаём директории под каждый кабинет
    for cab in data["cabinets"]:
        cab_id = str(cab["id"])
        (user_dir / "presets" / cab_id).mkdir(exist_ok=True)
        (user_dir / "creatives" / cab_id).mkdir(exist_ok=True)
        (user_dir / "audiences" / cab_id).mkdir(exist_ok=True)

    # Перезаписываем (если изменилось)
    with open(info_file, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return data


def udir(user_id: str) -> Path:
    d = USERS_DIR / user_id
    d.mkdir(parents=True, exist_ok=True)

    (d / "presets").mkdir(exist_ok=True)
    (d / "creatives").mkdir(exist_ok=True)
    (d / "audiences").mkdir(exist_ok=True)

    return d


def user_info_file(user_id: str) -> Path:
    return udir(user_id) / f"{user_id}.json"


def load_user_info(user_id: str) -> dict:
    # всегда сначала создаём базовую структуру
    base = ensure_user_structure(user_id)

    return base


def preset_path(user_id: str, cabinet_id: str, preset_id: str) -> Path:
    return USERS_DIR / user_id / "presets" / cabinet_id / f"{preset_id}.json"
    
def creatives_path(user_id: str, cabinet_id: str) -> Path:
    return USERS_DIR / user_id / "creatives" / cabinet_id / "sets.json"

def audiences_path(user_id: str, cabinet_id: str) -> Path:
    return USERS_DIR / user_id / "audiences" / cabinet_id / "audiences.json"

# ----------------------------------- API --------------------------------------------
secure_api = APIRouter(prefix="/api", dependencies=[Depends(require_tg_user)])
secure_auto = APIRouter(prefix="/auto_ads/api", dependencies=[Depends(require_tg_user)])
# -------------------------------------
#   HISTORY
# -------------------------------------
@secure_api.get("/history/get")
@secure_auto.get("/history/get")
def history_get(user_id: str = Query(...), cabinet_id: str = Query(...)):
  items = read_history_file(user_id, cabinet_id)
  return JSONResponse({"items": items})
    
# -------------------------------------
#   PRESETS (each in separate file)
# -------------------------------------
@secure_auto.post("/preset/save")
@secure_api.post("/preset/save")
async def save_preset(payload: dict):
    preset = payload.get("preset")
    fast_preset_flag = "true" if bool(preset.get("fastPreset")) else "false"
    user_id = payload.get("userId")
    cabinet_id = payload.get("cabinetId")
    preset_id = payload.get("presetId")

    if not user_id or not cabinet_id or not preset:
        raise HTTPException(400, "userId, cabinetId and preset required")
        
    fast_preset_flag = "true" if bool(preset.get("fastPreset")) else "false"
    data = ensure_user_structure(user_id)

    # создаём новый id
    if not preset_id:
        preset_id = f"preset_{uuid.uuid4().hex[:8]}"

    # файл пресета
    fpath = preset_path(user_id, cabinet_id, preset_id)
    with open(fpath, "w", encoding="utf-8") as f:
        json.dump(preset, f, ensure_ascii=False, indent=2)

    # ====== НОВОЕ: добавляем запись в глобальную очередь ======
    # Соберём список "tokens" (НЕ секреты, а имена переменных из .env).
    # Если cabinet_id == "all" — берём все кабинеты кроме "all" с заполненным token.
    if str(cabinet_id) == "all":
        token_names = [
            c.get("token") for c in data.get("cabinets", [])
            if str(c.get("id")) != "all" and c.get("token")
        ]
    else:
        token_names = [
            c.get("token") for c in data.get("cabinets", [])
            if str(c.get("id")) == str(cabinet_id) and c.get("token")
        ]

    # Кол-во дублей
    count_repeats = 1
    try:
        count_repeats = int(preset.get("company", {}).get("duplicates", 1) or 1)
    except Exception:
        count_repeats = 1
    
    company = preset.get("company", {}) or {}
    trigger_time = ""
    if str(company.get("trigger", "time")) == "time":
        trigger_time = str(company.get("time") or "")
    
    # список "token"-имён кабинетов
    if str(cabinet_id) == "all":
        token_names = [
            c.get("token") for c in data.get("cabinets", [])
            if str(c.get("id")) != "all" and c.get("token")
        ]
    else:
        token_names = [
            c.get("token") for c in data.get("cabinets", [])
            if str(c.get("id")) == str(cabinet_id) and c.get("token")
        ]
    
    queue_item = {
        "user_id": str(user_id),
        "cabinet_id": str(cabinet_id),
        "preset_id": str(preset_id),   # ← ВАЖНО: теперь пишем preset_id
        "tokens": token_names,
        "date_time": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "count_repeats": count_repeats,
        "trigger_time": trigger_time,
        "fast_preset": fast_preset_flag,
        "status": "active"
    }
    
    upsert_global_queue(queue_item)

    return {"status": "ok", "preset_id": preset_id}


@secure_api.get("/preset/list")
@secure_auto.get("/preset/list")
def list_presets(user_id: str, cabinet_id: str):
    ensure_user_structure(user_id)
    pdir = USERS_DIR / user_id / "presets" / cabinet_id
    presets = []

    for file in pdir.glob("*.json"):
        try:
            with open(file, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                raise ValueError("Preset file is not an object")
            if "company" not in data or not isinstance(data["company"], dict):
                data["company"] = {}
            if "groups" not in data or not isinstance(data["groups"], list):
                data["groups"] = []
            if "ads" not in data or not isinstance(data["ads"], list):
                data["ads"] = []

            mtime = file.stat().st_mtime
            created_at = datetime.utcfromtimestamp(mtime).isoformat(timespec="seconds") + "Z"

            presets.append({
                "preset_id": file.stem,
                "created_at": created_at,
                "data": data
            })
        except Exception as e:
            log_error(f"Skip invalid preset file: {file} | {repr(e)}")

    return {"presets": presets}

@secure_auto.delete("/preset/delete")
@secure_api.delete("/preset/delete")
def delete_preset(
    user_id: str = Query(...),
    cabinet_id: str = Query(...),
    preset_id: str = Query(...),
):
    ensure_user_structure(user_id)
    f = preset_path(user_id, cabinet_id, preset_id)
    if f.exists():
        f.unlink()
    try:
        remove_from_global_queue(user_id, cabinet_id, preset_id)
    except Exception as e:
        log_error(f"delete_preset: remove_from_global_queue failed: {repr(e)}")
    return {"status": "deleted"}



# -------------------------------------
#   CREATIVE SETS
# -------------------------------------
@secure_api.post("/creatives/save")
async def save_creatives(payload: dict):
    user_id = payload.get("userId")
    cabinet_id = payload.get("cabinetId")
    creatives = payload.get("creatives")

    if not user_id or not cabinet_id:
        log_error(f"creatives/save: missing fields user_id={user_id} cabinet_id={cabinet_id}")
        raise HTTPException(400, "Missing userId or cabinetId")

    try:
        ensure_user_structure(user_id)
        f = creatives_path(user_id, cabinet_id)
        lock = f.with_suffix(f.suffix + ".lock")

        # Блокируем на время записи
        with file_lock(lock):
            atomic_write_json(f, creatives if creatives is not None else [])
        return {"status": "ok"}
    except Exception as e:
        log_error(f"creatives/save[{user_id}/{cabinet_id}] error: {repr(e)}")
        return JSONResponse(status_code=500, content={"error": "Internal Server Error"})


@secure_api.get("/creatives/get")
def get_creatives(user_id: str, cabinet_id: str):
    try:
        ensure_user_structure(user_id)
        f = creatives_path(user_id, cabinet_id)
        if not f.exists():
            return {"creatives": []}

        lock = f.with_suffix(f.suffix + ".lock")
        with file_lock(lock):
            # читаем целиком безопасно
            with open(f, "r", encoding="utf-8") as fh:
                text = fh.read()

        try:
            data = json.loads(text) if text.strip() else []
            if not isinstance(data, list):
                # если вдруг не список — приводим
                data = []
            return {"creatives": data}
        except json.JSONDecodeError as je:
            # перекладываем битую версию в .bad и возвращаем пусто
            bad = f.with_suffix(f.suffix + f".bad_{int(datetime.utcnow().timestamp())}")
            try:
                with open(bad, "w", encoding="utf-8") as bfh:
                    bfh.write(text)
            except Exception as e2:
                log_error(f"creatives/get failed to write .bad: {repr(e2)}")
            log_error(f"creatives/get JSONDecodeError on {f}: {repr(je)}; moved to {bad.name}")
            return {"creatives": []}
    except Exception as e:
        log_error(f"creatives/get[{user_id}/{cabinet_id}] error: {repr(e)}")
        return JSONResponse(status_code=500, content={"error": "Internal Server Error"})

@app.get("/video/{cabinet_id}/{filename}")
@app.get("/auto_ads/video/{cabinet_id}/{filename}")
def serve_file(cabinet_id: str, filename: str):
    path = cabinet_storage(cabinet_id) / filename
    if not path.exists():
        raise HTTPException(404, "File not found")
    return FileResponse(path)

@secure_api.post("/creative/delete")
async def creative_delete(payload: dict):
    """
    Тело:
    {
      "userId": "...",
      "cabinetId": "...",                    # выбранный в UI; для urls по кабинетам он не критичен
      "item": {
        "type": "video" | "image",
        "url": "/auto_ads/video/<cab>/<file>",      # если файл один
        "urls": { "<cab>": "/auto_ads/video/<cab>/<file>", ... },  # если загрузка была с 'all'
        "thumbUrl": "/auto_ads/video/<cab>/<file>.jpg"             # опционально (видео)
      }
    }
    Удаляет соответствующие файлы в /mnt/data/auto_ads_storage/video/<cab>/...
    """
    try:
        user_id = payload.get("userId")
        cabinet_id = str(payload.get("cabinetId", ""))  # не обязателен, если есть item.urls
        item = payload.get("item") or {}
        it_type = str(item.get("type", ""))
        if not user_id:
            raise HTTPException(400, "Missing userId")
        if not isinstance(item, dict):
            raise HTTPException(400, "Missing item")

        to_delete: list[tuple[str, str]] = []  # (cabinet_id, filename)

        def add_by_url(url: str):
            if not url:
                return
            # берем только basename чтобы избежать traversal
            name = Path(url).name
            # вытащим cabinet из url (/auto_ads/video/<cab>/<file>) — на фронте ты так формируешь
            try:
                parts = url.strip("/").split("/")
                # [..., 'video', '<cab>', '<file>']
                idx = parts.index("video")
                cab = parts[idx + 1]
            except Exception:
                cab = cabinet_id or "all"
            to_delete.append((cab, name))

        # 1) множественный вариант (когда загружали в 'all')
        if isinstance(item.get("urls"), dict) and item["urls"]:
            for cab, url in item["urls"].items():
                if isinstance(url, str):
                    name = Path(url).name
                    to_delete.append((str(cab), name))
        # 2) одиночный вариант
        elif isinstance(item.get("url"), str):
            add_by_url(item["url"])

        # (опционально) если пришёл thumbUrl — удалим его явно
        thumb_url = item.get("thumbUrl")
        if isinstance(thumb_url, str) and thumb_url:
            try:
                parts = thumb_url.strip("/").split("/")
                idx = parts.index("video")
                cab_t = parts[idx + 1]
                name_t = Path(thumb_url).name
                to_delete.append((str(cab_t), name_t))
            except Exception:
                pass

        deleted: list[str] = []
        for cab, fname in to_delete:
            storage = cabinet_storage(cab)
            file_path = storage / fname
            _safe_unlink(file_path)
            deleted.append(str(file_path))
        
            base_no_ext, ext = os.path.splitext(fname)
        
            # 1) metadata sidecar
            meta_path = storage / f"{base_no_ext}.json"
            _safe_unlink(meta_path)
        
            # 2) thumbnails (видео)
            # приоритет: если тип видео — удаляем возможные превью по нескольким паттернам
            if it_type == "video":
                # a) <final_fullname>.jpg (если генерили как "<final_name>.jpg")
                _safe_unlink(storage / (fname + ".jpg"))
                # b) <final_stem>.jpg (если отрезали расширение исходника)
                _safe_unlink(storage / f"{base_no_ext}.jpg")
                
        # --- ДОПОЛНИТЕЛЬНО: удаляем элемент из creatives/sets.json и чистим пресеты кабинета ---
        try:
            ensure_user_structure(user_id)
            # читаем sets.json ТЕКУЩЕГО cabinet_id (из параметра запроса)
            sets_file = creatives_path(user_id, cabinet_id)
            if sets_file.exists():
                lock2 = sets_file.with_suffix(sets_file.suffix + ".lock")
                with file_lock(lock2):
                    raw = sets_file.read_text(encoding="utf-8")
                    sets = json.loads(raw) if raw.strip() else []
                    if not isinstance(sets, list):
                        sets = []

                    # Пытаемся найти элемент по url/urls и вычислить его vk-id для текущего кабинета
                    item_vk_id_to_drop = None

                    def _match_item(it: dict) -> bool:
                        u = it.get("url")
                        if isinstance(u, str) and any(Path(u).name == fname for _cab, fname in to_delete):
                            return True
                        urls = it.get("urls")
                        if isinstance(urls, dict):
                            for _cab, u2 in urls.items():
                                if isinstance(u2, str):
                                    if any(Path(u2).name == fname for _cab2, fname in to_delete):
                                        return True
                        return False

                    for s in sets:
                        items = s.get("items") or []
                        keep = []
                        for it in items:
                            if _match_item(it):
                                # берём vk-id для текущего кабинета (если all — пробуем вытянуть из url)
                                vk_by_cab = it.get("vkByCabinet") or {}
                                cand = vk_by_cab.get(str(cabinet_id))
                                if isinstance(cand, (str, int)):
                                    item_vk_id_to_drop = str(cand)
                                elif isinstance(it.get("id"), (str, int)):
                                    item_vk_id_to_drop = str(it["id"])
                                # пропускаем (удаляем этот item)
                                continue
                            keep.append(it)
                        s["items"] = keep

                    atomic_write_json(sets_file, sets)

                # если нашли vk-id — удалим его из пресетов этого кабинета
                if item_vk_id_to_drop:
                    _drop_media_id_in_presets_for_cab(user_id, str(cabinet_id), item_vk_id_to_drop)
        except Exception as e:
            log_error(f"/creative/delete: cleanup sets/presets failed: {repr(e)}")
        return {"status": "ok", "deleted": deleted}
    except HTTPException:
        raise
    except Exception as e:
        log_error(f"/creative/delete error: {repr(e)}")
        return JSONResponse(status_code=500, content={"error": "Internal Server Error"})
        
@secure_auto.post("/creative/rehash")
@secure_api.post("/creative/rehash")
async def creative_rehash(payload: dict):
    """
    Тело:
    {
      "userId": "...",
      "cabinetId": "...",      # кабинет, для которого открыт UI (может быть 'all')
      "setId": "...",          # id набора креативов
      "itemId": "..."          # id элемента внутри набора
    }

    Делает:
      - перезаливку видео в VK (новый vk_id),
      - пересоздание локальных файлов/превью/json,
      - обновление creatives (vkByCabinet, url, id),
      - замену old_vk_id -> new_vk_id во всех пресетах пользователя.
    """
    try:
        user_id = payload.get("userId")
        cabinet_id = str(payload.get("cabinetId", ""))
        set_id = payload.get("setId")
        item_id = payload.get("itemId")

        if not user_id or not set_id or not item_id:
            raise HTTPException(400, "Missing userId/setId/itemId")

        user_data = ensure_user_structure(user_id)

        f = creatives_path(user_id, cabinet_id)
        if not f.exists():
            raise HTTPException(404, "Creatives file not found")

        lock = f.with_suffix(f.suffix + ".lock")

        # --- Шаг 1: читаем sets.json и определяем список файлов для rehash ---
        with file_lock(lock):
            with open(f, "r", encoding="utf-8") as fh:
                text = fh.read()

            try:
                data = json.loads(text) if text.strip() else []
            except json.JSONDecodeError as je:
                log_error(f"/creative/rehash JSONDecodeError on {f}: {repr(je)}")
                raise HTTPException(500, "Invalid creatives storage")

            if not isinstance(data, list):
                raise HTTPException(500, "Creatives storage is not a list")

            target_set = None
            for s in data:
                if str(s.get("id")) == str(set_id):
                    target_set = s
                    break

            if not target_set:
                raise HTTPException(404, "Creative set not found")

            items = target_set.get("items") or []
            target_item = None
            for it in items:
                if str(it.get("id")) == str(item_id):
                    target_item = it
                    break

            if not target_item:
                raise HTTPException(404, "Creative item not found")

            # список (cabinet_id, filename) для rehash
            tasks: list[tuple[str, str]] = []

            if isinstance(target_item.get("urls"), dict) and target_item["urls"]:
                # вариант когда загружали в 'all' — несколько кабинетов
                for cab, url in target_item["urls"].items():
                    if isinstance(url, str) and url:
                        name = Path(url).name
                        tasks.append((str(cab), name))
            elif isinstance(target_item.get("url"), str) and target_item["url"]:
                url = target_item["url"]
                name = Path(url).name
                # вытащим cabinet из url (/auto_ads/video/<cab>/<file>)
                try:
                    parts = url.strip("/").split("/")
                    idx = parts.index("video")
                    cab = parts[idx + 1]
                except Exception:
                    cab = cabinet_id or "all"
                tasks.append((str(cab), name))

        if not tasks:
            raise HTTPException(400, "No files to rehash for this item")

        # --- Шаг 2: перезаливаем файлы в VK и готовим mapping old->new ---
        rehash_results: list[dict] = []
        mapping: dict[str, str] = {}

        for cab, fname in tasks:
            try:
                res = _rehash_one_file(user_id, cab, fname)
                rehash_results.append(res)
                ov = res.get("old_vk_id")
                nv = res.get("new_vk_id")
                if ov and nv:
                    mapping[str(ov)] = str(nv)
            except HTTPException:
                raise
            except Exception as e:
                log_error(f"/creative/rehash _rehash_one_file error for {cab}/{fname}: {repr(e)}")
                raise HTTPException(500, "Internal rehash error")

        # --- Шаг 3: обновляем creatives (vkByCabinet, url, id, thumbUrl) ---
        with file_lock(lock):
            with open(f, "r", encoding="utf-8") as fh:
                text = fh.read()

            try:
                data = json.loads(text) if text.strip() else []
            except json.JSONDecodeError as je:
                log_error(f"/creative/rehash second read JSONDecodeError on {f}: {repr(je)}")
                raise HTTPException(500, "Invalid creatives storage on second read")

            if not isinstance(data, list):
                raise HTTPException(500, "Creatives storage is not a list (second read)")

            target_set = None
            for s in data:
                if str(s.get("id")) == str(set_id):
                    target_set = s
                    break

            if not target_set:
                # если вдруг кто-то удалил набор параллельно — просто сохраняем как есть
                log_error(f"/creative/rehash: set {set_id} disappeared on second read")
                atomic_write_json(f, data)
            else:
                items = target_set.get("items") or []
                target_item = None
                for it in items:
                    if str(it.get("id")) == str(item_id):
                        target_item = it
                        break

                if target_item:
                    # обновляем item согласно rehash_results
                    if isinstance(target_item.get("urls"), dict) and target_item["urls"]:
                        # вариант 'all': urls + vkByCabinet
                        urls = dict(target_item.get("urls") or {})
                        vk_by_cab = dict(target_item.get("vkByCabinet") or {})
                        thumb_url = target_item.get("thumbUrl")

                        for res in rehash_results:
                            cab = str(res.get("cabinet_id"))
                            nv = str(res.get("new_vk_id"))
                            url = res.get("url")
                            tmb = res.get("thumb_url")
                            if cab and nv:
                                vk_by_cab[cab] = nv
                            if cab and isinstance(url, str):
                                urls[cab] = url
                            if tmb and not thumb_url:
                                thumb_url = tmb

                        target_item["urls"] = urls
                        target_item["vkByCabinet"] = vk_by_cab
                        if thumb_url:
                            target_item["thumbUrl"] = thumb_url
                    else:
                        # одиночный кабинет: используем первый результат
                        res0 = rehash_results[0]
                        nv = str(res0.get("new_vk_id"))
                        url = res0.get("url")
                        tmb = res0.get("thumb_url")
                        cab = str(res0.get("cabinet_id"))

                        if nv:
                            target_item["id"] = nv
                        if isinstance(url, str):
                            target_item["url"] = url
                        vk_by_cab = dict(target_item.get("vkByCabinet") or {})
                        if cab and nv:
                            vk_by_cab[cab] = nv
                        target_item["vkByCabinet"] = vk_by_cab
                        if tmb:
                            target_item["thumbUrl"] = tmb

                    # сохраняем весь список
                    atomic_write_json(f, data)
                else:
                    log_error(f"/creative/rehash: item {item_id} disappeared on second read")
                    atomic_write_json(f, data)

        # --- Шаг 4: заменяем id в пресетах ТЕКУЩЕГО кабинета ---
        try:
            _replace_media_id_in_presets_for_cab(user_id, str(cabinet_id), mapping)
        except Exception as e:
            log_error(f"/creative/rehash _replace_media_id_in_presets_for_cab error: {repr(e)}")

        return {"status": "ok", "results": rehash_results}

    except HTTPException:
        raise
    except Exception as e:
        log_error(f"/creative/rehash fatal error: {repr(e)}")
        return JSONResponse(status_code=500, content={"error": "Internal Server Error"})

# -------- Queue status (per preset) --------
@secure_api.get("/queue/status/get")
@secure_auto.get("/queue/status/get")
def queue_status_get(user_id: str = Query(...), cabinet_id: str = Query(...)):
    """
    Возвращает статусы пресетов для пары (user_id, cabinet_id).
    Формат ответа: {"items":[{"preset_id": "...", "status":"active|deactive"}, ...]}
    """
    items = []
    for it in read_global_queue():
        if str(it.get("user_id","")) == str(user_id) and str(it.get("cabinet_id","")) == str(cabinet_id):
            pid = str(it.get("preset_id",""))
            if not pid:
                continue
            st = it.get("status") or "active"
            st = "deactive" if st == "deactive" else "active"
            items.append({"preset_id": pid, "status": st})
    return {"items": items}

@secure_api.post("/queue/status/set")
@secure_auto.post("/queue/status/set")
async def queue_status_set(payload: dict):
    """
    Тело: { "userId": "...", "cabinetId": "...", "presetId": "...", "status": "active|deactive" }
    """
    user_id   = payload.get("userId")
    cabinet_id= payload.get("cabinetId")
    preset_id = payload.get("presetId")
    status    = payload.get("status")
    if not user_id or cabinet_id is None or not preset_id:
        raise HTTPException(400, "Missing userId/cabinetId/presetId")
    if status not in ("active","deactive"):
        raise HTTPException(400, "Invalid status")

    try:
        update_status_in_global_queue(str(user_id), str(cabinet_id), str(preset_id), status)
        return {"status":"ok"}
    except Exception as e:
        log_error(f"queue/status/set error: {repr(e)}")
        return JSONResponse(status_code=500, content={"error":"Internal Server Error"})


# -------------------------------------
#   LOGO SETS
# -------------------------------------
@app.get("/auto_ads/logo/{cabinet_id}/{filename}")
@app.get("/logo/{cabinet_id}/{filename}")
def serve_logo(cabinet_id: str, filename: str):
    path = logo_storage(cabinet_id) / filename
    if not path.exists():
        raise HTTPException(404, "Logo not found")
    return FileResponse(path)

@secure_api.post("/logo/upload")
async def upload_logo(
    user_id: str,
    cabinet_id: str,
    file: UploadFile = File(...),
):
    try:
        ensure_user_structure(user_id)
        if not file.content_type.startswith("image/"):
            raise HTTPException(400, "Only image allowed")

        # читаем в память
        content = await file.read()
        img = Image.open(BytesIO(content)).convert("RGBA")
        w, h = img.size

        # центр-кроп до квадрата
        side = min(w, h)
        left = (w - side) // 2
        top = (h - side) // 2
        img = img.crop((left, top, left + side, top + side))

        # resize 256x256
        img = img.resize((256, 256), Image.LANCZOS).convert("RGB")

        # сохраняем во временный файл (jpeg)
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
            img.save(tmp, format="JPEG", quality=95)
            tmp_path = tmp.name

        # берём токен кабинета (как в upload_creative)
        data = ensure_user_structure(user_id)
        cab = next((c for c in data["cabinets"] if str(c["id"]) == str(cabinet_id)), None)
        if not cab or not cab.get("token"):
            return JSONResponse(status_code=400, content={"error": "Invalid cabinet or missing token"})
        real_token = os.getenv(cab["token"])
        if not real_token:
            raise HTTPException(500, f"Token {cab['token']} not found in environment")

        headers = {"Authorization": f"Bearer {real_token}"}
        files = {
            "file": ("img256x256.jpg", open(tmp_path, "rb"), "image/jpeg"),
            "data": (None, json.dumps({"width": 256, "height": 256}), "application/json"),
        }

        # загружаем в VK
        vk_url = "https://ads.vk.com/api/v2/content/static.json"
        resp = requests.post(vk_url, headers=headers, files=files, timeout=20)
        if resp.status_code != 200:
            return JSONResponse(status_code=502, content={"error": resp.text})
        vk_id = resp.json().get("id")
        if not vk_id:
            raise HTTPException(500, "VK did not return id")

        # сохраняем локально
        storage = logo_storage(cabinet_id)
        final_name = f"{vk_id}_logo.jpg"
        final_path = storage / final_name
        shutil.copy(tmp_path, final_path)

        # сохраняем мету под блокировкой
        meta_path = logo_meta_path(user_id, cabinet_id)
        lock = meta_path.with_suffix(".lock")
        with file_lock(lock):
            atomic_write_json(meta_path, {
                "id": vk_id,
                "url": f"/auto_ads/logo/{cabinet_id}/{final_name}"
            })

        return {"status": "ok", "logo": {"id": vk_id, "url": f"/auto_ads/logo/{cabinet_id}/{final_name}"}}
    except Exception as e:
        log_error(f"logo/upload[{user_id}/{cabinet_id}] error: {repr(e)}")
        return JSONResponse(status_code=500, content={"error": "Internal Server Error"})
    finally:
        try:
            os.remove(tmp_path)  # type: ignore
        except Exception:
            pass

@secure_auto.get("/logo/get")
@secure_api.get("/logo/get")
def get_logo(user_id: str, cabinet_id: str):
    try:
        ensure_user_structure(user_id)
        meta_path = logo_meta_path(user_id, cabinet_id)
        if not meta_path.exists():
            return {"logo": None}
        with open(meta_path, "r", encoding="utf-8") as fh:
            return {"logo": json.load(fh)}
    except Exception as e:
        log_error(f"logo/get[{user_id}/{cabinet_id}] error: {repr(e)}")
        return JSONResponse(status_code=500, content={"error": "Internal Server Error"})

# -------------------------------------
#   Interests and regions
# -------------------------------------

@secure_api.get("/interests")
def get_interests():
    try:
        if not INTERESTS_FILE.exists():
            return {"interests": []}
        with open(INTERESTS_FILE, "r", encoding="utf-8") as fh:
            j = json.load(fh)
        # допускаем оба формата: {"interests":[...]} или просто [...]
        if isinstance(j, dict) and "interests" in j:
            return {"interests": j["interests"]}
        return {"interests": j}
    except Exception as e:
        log_error(f"/api/interests error: {repr(e)}")
        return JSONResponse(status_code=500, content={"error": "Internal Server Error"})

@secure_api.get("/regions")
def get_regions():
    try:
        if not REGIONS_FILE.exists():
            return {"items": []}
        with open(REGIONS_FILE, "r", encoding="utf-8") as fh:
            j = json.load(fh)
        # допускаем оба формата: {"items":[...]} или просто [...]
        if isinstance(j, dict) and "items" in j:
            return {"items": j["items"]}
        return {"items": j}
    except Exception as e:
        log_error(f"/api/regions error: {repr(e)}")
        return JSONResponse(status_code=500, content={"error": "Internal Server Error"})


# -------------------------------------
#   AUDIENCES
# -------------------------------------
@secure_api.post("/audiences/save")
async def save_audiences(payload: dict):
    user_id = payload.get("userId")
    cabinet_id = payload.get("cabinetId")
    audiences = payload.get("audiences")

    ensure_user_structure(user_id)

    f = audiences_path(user_id, cabinet_id)
    with open(f, "w") as file:
        json.dump(audiences, file, ensure_ascii=False, indent=2)

    return {"status": "ok"}

@secure_auto.get("/vk/audiences/fetch")
@secure_api.get("/vk/audiences/fetch")
def fetch_vk_audiences(user_id: str, cabinet_id: str):
    data = ensure_user_structure(user_id)

    cab = next((c for c in data["cabinets"] if str(c["id"]) == str(cabinet_id)), None)
    if not cab or not cab.get("token"):
        return JSONResponse(status_code=400, content={"audiences": [], "error": "Invalid cabinet or missing token"})

    token = os.getenv(cab["token"])
    if not token:
        return JSONResponse(status_code=500, content={"audiences": [], "error": f"Token {cab['token']} not found in .env"})

    headers = {"Authorization": f"Bearer {token}"}

    try:
        r = requests.get("https://ads.vk.com/api/v2/remarketing/segments.json?limit=1",
                         headers=headers, timeout=10)
        j = r.json()
        count = int(j.get("count", 0))
    except Exception as e:
        return JSONResponse(status_code=502, content={"audiences": [], "error": f"VK count error: {str(e)}"})

    if count == 0:
        return {"audiences": []}

    offset = max(0, count - 50)
    url2 = f"https://ads.vk.com/api/v2/remarketing/segments.json?limit=50&offset={offset}"

    try:
        r2 = requests.get(url2, headers=headers, timeout=15)
        j2 = r2.json()
        items = j2.get("items", [])
    except Exception as e:
        return JSONResponse(status_code=502, content={"audiences": [], "error": f"VK list error: {str(e)}"})

    out = [{
        "type": "vk",
        "id": str(it["id"]),
        "name": it["name"],
        "created": it.get("created", "")
    } for it in items]

    # сохраняем локально
    f = audiences_path(user_id, cabinet_id)
    with open(f, "w") as file:
        json.dump(out, file, ensure_ascii=False, indent=2)

    return {"audiences": out}

@secure_auto.get("/abstract_audiences/get")
@secure_api.get("/abstract_audiences/get")
def get_abstract_audiences(user_id: str, cabinet_id: str):
    """
    Возвращает абстрактные аудитории ДЛЯ КОНКРЕТНОГО кабинета.
    Если файла нет — создаёт его с дефолтным набором и возвращает.
    """
    ensure_user_structure(user_id)
    f = abstract_audiences_path(user_id, cabinet_id)

    if not f.exists():
        defaults = default_abstract_audiences()
        with open(f, "w") as fh:
            json.dump(defaults, fh, ensure_ascii=False, indent=2)
        return {"audiences": defaults}

    with open(f, "r") as fh:
        return {"audiences": json.load(fh)}


@secure_api.post("/abstract_audiences/save")
def save_abstract_audiences(payload: dict):
    user_id = payload.get("userId")
    cabinet_id = payload.get("cabinetId")
    items = payload.get("audiences", [])
    if not user_id or not cabinet_id:
        raise HTTPException(400, "Missing userId or cabinetId")

    ensure_user_structure(user_id)
    f = abstract_audiences_path(user_id, cabinet_id)
    with open(f, "w") as fh:
        json.dump(items, fh, ensure_ascii=False, indent=2)
    return {"status": "ok"}

@secure_auto.get("/audiences/get")
@secure_api.get("/audiences/get")
def get_audiences(user_id: str, cabinet_id: str):
    ensure_user_structure(user_id)

    f = audiences_path(user_id, cabinet_id)
    if not f.exists():
        return {"audiences": []}

    with open(f, "r") as file:
        return {"audiences": json.load(file)}

@secure_api.get("/vk/audiences/search")
def search_vk_audiences(user_id: str, cabinet_id: str, q: str = ""):
    """
    Возвращает последние (до 50) аудиторий VK, у которых имя начинается с q.
    Делает JSON-ответ даже при ошибках.
    """
    data = ensure_user_structure(user_id)

    cab = next((c for c in data["cabinets"] if str(c["id"]) == str(cabinet_id)), None)
    if not cab or not cab.get("token"):
        return JSONResponse(
            status_code=400,
            content={"audiences": [], "error": "Invalid cabinet or missing token"},
        )

    token = os.getenv(cab["token"])
    if not token:
        return JSONResponse(
            status_code=500,
            content={"audiences": [], "error": f"Token {cab['token']} not found in .env"},
        )

    headers = {"Authorization": f"Bearer {token}"}

    # нормализуем строку поиска
    q = (q or "").strip()

    # --- 1) узнаём count с теми же фильтрами, что и основной запрос ---

    if not q:
        # без строки поиска — общее количество без фильтров
        count_url = "https://ads.vk.com/api/v2/remarketing/segments.json?limit=1"
    else:
        # есть строка поиска — считаем количество только совпадающих по префиксу имени
        count_url = (
            "https://ads.vk.com/api/v2/remarketing/segments.json"
            f"?limit=1&_name__startswith={quote(q)}"
        )

    try:
        r0 = requests.get(count_url, headers=headers, timeout=10)
        j0 = r0.json()
        count = int(j0.get("count", 0))
    except Exception as e:
        return JSONResponse(
            status_code=502,
            content={"audiences": [], "error": f"VK count error: {str(e)}"},
        )

    # --- 2) по этому count берём "последние 50" с теми же фильтрами ---

    offset = max(0, count - 50)

    if not q:
        # без фильтра по имени — просто хвост списка
        url = (
            "https://ads.vk.com/api/v2/remarketing/segments.json"
            f"?limit=50&offset={offset}"
        )
    else:
        # тот же префикс по имени + смещение
        url = (
            "https://ads.vk.com/api/v2/remarketing/segments.json"
            f"?limit=50&offset={offset}&_name__startswith={quote(q)}"
        )

    try:
        r = requests.get(url, headers=headers, timeout=15)
        j = r.json()
        items = j.get("items", [])
    except Exception as e:
        return JSONResponse(
            status_code=502,
            content={"audiences": [], "error": f"VK search error: {str(e)}"},
        )

    out = [
        {
            "type": "vk",
            "id": str(it.get("id", "")),
            "name": it.get("name", ""),
            "created": it.get("created", ""),
        }
        for it in items
    ]

    return {"audiences": out}

# ====== USERS LISTS (remarketing/users_lists) ======

@secure_auto.get("/vk/users_lists/page")
@secure_api.get("/vk/users_lists/page")
def vk_users_lists_page(
    user_id: str,
    cabinet_id: str,
    offset: int = Query(-1, description="offset VK; -1 = последняя страница"),
    limit: int = Query(200, le=200),
):
    """
    Возвращает страницу списков из /api/v3/remarketing/users_lists.json.

    Если offset = -1 (по умолчанию) — считаем count и берём последнюю
    страницу (самые свежие списки).
    Для пролистывания фронт просто передаёт offset (на 200 меньше, чем
    предыдущий).
    """
    data = ensure_user_structure(user_id)
    cab = next((c for c in data["cabinets"] if str(c["id"]) == str(cabinet_id)), None)
    if not cab or not cab.get("token"):
        return JSONResponse(
            status_code=400,
            content={"items": [], "error": "Invalid cabinet or missing token"},
        )

    token = os.getenv(cab["token"])
    if not token:
        return JSONResponse(
            status_code=500,
            content={"items": [], "error": f"Token {cab['token']} not found in .env"},
        )

    headers = {"Authorization": f"Bearer {token}"}

    # --- 1) узнаём общее count, чтобы уметь брать "хвост" ---
    try:
        r0 = requests.get(
            "https://ads.vk.com/api/v3/remarketing/users_lists.json?limit=1",
            headers=headers,
            timeout=10,
        )
        j0 = r0.json()
        count = int(j0.get("count", 0))
    except Exception as e:
        return JSONResponse(
            status_code=502,
            content={"items": [], "error": f"VK users_lists count error: {str(e)}"},
        )

    if count == 0:
        return {"items": [], "count": 0, "limit": limit, "offset": 0}

    if offset < 0:
        # последняя страница (самые новые списки)
        real_offset = max(0, count - limit)
    else:
        real_offset = max(0, offset)

    url = (
        "https://ads.vk.com/api/v3/remarketing/users_lists.json"
        f"?limit={limit}&offset={real_offset}"
    )

    try:
        r = requests.get(url, headers=headers, timeout=15)
        j = r.json()
        items = j.get("items", [])
    except Exception as e:
        return JSONResponse(
            status_code=502,
            content={"items": [], "error": f"VK users_lists list error: {str(e)}"},
        )
        
    items = list(reversed(items))
    # Отдаём как есть, чтобы на фронте не дублировать поля
    return {
        "items": items,
        "count": count,
        "limit": limit,
        "offset": real_offset,
    }

@secure_auto.post("/vk/users_lists/create_segments")
@secure_api.post("/vk/users_lists/create_segments")
def create_segments_from_users_lists(payload: dict):
    user_id   = payload.get("userId")
    cabinet_id= payload.get("cabinetId")
    # Берём только выбранные чекбоксом id (фронт должен передавать их в listIds)
    raw_ids   = payload.get("listIds") or []
    mode      = (payload.get("mode") or "merge").lower()
    base_name = (payload.get("baseName") or "").strip()

    if not user_id or not cabinet_id:
        raise HTTPException(400, "Missing userId/cabinetId")

    # нормализуем список выбранных id: числа/строки -> int, убираем дубли/пустые
    try:
        list_ids = sorted({int(x) for x in raw_ids if str(x).strip()})
    except Exception:
        raise HTTPException(400, "listIds must be an array of ids")

    if not list_ids:
        raise HTTPException(400, "No selected lists (listIds is empty)")

    data = ensure_user_structure(user_id)
    cab = next((c for c in data["cabinets"] if str(c["id"]) == str(cabinet_id)), None)
    if not cab or not cab.get("token"):
        raise HTTPException(400, "Invalid cabinet or missing token")

    token = os.getenv(cab["token"])
    if not token:
        raise HTTPException(500, f"Token {cab['token']} not found in .env")

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    vk_url  = "https://ads.vk.com/api/v2/remarketing/segments.json"

    def make_relations(ids: list[int]):
        return [
            {"object_type": "remarketing_users_list",
             "params": {"source_id": int(lid), "type": "positive"}}
            for lid in ids
        ]

    # Хелпер: получить имя списка по id (как в прошлом сообщении)
    def _fetch_users_list_name(headers: dict, list_id: int) -> str:
        lid = int(list_id)
        base = "https://ads.vk.com/api/v3/remarketing/users_lists.json"
        for url in (f"{base}?limit=1&ids={lid}", f"{base}?limit=1&id={lid}", f"{base}?limit=1&_id__in={lid}"):
            try:
                r = requests.get(url, headers=headers, timeout=10)
                items = (r.json().get("items") or [])
                for it in items:
                    if int(it.get("id", -1)) == lid:
                        nm = (it.get("name") or "").strip()
                        if nm: return nm
            except Exception as e:
                log_error(f"users_list_name try1 failed id={lid}: {repr(e)}")
        try:
            r0 = requests.get(f"{base}?limit=1", headers=headers, timeout=10)
            count = int(r0.json().get("count", 0))
            if count > 0:
                offset = max(0, count - 200)
                r1 = requests.get(f"{base}?limit=200&offset={offset}", headers=headers, timeout=15)
                for it in (r1.json().get("items") or []):
                    if int(it.get("id", -1)) == lid:
                        nm = (it.get("name") or "").strip()
                        if nm: return nm
        except Exception as e:
            log_error(f"users_list_name try2 failed id={lid}: {repr(e)}")
        return f"Список {lid}"

    if mode == "merge":
        # Объединяем только выбранные списки; логическое ИЛИ -> pass_condition = 1
        if not base_name:
            base_name = "Новый сегмент"
        body = {
            "name": base_name,
            "relations": make_relations(list_ids),
            "pass_condition": 1,
        }
        resp = requests.post(vk_url, headers=headers, data=json.dumps(body), timeout=20)
        if resp.status_code != 200:
            # логируем в global_error
            try:
                vk_err = resp.json()
            except Exception:
                vk_err = {"raw": resp.text[:400]}
            log_error(f"VK segments MERGE failed cab={cabinet_id} ids={list_ids} err={vk_err}")
            return JSONResponse(status_code=502, content={"error": vk_err})
        return {"status": "ok", "mode": "merge", "segment": resp.json()}

    # mode == "per_list": по ОТМЕЧЕННОМУ списку -> отдельный сегмент с тем же именем
    created, errors = [], []
    for lid in list_ids:
        seg_name = _fetch_users_list_name(headers, lid)  # точное имя из VK
        body = {
            "name": seg_name,
            "relations": make_relations([lid]),
            "pass_condition": 1,  # одно условие
        }
        try:
            resp = requests.post(vk_url, headers=headers, data=json.dumps(body), timeout=20)
            if resp.status_code == 200:
                created.append({"list_id": lid, "segment": resp.json()})
            else:
                try:
                    vk_err = resp.json()
                except Exception:
                    vk_err = {"raw": resp.text[:400]}
                log_error(f"VK segments PER_LIST failed cab={cabinet_id} list_id={lid} name='{seg_name}' err={vk_err}")
                errors.append({"list_id": lid, "error": vk_err, "status": resp.status_code})
        except Exception as e:
            log_error(f"VK segments PER_LIST exception cab={cabinet_id} list_id={lid} name='{seg_name}' err={repr(e)}")
            errors.append({"list_id": lid, "error": str(e)})

    return {"status": "ok" if not errors else "partial", "mode": "per_list", "created": created, "errors": errors}

# -------------------------------------
#   SETTINGS (theme, language, any future)
# -------------------------------------
@secure_api.post("/settings/save")
async def save_settings(payload: dict):
    user_id = payload.get("userId")
    settings = payload.get("settings")

    if not user_id or not settings:
        raise HTTPException(400, "Missing userId or settings")

    user_dir = USERS_DIR / user_id
    info_file = user_dir / f"{user_id}.json"

    data = ensure_user_structure(user_id)

    # Обновляем только settings-поле
    data.update(settings)

    with open(info_file, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return {"status": "ok"}

@secure_auto.get("/settings/get")
@secure_api.get("/settings/get")
def get_settings(user_id: str):
    try:
        data = ensure_user_structure(user_id)
        return {"settings": data}
    except Exception as e:
        log_error(f"/api/settings/get failed for {user_id}: {repr(e)}")
        return JSONResponse(status_code=500, content={"error":"Internal Server Error"})
# --------------  TEXT  -----------------

@secure_api.post("/textsets/save")
def save_textsets(payload: dict):
    user_id = payload.get("userId")
    cabinet_id = payload.get("cabinetId")
    sets = payload.get("textsets", [])
    if not user_id or cabinet_id is None:
        log_error(f"textsets/save: missing userId={user_id} cabinetId={cabinet_id}")
        raise HTTPException(400, "Missing userId or cabinetId")

    try:
        ensure_user_structure(user_id)
        f = textsets_path(user_id, str(cabinet_id))
        lock = f.with_suffix(f.suffix + ".lock")

        # атомарная запись под блокировкой
        with file_lock(lock):
            atomic_write_json(f, sets if isinstance(sets, list) else [])
        return {"status": "ok"}
    except Exception as e:
        log_error(f"textsets/save[{user_id}/{cabinet_id}] error: {repr(e)}")
        return JSONResponse(status_code=500, content={"error": "Internal Server Error"})

@secure_auto.get("/textsets/get")
@secure_api.get("/textsets/get")
def get_textsets(user_id: str, cabinet_id: str):
    try:
        ensure_user_structure(user_id)
        f = textsets_path(user_id, str(cabinet_id))
        if not f.exists():
            return {"textsets": []}

        lock = f.with_suffix(f.suffix + ".lock")
        with file_lock(lock):
            with open(f, "r", encoding="utf-8") as fh:
                text = fh.read()

        try:
            data = json.loads(text) if text.strip() else []
            if not isinstance(data, list):
                data = []
            return {"textsets": data}
        except json.JSONDecodeError as je:
            # переименуем битый файл, чтобы не валить последующие запросы
            bad = f.with_suffix(f.suffix + f".bad_{int(datetime.utcnow().timestamp())}")
            try:
                with open(bad, "w", encoding="utf-8") as bfh:
                    bfh.write(text)
            except Exception as e2:
                log_error(f"textsets/get failed to write .bad: {repr(e2)}")
            log_error(f"textsets/get JSONDecodeError on {f}: {repr(je)}; moved to {bad.name}")
            return {"textsets": []}
    except Exception as e:
        log_error(f"textsets/get[{user_id}/{cabinet_id}] error: {repr(e)}")
        return JSONResponse(status_code=500, content={"error": "Internal Server Error"})

# -------------------------------------
#   FILE STORAGE (videos/images)
# -------------------------------------
@secure_api.post("/upload")
async def upload_creative(
    user_id: str,
    cabinet_id: str,
    file: UploadFile = File(...),
):
    content_type = (file.content_type or "").lower()
    filename_lower = (file.filename or "").lower()

    is_image = content_type.startswith("image/")
    is_video = content_type.startswith("video/") or filename_lower.endswith(
        (".mov", ".mp4", ".m4v", ".webm", ".avi", ".mkv")
    )

    if not (is_image or is_video):
        raise HTTPException(400, "Only image or video allowed")

    # сохраним загрузку во временный файл
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = Path(tmp.name)

    try:
        # определяем размеры
        if is_image:
            img = Image.open(tmp_path)
            width, height = img.size
        else:
            # можно позже улучшить детекцию через ffprobe
            width, height = (720, 1280)

        # грузим структуру пользователя
        data = ensure_user_structure(user_id)

        # если cabinet_id == "all" — все реальные кабинеты с токенами
        if cabinet_id == "all":
            target_cabinets = [
                c for c in data["cabinets"]
                if str(c.get("id")) != "all" and c.get("token")
            ]
        else:
            target_cabinets = [
                c for c in data["cabinets"]
                if str(c.get("id")) == str(cabinet_id)
            ]

        if not target_cabinets:
            raise HTTPException(400, "No valid cabinets found")

        results = []

        for cabinet in target_cabinets:
            token_name = cabinet.get("token")
            if not token_name:
                # пропустим кабинет без токена
                continue

            real_token = os.getenv(token_name)
            if not real_token:
                raise HTTPException(500, f"Token {token_name} not found in environment")

            vk_url = (
                "https://ads.vk.com/api/v2/content/static.json"
                if is_image else
                "https://ads.vk.com/api/v2/content/video.json"
            )
            headers = {"Authorization": f"Bearer {real_token}"}
            with open(tmp_path, "rb") as fh:
                files = {
                    "file": (file.filename, fh, content_type or "application/octet-stream"),
                    "data": (None, json.dumps({"width": width, "height": height}), "application/json"),
                }
                resp = requests.post(vk_url, headers=headers, files=files, timeout=60)

            if resp.status_code != 200:
                return {
                    "status": "error",
                    "cabinet_id": cabinet["id"],
                    "vk_error": resp.text,
                }

            resp_json = resp.json()
            vk_id = resp_json.get("id")
            if not vk_id:
                raise HTTPException(500, f"No id returned for cabinet {cabinet['id']}")

            # кладём локальную копию под vk_id
            storage = cabinet_storage(cabinet["id"])
            
            # подбираем отображаемое имя заранее
            display_name = next_display_name(storage, file.filename)
            final_name = f"{vk_id}_{display_name}"
            final_path = storage / final_name
            
            # копируем ОДИН раз
            shutil.copy(tmp_path, final_path)
            
            # генерим превью (только для видео) уже от конечного файла
            thumb_url = None
            if is_video:
                try:
                    thumb_name = f"{final_name}.jpg"
                    thumb_path = storage / thumb_name
                    proc = subprocess.run(
                        [
                            "ffmpeg", "-y",
                            "-ss", "1",
                            "-i", str(final_path),
                            "-vframes", "1",
                            "-vf", "scale=360:-1",
                            str(thumb_path),
                        ],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                    )
                    if proc.returncode == 0 and thumb_path.exists():
                        thumb_url = f"/auto_ads/video/{cabinet['id']}/{thumb_name}"
                    else:
                        log_error(f"ffmpeg failed for {final_path}: {proc.stderr[:400]}")
                except Exception as e:
                    log_error(f"thumb exception for {final_path}: {repr(e)}")

            try:
                base_no_ext, _ = os.path.splitext(final_name)
                meta_path = storage / f"{base_no_ext}.json"
                meta = {
                    "vk_response": resp_json,
                    "cabinet_id": str(cabinet["id"]),
                    "vk_id": vk_id,
                    "display_name": display_name,
                    "stored_file": f"/auto_ads/video/{cabinet['id']}/{final_name}",
                    "thumb_url": thumb_url,
                    "content_type": content_type or "",
                    "width": width,
                    "height": height,
                    "uploaded_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                    "type": "image" if is_image else "video",
                }
                # атомарно, как и по проекту
                atomic_write_json(storage / f"{os.path.splitext(final_name)[0]}.json", meta)
            except Exception as e:
                log_error(f"upload meta write failed for {final_path}: {repr(e)}")
            
            results.append({
                "cabinet_id": cabinet["id"],
                "vk_id": vk_id,
                "url": f"/auto_ads/video/{cabinet['id']}/{final_name}",
                "display_name": display_name,
                **({"thumb_url": thumb_url} if thumb_url else {}),
            })
        # --- Авто-добавление в creatives/sets.json выбранного кабинета ---
        try:
            ensure_user_structure(user_id)
            sets_file = creatives_path(user_id, cabinet_id)
            lock3 = sets_file.with_suffix(sets_file.suffix + ".lock")
            with file_lock(lock3):
                existing = []
                if sets_file.exists():
                    try:
                        existing = json.loads(sets_file.read_text(encoding="utf-8")) or []
                        if not isinstance(existing, list):
                            existing = []
                    except Exception:
                        existing = []

                # если набора нет — создадим "Набор 1"
                if not existing:
                    existing = [{"id": f"id_{uuid.uuid4().hex[:8]}", "name": "Набор 1", "items": []}]

                # добавляем как ОДИН item:
                # - если cabinet_id == "all": собираем urls/vkByCabinet
                # - иначе: одиночный url + id
                if str(cabinet_id) == "all":
                    urls = {str(r["cabinet_id"]): r["url"] for r in results if r.get("url")}
                    vk_by = {str(r["cabinet_id"]): r["vk_id"] for r in results if r.get("vk_id")}
                    # выберем "главный" id как первый из результатов (для удобства)
                    main_id = str(results[0]["vk_id"])
                    item = {
                        "id": main_id,
                        "url": results[0]["url"],
                        "urls": urls,
                        "vkByCabinet": vk_by,
                        "type": "image" if is_image else "video",
                    }
                    if is_video and results[0].get("thumb_url"):
                        item["thumbUrl"] = results[0]["thumb_url"]
                else:
                    r0 = results[0]
                    item = {
                        "id": str(r0["vk_id"]),
                        "url": r0["url"],
                        "vkByCabinet": {str(cabinet_id): str(r0["vk_id"])},
                        "type": "image" if is_image else "video",
                    }
                    if is_video and r0.get("thumb_url"):
                        item["thumbUrl"] = r0["thumb_url"]

                # добавляем в первый набор
                existing[0].setdefault("items", []).append(item)

                atomic_write_json(sets_file, existing)
        except Exception as e:
            log_error(f"upload: auto-append to sets.json failed: {repr(e)}")

        return {"status": "ok", "results": results}

    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass


# -------------------------------------
#   FRONTEND BUILD
# -------------------------------------
app.include_router(secure_api)
app.include_router(secure_auto)

@secure_api.get("/status")
def status():
    return {"status": "running"}
    
if FRONTEND_DIR.exists():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="auto_ads_frontend")

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8899, reload=True)
