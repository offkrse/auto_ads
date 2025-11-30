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

app = FastAPI()

VersionApp = "0.6"
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
@secure_api.post("/preset/save")
async def save_preset(payload: dict):
    user_id = payload.get("userId")
    cabinet_id = payload.get("cabinetId")
    preset = payload.get("preset")
    preset_id = payload.get("presetId")

    if not user_id or not cabinet_id or not preset:
        raise HTTPException(400, "userId, cabinetId and preset required")

    data = ensure_user_structure(user_id)

    # создаём новый id
    if not preset_id:
        preset_id = f"preset_{len(os.listdir(USERS_DIR / user_id / 'presets' / cabinet_id)) + 1}"

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
        "status": "active"
    }
    
    upsert_global_queue(queue_item)

    return {"status": "ok", "preset_id": preset_id}


@secure_api.get("/preset/list")
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
            presets.append({"preset_id": file.stem, "data": data})
        except Exception as e:
            log_error(f"Skip invalid preset file: {file} | {repr(e)}")

    return {"presets": presets}


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

            # если это видео и превью генерировалось по паттерну "<final_name>.jpg" — тоже уберём
            if it_type == "video":
                jpg1 = storage / (fname + ".jpg")
                _safe_unlink(jpg1)
                # иногда превью уже прислал фронт в thumbUrl, он выше добавлен

        return {"status": "ok", "deleted": deleted}
    except HTTPException:
        raise
    except Exception as e:
        log_error(f"/creative/delete error: {repr(e)}")
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
            final_name = f"{vk_id}_{file.filename}"
            final_path = storage / final_name
            shutil.copy(tmp_path, final_path)

            # генерим превью для видео
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

            results.append({
                "cabinet_id": cabinet["id"],
                "vk_id": vk_id,
                "url": f"/auto_ads/video/{cabinet['id']}/{final_name}",
                **({"thumb_url": thumb_url} if thumb_url else {}),
            })

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
