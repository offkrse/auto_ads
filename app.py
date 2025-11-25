from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path
import requests
from PIL import Image
import tempfile
import json
import shutil
import uvicorn
import os

app = FastAPI()

BASE_DIR = Path("/opt/auto_ads")
USERS_DIR = BASE_DIR / "users"
USERS_DIR.mkdir(parents=True, exist_ok=True)

STORAGE_DIR = Path("/mnt/data/auto_ads_storage/video")
STORAGE_DIR.mkdir(parents=True, exist_ok=True)

FRONTEND_DIR = BASE_DIR / "frontend"

# === env ===
from dotenv import load_dotenv
load_dotenv("/opt/auto_ads/.env")


# -------------------------------------
#   HELPERS
# -------------------------------------
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

# -------------------------------------
#   PRESETS (each in separate file)
# -------------------------------------
@app.post("/api/preset/save")
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
    with open(fpath, "w") as f:
        json.dump(preset, f, ensure_ascii=False, indent=2)

    return {"status": "ok", "preset_id": preset_id}


@app.get("/api/preset/list")
def list_presets(user_id: str, cabinet_id: str):
    ensure_user_structure(user_id)

    pdir = USERS_DIR / user_id / "presets" / cabinet_id
    presets = []

    for file in pdir.glob("*.json"):
        with open(file, "r") as f:
            presets.append({
                "preset_id": file.stem,
                "data": json.load(f)
            })

    return {"presets": presets}


@app.delete("/api/preset/delete")
def delete_preset(user_id: str, preset_id: str):
    info = load_user_info(user_id)

    if preset_id in info["presets"]:
        info["presets"].remove(preset_id)

    save_user_info(user_id, info)

    f = preset_file(user_id, preset_id)
    if f.exists():
        f.unlink()

    return {"status": "deleted"}


# -------------------------------------
#   CREATIVE SETS
# -------------------------------------
@app.post("/api/creatives/save")
async def save_creatives(payload: dict):
    user_id = payload.get("userId")
    cabinet_id = payload.get("cabinetId")
    creatives = payload.get("creatives")

    if not user_id or not cabinet_id:
        raise HTTPException(400, "Missing userId or cabinetId")

    ensure_user_structure(user_id)

    f = creatives_path(user_id, cabinet_id)
    with open(f, "w") as file:
        json.dump(creatives, file, ensure_ascii=False, indent=2)

    return {"status": "ok"}


@app.get("/api/creatives/get")
def get_creatives(user_id: str, cabinet_id: str):
    ensure_user_structure(user_id)

    f = creatives_path(user_id, cabinet_id)
    if not f.exists():
        return {"creatives": []}

    with open(f, "r") as file:
        return {"creatives": json.load(file)}


@app.get("/video/{cabinet_id}/{filename}")
def serve_file(cabinet_id: str, filename: str):
    path = cabinet_storage(cabinet_id) / filename
    if not path.exists():
        raise HTTPException(404, "File not found")
    return FileResponse(path)


# -------------------------------------
#   AUDIENCES
# -------------------------------------
@app.post("/api/audiences/save")
async def save_audiences(payload: dict):
    user_id = payload.get("userId")
    cabinet_id = payload.get("cabinetId")
    audiences = payload.get("audiences")

    ensure_user_structure(user_id)

    f = audiences_path(user_id, cabinet_id)
    with open(f, "w") as file:
        json.dump(audiences, file, ensure_ascii=False, indent=2)

    return {"status": "ok"}


@app.get("/api/audiences/get")
def get_audiences(user_id: str, cabinet_id: str):
    ensure_user_structure(user_id)

    f = audiences_path(user_id, cabinet_id)
    if not f.exists():
        return {"audiences": []}

    with open(f, "r") as file:
        return {"audiences": json.load(file)}


# -------------------------------------
#   SETTINGS (theme, language, any future)
# -------------------------------------
@app.post("/api/settings/save")
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


@app.get("/api/settings/get")
def get_settings(user_id: str):
    data = ensure_user_structure(user_id)
    return {"settings": data}

# -------------------------------------
#   FILE STORAGE (videos/images)
# -------------------------------------
@app.post("/api/upload")
async def upload_creative(
    user_id: str,
    cabinet_id: str,
    file: UploadFile = File(...)
):
    content_type = file.content_type
    is_image = content_type.startswith("image")
    is_video = content_type.startswith("video")

    if not (is_image or is_video):
        raise HTTPException(400, "Only image or video allowed")

    # подготовка для анализа размеров
    import tempfile
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = tmp.name

    # определяем ширину и высоту
    if is_image:
        img = Image.open(tmp_path)
        width, height = img.size
    else:
        width, height = (720, 1280)

    # берём токены
    data = ensure_user_structure(user_id)

    # если cabinet_id == "all", выбираем все кабинеты, кроме all
    if cabinet_id == "all":
        target_cabinets = [
            c for c in data["cabinets"]
            if c["id"] != "all" and c.get("token")
        ]
    else:
        target_cabinets = [
            c for c in data["cabinets"]
            if str(c["id"]) == str(cabinet_id)
        ]

    if not target_cabinets:
        raise HTTPException(400, "No valid cabinets found")

    results = []

    # перебираем все кабинеты
    for cabinet in target_cabinets:
        token_name = cabinet.get("token")
        if not token_name:
            continue

        real_token = os.getenv(token_name)
        if not real_token:
            raise HTTPException(500, f"Token {token_name} not found in environment")

        vk_url = (
            "https://ads.vk.com/api/v2/content/static.json"
            if is_image else
            "https://ads.vk.com/api/v2/content/video.json"
        )

        headers = {
            "Authorization": f"Bearer {real_token}"
        }

        files = {
            "file": (file.filename, open(tmp_path, "rb"), content_type),
            "data": (None, json.dumps({"width": width, "height": height}), "application/json")
        }

        # отправляем файл в VK ADS
        resp = requests.post(vk_url, headers=headers, files=files)

        if resp.status_code != 200:
            return {
                "status": "error",
                "cabinet_id": cabinet["id"],
                "vk_error": resp.text
            }

        resp_json = resp.json()
        vk_id = resp_json.get("id")
        if not vk_id:
            raise HTTPException(500, f"No id returned for cabinet {cabinet['id']}")

        # сохраняем локально
        storage = cabinet_storage(cabinet["id"])
        final_name = f"{vk_id}_{file.filename}"
        final_path = storage / final_name
        shutil.copy(tmp_path, final_path)

        results.append({
            "cabinet_id": cabinet["id"],
            "vk_id": vk_id,
            "url": f"/auto_ads/video/{cabinet['id']}/{final_name}"
        })

    return {
        "status": "ok",
        "results": results
    }


# -------------------------------------
#   FRONTEND BUILD
# -------------------------------------


@app.get("/api/status")
def status():
    return {"status": "running"}


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8899, reload=True)
