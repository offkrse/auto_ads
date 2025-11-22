from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path
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


# -------------------------------------
#   HELPERS
# -------------------------------------
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
    f = user_info_file(user_id)
    if not f.exists():
        data = {
            "user_id": user_id,
            "presets": [],
            "creatives": [],
            "audiences": [],
            "settings": {},
        }
        save_user_info(user_id, data)
        return data

    with open(f, "r") as file:
        return json.load(file)


def save_user_info(user_id: str, data: dict):
    f = user_info_file(user_id)
    with open(f, "w") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)


def preset_file(user_id: str, preset_id: str) -> Path:
    return udir(user_id) / "presets" / f"{preset_id}.json"


def creatives_file(user_id: str) -> Path:
    return udir(user_id) / "creatives" / "creatives.json"


def audiences_file(user_id: str) -> Path:
    return udir(user_id) / "audiences" / "audiences.json"


def settings_file(user_id: str) -> Path:
    return udir(user_id) / "settings.json"


# -------------------------------------
#   PRESETS (each in separate file)
# -------------------------------------
@app.post("/api/auto_ads/preset/save")
async def save_preset(payload: dict):
    user_id = payload.get("userId")
    preset = payload.get("preset")
    preset_id = payload.get("presetId")

    if not user_id or not preset:
        raise HTTPException(400, "userId and preset required")

    info = load_user_info(user_id)

    # создаём новый id если нет
    if not preset_id:
        preset_id = f"preset_{len(info['presets']) + 1}"

    # путь к файлу пресета
    file_path = preset_file(user_id, preset_id)

    with open(file_path, "w") as f:
        json.dump(preset, f, ensure_ascii=False, indent=2)

    # обновляем user info
    if preset_id not in info["presets"]:
        info["presets"].append(preset_id)

    save_user_info(user_id, info)

    return {"status": "ok", "preset_id": preset_id}


@app.get("/api/auto_ads/preset/list")
def list_presets(user_id: str):
    info = load_user_info(user_id)
    result = []

    for preset_id in info["presets"]:
        f = preset_file(user_id, preset_id)
        if f.exists():
            with open(f, "r") as file:
                result.append({
                    "preset_id": preset_id,
                    "data": json.load(file)
                })

    return {"presets": result}


@app.delete("/api/auto_ads/preset/delete")
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
@app.post("/api/auto_ads/creatives/save")
async def save_creatives(payload: dict):
    user_id = payload.get("userId")
    creatives = payload.get("creatives")

    if not user_id:
        raise HTTPException(400, "userId required")

    f = creatives_file(user_id)

    with open(f, "w") as file:
        json.dump(creatives, file, ensure_ascii=False, indent=2)

    info = load_user_info(user_id)
    info["creatives"] = creatives
    save_user_info(user_id, info)

    return {"status": "ok"}


@app.get("/api/auto_ads/creatives/get")
def get_creatives(user_id: str):
    f = creatives_file(user_id)
    if not f.exists():
        return {"creatives": []}

    with open(f, "r") as file:
        return {"creatives": json.load(file)}


# -------------------------------------
#   AUDIENCES
# -------------------------------------
@app.post("/api/auto_ads/audiences/save")
async def save_audiences(payload: dict):
    user_id = payload.get("userId")
    audiences = payload.get("audiences")

    f = audiences_file(user_id)
    with open(f, "w") as file:
        json.dump(audiences, file, ensure_ascii=False, indent=2)

    info = load_user_info(user_id)
    info["audiences"] = audiences
    save_user_info(user_id, info)

    return {"status": "ok"}


@app.get("/api/auto_ads/audiences/get")
def get_saved_audiences(user_id: str):
    f = audiences_file(user_id)
    if not f.exists():
        return {"audiences": []}

    with open(f, "r") as file:
        return {"audiences": json.load(file)}


# -------------------------------------
#   SETTINGS (theme, language, any future)
# -------------------------------------
@app.post("/api/auto_ads/settings/save")
async def save_settings(payload: dict):
    user_id = payload.get("userId")
    settings = payload.get("settings")

    f = settings_file(user_id)

    with open(f, "w") as file:
        json.dump(settings, file, ensure_ascii=False, indent=2)

    info = load_user_info(user_id)
    info["settings"] = settings
    save_user_info(user_id, info)

    return {"status": "ok"}


@app.get("/api/auto_ads/settings/get")
def get_settings(user_id: str):
    f = settings_file(user_id)
    if not f.exists():
        return {"settings": {}}

    with open(f, "r") as file:
        return {"settings": json.load(file)}


# -------------------------------------
#   FILE STORAGE (videos/images)
# -------------------------------------
@app.post("/api/auto_ads/upload")
async def upload_creative(file: UploadFile = File(...)):
    filename = file.filename
    save_path = STORAGE_DIR / filename

    with open(save_path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    url = f"/auto_ads/video/{filename}"
    return {"status": "ok", "url": url}


@app.get("/auto_ads/video/{filename}")
def serve_file(filename: str):
    path = STORAGE_DIR / filename
    if not path.exists():
        raise HTTPException(404, "File not found")
    return FileResponse(path)


# -------------------------------------
#   FRONTEND BUILD
# -------------------------------------
if FRONTEND_DIR.exists():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")


@app.get("/api/auto_ads/status")
def status():
    return {"status": "running"}


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8899, reload=True)
