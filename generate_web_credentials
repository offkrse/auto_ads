#!/usr/bin/env python3
"""
Скрипт для генерации файла web_credentials.json
на основе существующих пользователей в /opt/auto_ads/users/

Использование:
    python3 generate_web_credentials.py

Файл будет создан в /opt/auto_ads/data/web_credentials.json
"""

import os
import json
import random
import string
from pathlib import Path

USERS_DIR = Path("/opt/auto_ads/users")
DATA_DIR = Path("/opt/auto_ads/data")
CREDENTIALS_FILE = DATA_DIR / "web_credentials.json"


def generate_password(length: int = 5) -> str:
    """Генерирует случайный пароль из букв и цифр."""
    chars = string.ascii_lowercase + string.digits
    return ''.join(random.choice(chars) for _ in range(length))


def get_existing_tg_user_ids() -> list[str]:
    """Получает список tg_user_id из директорий пользователей."""
    if not USERS_DIR.exists():
        print(f"Директория {USERS_DIR} не существует!")
        return []
    
    user_ids = []
    for item in USERS_DIR.iterdir():
        if item.is_dir() and item.name.isdigit():
            user_ids.append(item.name)
    
    return sorted(user_ids, key=int)


def load_existing_credentials() -> dict:
    """Загружает существующие credentials если файл есть."""
    if CREDENTIALS_FILE.exists():
        try:
            return json.loads(CREDENTIALS_FILE.read_text(encoding="utf-8"))
        except:
            pass
    return {}


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    # Получаем существующих пользователей
    tg_user_ids = get_existing_tg_user_ids()
    
    if not tg_user_ids:
        print("Не найдено пользователей в", USERS_DIR)
        return
    
    print(f"Найдено {len(tg_user_ids)} пользователей")
    
    # Загружаем существующие credentials (чтобы не затереть пароли)
    existing = load_existing_credentials()
    
    credentials = {}
    new_users = []
    
    for tg_id in tg_user_ids:
        login = tg_id  # логин = tg_user_id
        
        # Проверяем есть ли уже этот пользователь
        if login in existing:
            # Сохраняем существующий пароль
            credentials[login] = existing[login]
            print(f"  [существует] {login}")
        else:
            # Генерируем новый пароль
            password = generate_password(5)
            credentials[login] = {
                "tg_user_id": tg_id,
                "password": password
            }
            new_users.append((login, password))
            print(f"  [новый] {login} -> пароль: {password}")
    
    # Сохраняем credentials
    CREDENTIALS_FILE.write_text(
        json.dumps(credentials, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    
    print(f"\nФайл сохранён: {CREDENTIALS_FILE}")
    print(f"Всего пользователей: {len(credentials)}")
    print(f"Новых пользователей: {len(new_users)}")
    
    if new_users:
        print("\n" + "="*50)
        print("НОВЫЕ УЧЁТНЫЕ ДАННЫЕ:")
        print("="*50)
        for login, password in new_users:
            print(f"Логин: {login}  |  Пароль: {password}")
        print("="*50)


if __name__ == "__main__":
    main()
