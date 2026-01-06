#!/usr/bin/env python3
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from loguru import logger

# Позволяет запускать файл напрямую: `python modules/db_utils.py`
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if __name__ == "__main__":
    root_s = str(PROJECT_ROOT)
    if root_s not in sys.path:
        sys.path.insert(0, root_s)

# Путь к БД квестов
QUESTS_DB_PATH = PROJECT_ROOT / "quests.db"


def init_quests_database(db_path: Path = QUESTS_DB_PATH) -> None:
    """
    Создает базу данных и таблицы для хранения выполненных кошельков по квестам.

    Args:
        db_path: Путь к файлу базы данных
    """
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        # Создаем таблицу completed_wallets
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS completed_wallets (
                address TEXT NOT NULL,
                module TEXT NOT NULL,
                completed_count INTEGER NOT NULL,
                target_count INTEGER NOT NULL,
                completed_at TIMESTAMP NOT NULL,
                last_check TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (address, module)
            )
            """
        )

        # Создаем индексы
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_completed_wallets_module 
            ON completed_wallets(module)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_completed_wallets_completed_at 
            ON completed_wallets(completed_at)
            """
        )

        conn.commit()
        conn.close()
        logger.debug(f"База данных квестов инициализирована: {db_path}")

    except Exception as e:
        logger.error(f"Ошибка при инициализации базы данных квестов: {e}")
        raise


def is_wallet_completed(
    address: str, module: str, db_path: Path = QUESTS_DB_PATH
) -> bool:
    """
    Проверяет, выполнен ли квест для указанного кошелька и модуля.

    Args:
        address: Адрес кошелька (checksum format)
        module: Название модуля ('redbutton', 'cashorcrash', 'uniswap')
        db_path: Путь к файлу базы данных

    Returns:
        True если кошелек уже выполнен, False если нет
    """
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT address, module, completed_count, target_count
            FROM completed_wallets
            WHERE address = ? AND module = ?
            """,
            (address, module),
        )

        row = cursor.fetchone()
        conn.close()

        if row:
            logger.debug(
                f"Кошелек {address} для модуля {module} найден в БД: {row[2]}/{row[3]}"
            )
            return True

        return False

    except Exception as e:
        logger.warning(f"Ошибка при проверке БД для {address} ({module}): {e}")
        # При ошибке БД возвращаем False, чтобы продолжить проверку через API
        return False


def get_wallet_progress(
    address: str, module: str, db_path: Path = QUESTS_DB_PATH
) -> Optional[dict]:
    """
    Получает информацию о прогрессе кошелька из БД.

    Args:
        address: Адрес кошелька (checksum format)
        module: Название модуля ('redbutton', 'cashorcrash', 'uniswap')
        db_path: Путь к файлу базы данных

    Returns:
        Словарь с данными о прогрессе или None если не найден
    """
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT address, module, completed_count, target_count, 
                   completed_at, last_check, created_at
            FROM completed_wallets
            WHERE address = ? AND module = ?
            """,
            (address, module),
        )

        row = cursor.fetchone()
        conn.close()

        if row:
            return {
                "address": row["address"],
                "module": row["module"],
                "completed_count": row["completed_count"],
                "target_count": row["target_count"],
                "completed_at": (
                    datetime.fromisoformat(row["completed_at"])
                    if row["completed_at"]
                    else None
                ),
                "last_check": (
                    datetime.fromisoformat(row["last_check"])
                    if row["last_check"]
                    else None
                ),
                "created_at": (
                    datetime.fromisoformat(row["created_at"])
                    if row["created_at"]
                    else None
                ),
            }

        return None

    except Exception as e:
        logger.warning(f"Ошибка при получении прогресса из БД для {address} ({module}): {e}")
        return None


def mark_wallet_completed(
    address: str,
    module: str,
    completed_count: int,
    target_count: int,
    db_path: Path = QUESTS_DB_PATH,
) -> None:
    """
    Сохраняет информацию о выполненном кошельке в БД.

    Args:
        address: Адрес кошелька (checksum format)
        module: Название модуля ('redbutton', 'cashorcrash', 'uniswap')
        completed_count: Выполненное количество транзакций
        target_count: Целевое количество транзакций
        db_path: Путь к файлу базы данных
    """
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        now_utc = datetime.now(timezone.utc).isoformat()

        cursor.execute(
            """
            INSERT OR REPLACE INTO completed_wallets 
            (address, module, completed_count, target_count, completed_at, last_check)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (address, module, completed_count, target_count, now_utc, now_utc),
        )

        conn.commit()
        conn.close()
        logger.debug(
            f"Кошелек {address} для модуля {module} сохранен в БД: {completed_count}/{target_count}"
        )

    except Exception as e:
        logger.error(f"Ошибка при сохранении кошелька в БД: {e}")


def update_wallet_last_check(
    address: str, module: str, db_path: Path = QUESTS_DB_PATH
) -> None:
    """
    Обновляет время последней проверки кошелька.

    Args:
        address: Адрес кошелька (checksum format)
        module: Название модуля ('redbutton', 'cashorcrash', 'uniswap')
        db_path: Путь к файлу базы данных
    """
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        now_utc = datetime.now(timezone.utc).isoformat()

        cursor.execute(
            """
            UPDATE completed_wallets
            SET last_check = ?
            WHERE address = ? AND module = ?
            """,
            (now_utc, address, module),
        )

        conn.commit()
        conn.close()

    except Exception as e:
        logger.debug(f"Ошибка при обновлении last_check для {address} ({module}): {e}")


def get_module_stats(module: str, db_path: Path = QUESTS_DB_PATH) -> dict:
    """
    Получает статистику по модулю из БД.

    Args:
        module: Название модуля ('redbutton', 'cashorcrash', 'uniswap')
        db_path: Путь к файлу базы данных

    Returns:
        Словарь со статистикой
    """
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT COUNT(*) 
            FROM completed_wallets
            WHERE module = ?
            """,
            (module,),
        )

        total = cursor.fetchone()[0]

        conn.close()

        return {
            "module": module,
            "total_completed": total,
        }

    except Exception as e:
        logger.error(f"Ошибка при получении статистики для модуля {module}: {e}")
        return {"module": module, "total_completed": 0}


# ==================== ФУНКЦИИ ДЛЯ РАБОТЫ С HARKAN ACCOUNTS ====================


def init_harkan_accounts_table(db_path: Path = QUESTS_DB_PATH) -> None:
    """
    Создает таблицу harkan_accounts для хранения данных аккаунтов Harkan.

    Args:
        db_path: Путь к файлу базы данных
    """
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        # Создаем таблицу harkan_accounts
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS harkan_accounts (
                wallet_address TEXT NOT NULL PRIMARY KEY,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                user_id TEXT NOT NULL,
                ip_address TEXT,
                access_token TEXT,
                refresh_token TEXT,
                claim_requested BOOLEAN DEFAULT 0,
                claim_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # Создаем индексы
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_harkan_accounts_username 
            ON harkan_accounts(username)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_harkan_accounts_claim_requested 
            ON harkan_accounts(claim_requested)
            """
        )

        conn.commit()
        conn.close()
        logger.debug(f"Таблица harkan_accounts инициализирована: {db_path}")

    except Exception as e:
        logger.error(f"Ошибка при инициализации таблицы harkan_accounts: {e}")
        raise


def get_harkan_account(
    wallet_address: str, db_path: Path = QUESTS_DB_PATH
) -> Optional[dict]:
    """
    Получает данные аккаунта Harkan из БД.

    Args:
        wallet_address: Адрес кошелька (checksum format)
        db_path: Путь к файлу базы данных

    Returns:
        Словарь с данными аккаунта или None если не найден
    """
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT wallet_address, username, password, user_id, ip_address,
                   access_token, refresh_token, claim_requested, claim_id,
                   created_at, updated_at
            FROM harkan_accounts
            WHERE wallet_address = ?
            """,
            (wallet_address,),
        )

        row = cursor.fetchone()
        conn.close()

        if row:
            return {
                "wallet_address": row["wallet_address"],
                "username": row["username"],
                "password": row["password"],
                "user_id": row["user_id"],
                "ip_address": row["ip_address"],
                "access_token": row["access_token"],
                "refresh_token": row["refresh_token"],
                "claim_requested": bool(row["claim_requested"]),
                "claim_id": row["claim_id"],
                "created_at": (
                    datetime.fromisoformat(row["created_at"])
                    if row["created_at"]
                    else None
                ),
                "updated_at": (
                    datetime.fromisoformat(row["updated_at"])
                    if row["updated_at"]
                    else None
                ),
            }

        return None

    except Exception as e:
        logger.warning(f"Ошибка при получении аккаунта Harkan для {wallet_address}: {e}")
        return None


def save_harkan_account(
    wallet_address: str,
    username: str,
    password: str,
    user_id: str,
    ip_address: Optional[str] = None,
    access_token: Optional[str] = None,
    refresh_token: Optional[str] = None,
    db_path: Path = QUESTS_DB_PATH,
) -> None:
    """
    Сохраняет данные аккаунта Harkan в БД.

    Args:
        wallet_address: Адрес кошелька (checksum format)
        username: Имя пользователя
        password: Пароль (в открытом виде)
        user_id: UUID пользователя
        ip_address: IP адрес прокси
        access_token: JWT токен доступа (опционально)
        refresh_token: Refresh токен (опционально)
        db_path: Путь к файлу базы данных
    """
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        now_utc = datetime.now(timezone.utc).isoformat()

        cursor.execute(
            """
            INSERT OR REPLACE INTO harkan_accounts 
            (wallet_address, username, password, user_id, ip_address,
             access_token, refresh_token, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                wallet_address,
                username,
                password,
                user_id,
                ip_address,
                access_token,
                refresh_token,
                now_utc,
                now_utc,
            ),
        )

        conn.commit()
        conn.close()
        logger.debug(f"Аккаунт Harkan для {wallet_address} сохранен в БД")

    except Exception as e:
        logger.error(f"Ошибка при сохранении аккаунта Harkan в БД: {e}")


def update_harkan_claim(
    wallet_address: str, claim_id: str, db_path: Path = QUESTS_DB_PATH
) -> None:
    """
    Обновляет информацию о поданной заявке на клайм NFT.

    Args:
        wallet_address: Адрес кошелька (checksum format)
        claim_id: ID заявки из ответа API
        db_path: Путь к файлу базы данных
    """
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        now_utc = datetime.now(timezone.utc).isoformat()

        cursor.execute(
            """
            UPDATE harkan_accounts
            SET claim_requested = 1, claim_id = ?, updated_at = ?
            WHERE wallet_address = ?
            """,
            (claim_id, now_utc, wallet_address),
        )

        conn.commit()
        conn.close()
        logger.debug(f"Заявка на клайм для {wallet_address} обновлена в БД")

    except Exception as e:
        logger.error(f"Ошибка при обновлении заявки Harkan в БД: {e}")


def is_harkan_claim_requested(
    wallet_address: str, db_path: Path = QUESTS_DB_PATH
) -> bool:
    """
    Проверяет, подана ли уже заявка на клайм NFT для кошелька.

    Args:
        wallet_address: Адрес кошелька (checksum format)
        db_path: Путь к файлу базы данных

    Returns:
        True если заявка уже подана, False если нет
    """
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT claim_requested
            FROM harkan_accounts
            WHERE wallet_address = ?
            """,
            (wallet_address,),
        )

        row = cursor.fetchone()
        conn.close()

        if row:
            return bool(row[0])

        return False

    except Exception as e:
        logger.warning(f"Ошибка при проверке заявки Harkan для {wallet_address}: {e}")
        return False


if __name__ == "__main__":
    # Тестирование функций
    logger.remove()
    logger.add(sys.stderr, level="DEBUG")

    init_quests_database()
    logger.info("База данных инициализирована")

    test_address = "0x1234567890123456789012345678901234567890"
    test_module = "redbutton"

    # Тест проверки несуществующего кошелька
    result = is_wallet_completed(test_address, test_module)
    logger.info(f"is_wallet_completed (не существует): {result}")

    # Тест сохранения
    mark_wallet_completed(test_address, test_module, 15, 15)
    logger.info("Кошелек сохранен в БД")

    # Тест проверки существующего кошелька
    result = is_wallet_completed(test_address, test_module)
    logger.info(f"is_wallet_completed (существует): {result}")

    # Тест получения прогресса
    progress = get_wallet_progress(test_address, test_module)
    logger.info(f"get_wallet_progress: {progress}")

    # Тест статистики
    stats = get_module_stats(test_module)
    logger.info(f"get_module_stats: {stats}")

