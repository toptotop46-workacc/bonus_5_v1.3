#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import random
import re
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Optional

import requests
from loguru import logger
from web3 import Web3

# Позволяет запускать файл напрямую: `python modules/reverie.py`
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if __name__ == "__main__":
    root_s = str(PROJECT_ROOT)
    if root_s not in sys.path:
        sys.path.insert(0, root_s)

# Импорт функций для работы с БД
from modules.db_utils import (
    init_quests_database,
    is_wallet_completed,
    mark_wallet_completed,
    QUESTS_DB_PATH,
)

# ==================== КОНФИГУРАЦИЯ ====================
# Конфиг RPC для Soneium
RPC_URL_DEFAULT = "https://soneium-rpc.publicnode.com"
CHAIN_ID = 1868

# URL страницы Reverie
REVERIE_URL = "https://www.alze.xyz/Reverie"

# Адрес контракта NFT Reverie
REVERIE_NFT_CONTRACT = "0x48dbb24faafd6c445299d7949b3cb11569b52033"

# ID расширения Rabby Wallet
RABBY_EXTENSION_ID = "acmacodkjbdgmoleebolmdjonilkdbch"

# ==================== ФУНКЦИИ ЗАГРУЗКИ ====================


def load_private_key(key_index: int = 0) -> str:
    """
    Загружает приватный ключ из файла keys.txt.

    Args:
        key_index: Индекс ключа (по умолчанию 0 - первый ключ)

    Returns:
        Приватный ключ как строка

    Raises:
        FileNotFoundError: Если файл не найден
        ValueError: Если ключ не найден или неверный формат
    """
    keys_file = PROJECT_ROOT / "keys.txt"
    if not keys_file.exists():
        raise FileNotFoundError(
            f"Файл {keys_file} не найден. "
            "Создайте файл и укажите в нем приватные ключи."
        )

    keys = []
    with open(keys_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            # Пропускаем комментарии и пустые строки
            if line and not line.startswith("#"):
                # Проверяем формат приватного ключа (64 символа hex)
                if re.match(r"^0x[a-fA-F0-9]{64}$", line):
                    keys.append(line)
                elif re.match(r"^[a-fA-F0-9]{64}$", line):
                    keys.append("0x" + line)

    if not keys:
        raise ValueError(f"В файле {keys_file} не найдено действительных приватных ключей")

    if key_index < 0 or key_index >= len(keys):
        raise ValueError(
            f"Индекс ключа {key_index} вне диапазона (доступно ключей: {len(keys)})"
        )

    return keys[key_index]


def load_all_keys() -> list[str]:
    """
    Загружает все приватные ключи из файла keys.txt.

    Returns:
        Список всех приватных ключей

    Raises:
        FileNotFoundError: Если файл не найден
        ValueError: Если не найдено действительных ключей
    """
    keys_file = PROJECT_ROOT / "keys.txt"
    if not keys_file.exists():
        raise FileNotFoundError(
            f"Файл {keys_file} не найден. "
            "Создайте файл и укажите в нем приватные ключи."
        )

    keys = []
    with open(keys_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            # Пропускаем комментарии и пустые строки
            if line and not line.startswith("#"):
                # Проверяем формат приватного ключа (64 символа hex)
                if re.match(r"^0x[a-fA-F0-9]{64}$", line):
                    keys.append(line)
                elif re.match(r"^[a-fA-F0-9]{64}$", line):
                    keys.append("0x" + line)

    if not keys:
        raise ValueError(f"В файле {keys_file} не найдено действительных приватных ключей")

    return keys


def load_adspower_api_key() -> str:
    """
    Загружает API ключ AdsPower из файла adspower_api_key.txt.

    Returns:
        API ключ как строка

    Raises:
        FileNotFoundError: Если файл не найден
        ValueError: Если файл пуст или ключ не найден
    """
    api_key_file = PROJECT_ROOT / "adspower_api_key.txt"

    if not api_key_file.exists():
        raise FileNotFoundError(
            f"Файл {api_key_file} не найден. "
            "Создайте файл и укажите в нем API ключ AdsPower."
        )

    with open(api_key_file, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f.readlines() if line.strip()]

    if not lines:
        raise ValueError(
            f"Файл {api_key_file} пуст. Укажите API ключ AdsPower в файле."
        )

    api_key = lines[0]  # Берем первую непустую строку

    if not api_key or api_key == "your_adspower_api_key_here":
        raise ValueError(
            f"В файле {api_key_file} указан шаблонный ключ. "
            "Замените его на реальный API ключ AdsPower."
        )

    return api_key


# ==================== ФУНКЦИИ ПРОВЕРКИ NFT ====================


def check_nft_balance(address: str) -> bool:
    """
    Проверяет, есть ли у кошелька NFT Reverie (баланс > 0).

    Args:
        address: Адрес кошелька (checksum format)

    Returns:
        True если есть NFT, False если нет
    """
    try:
        w3 = Web3(Web3.HTTPProvider(RPC_URL_DEFAULT, request_kwargs={"timeout": 30}))

        if not w3.is_connected():
            logger.warning("RPC недоступен при проверке баланса NFT")
            return False

        # ABI для функции balanceOf
        nft_abi = [
            {
                "inputs": [{"internalType": "address", "name": "owner", "type": "address"}],
                "name": "balanceOf",
                "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
                "stateMutability": "view",
                "type": "function",
            }
        ]

        contract = w3.eth.contract(
            address=Web3.to_checksum_address(REVERIE_NFT_CONTRACT), abi=nft_abi
        )

        balance = contract.functions.balanceOf(Web3.to_checksum_address(address)).call()

        has_nft = balance > 0

        if has_nft:
            logger.info(
                f"Кошелек {address} уже имеет NFT Reverie (баланс: {balance})"
            )
        else:
            logger.info(
                f"Кошелек {address} не имеет NFT Reverie (баланс: {balance})"
            )

        return has_nft

    except Exception as e:
        logger.error(f"Ошибка при проверке баланса NFT для {address}: {e}")
        return False


# ==================== КЛАСС REVERIE ====================


class Reverie:
    """
    Класс для создания и управления временными браузерами через AdsPower Local API.
    Создает временный профиль Windows, открывает браузер, открывает страницу Reverie,
    затем закрывает браузер и полностью удаляет профиль с кэшем.
    """

    def __init__(
        self,
        api_key: str,
        api_port: int = 50325,
        base_url: Optional[str] = None,
        timeout: int = 30,
    ):
        """
        Инициализация класса Reverie.

        Args:
            api_key: API ключ для AdsPower
            api_port: Порт API (по умолчанию 50325)
            base_url: Базовый URL API (если не указан, используется local.adspower.net)
            timeout: Таймаут для HTTP запросов в секундах
        """
        self.api_key = api_key
        self.api_port = api_port
        # Пробуем разные варианты базового URL
        if base_url:
            self.base_url = base_url
        else:
            # По умолчанию пробуем local.adspower.net, но можно использовать 127.0.0.1
            self.base_url = f"http://local.adspower.net:{api_port}"
        self.timeout = timeout
        self.profile_id: Optional[str] = None
        self.session = requests.Session()
        self.session.headers.update(
            {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}
        )
        # Время последнего запроса к API AdsPower (для rate limiting)
        self.last_request_time: float = 0.0
        # Минимальная задержка между запросами (в секундах)
        self.api_request_delay: float = 2.0

    def _make_request(
        self, method: str, endpoint: str, data: Optional[dict] = None
    ) -> dict[str, Any]:
        """
        Выполняет HTTP запрос к AdsPower API.
        Пробует разные способы передачи API ключа и форматы эндпоинтов.

        Args:
            method: HTTP метод (GET, POST, DELETE)
            endpoint: Эндпоинт API
            data: Данные для отправки (для POST/DELETE)

        Returns:
            Ответ API в виде словаря

        Raises:
            requests.RequestException: При ошибке HTTP запроса
            ValueError: При ошибке в ответе API
        """
        # Пробуем разные варианты эндпоинтов
        if "/api/v2/" in endpoint:
            endpoints_to_try = [endpoint]
        else:
            endpoints_to_try = [
                endpoint.replace("/api/v1/", "/api/v2/"),
                endpoint,
                endpoint.replace("/api/v1/", "/v1/"),
                endpoint.replace("/api/v1/", "/api/"),
            ]

        endpoints_to_try = list(dict.fromkeys(endpoints_to_try))

        # Добавляем задержку между запросами к API AdsPower для избежания rate limit
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < self.api_request_delay:
            sleep_time = self.api_request_delay - time_since_last_request
            logger.debug(
                f"Задержка {sleep_time:.2f} сек перед запросом к API AdsPower (rate limiting)"
            )
            time.sleep(sleep_time)

        last_error = None
        request_made = False

        for endpoint_variant in endpoints_to_try:
            url = f"{self.base_url}{endpoint_variant}"
            params = {"api_key": self.api_key}

            try:
                if method.upper() == "GET":
                    response = self.session.get(url, params=params, timeout=self.timeout)
                elif method.upper() == "POST":
                    logger.debug(f"POST запрос к {url} с данными: {data}")
                    response = self.session.post(
                        url, params=params, json=data, timeout=self.timeout
                    )
                    logger.debug(
                        f"Ответ: статус {response.status_code}, тело: {response.text[:200]}"
                    )
                elif method.upper() == "DELETE":
                    response = self.session.delete(
                        url, params=params, json=data, timeout=self.timeout
                    )
                else:
                    raise ValueError(f"Неподдерживаемый HTTP метод: {method}")

                request_made = True
                self.last_request_time = time.time()

                if response.status_code == 404:
                    last_error = f"404 Not Found: {url}"
                    logger.debug(
                        f"Эндпоинт {endpoint_variant} вернул 404, пробуем следующий вариант"
                    )
                    continue

                response.raise_for_status()
                result = response.json()

                if result.get("code") != 0:
                    error_msg = result.get("msg", "Неизвестная ошибка API")
                    raise ValueError(f"Ошибка API: {error_msg}")

                logger.debug(f"Успешный запрос к {endpoint_variant}")
                return result

            except requests.RequestException as e:
                last_error = str(e)
                if not request_made:
                    request_made = True
                    self.last_request_time = time.time()

                if hasattr(e, "response") and e.response is not None:
                    if e.response.status_code == 404:
                        logger.debug(
                            f"Эндпоинт {endpoint_variant} вернул 404, пробуем следующий вариант"
                        )
                        continue
                logger.debug(
                    f"Ошибка для {endpoint_variant}: {e}, пробуем следующий вариант"
                )
                continue
            except ValueError as e:
                raise

        raise requests.RequestException(
            f"Все варианты эндпоинтов вернули ошибку. Последняя ошибка: {last_error}"
        )

    def create_temp_profile(self, name: Optional[str] = None, use_proxy: bool = True) -> str:
        """
        Создает временный профиль Windows используя API v2.

        Args:
            name: Имя профиля (если не указано, генерируется автоматически)
            use_proxy: Использовать ли случайный прокси (по умолчанию True)

        Returns:
            ID созданного профиля
        """
        if name is None:
            timestamp = int(time.time())
            unique_id = str(uuid.uuid4())[:8]
            name = f"temp_reverie_{timestamp}_{unique_id}"

        logger.info(f"Создание временного профиля Windows: {name}")

        profile_data = {
            "name": name,
            "group_id": "0",
            "fingerprint_config": {
                "automatic_timezone": "1",
                "language": ["en-US", "en"],
                "webrtc": "disabled",
                "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            },
        }

        if use_proxy:
            profile_data["proxyid"] = "random"
            logger.info("Использование случайного прокси из сохраненных")
        else:
            profile_data["user_proxy_config"] = {"proxy_soft": "no_proxy"}
            logger.info("Профиль создается без прокси")

        try:
            result = self._make_request("POST", "/api/v2/browser-profile/create", profile_data)
            self.profile_id = result.get("data", {}).get("profile_id")
            if not self.profile_id:
                raise ValueError("API не вернул profile_id профиля")

            logger.success(f"Профиль создан успешно. ID: {self.profile_id}")
            return self.profile_id

        except Exception as e:
            logger.error(f"Ошибка при создании профиля: {e}")
            raise

    def start_browser(self, profile_id: Optional[str] = None) -> dict[str, Any]:
        """
        Запускает браузер с указанным профилем используя API v2.

        Args:
            profile_id: ID профиля (если не указан, используется последний созданный)

        Returns:
            Данные о запущенном браузере
        """
        profile_id_value = profile_id or self.profile_id
        if not profile_id_value:
            raise ValueError("Не указан profile_id и нет созданного профиля")

        logger.info(f"Запуск браузера для профиля {profile_id_value}")

        browser_data = {"profile_id": profile_id_value}

        try:
            result = self._make_request("POST", "/api/v2/browser-profile/start", browser_data)
            browser_info = result.get("data", {})

            if not browser_info:
                raise ValueError("API не вернул данные о браузере")

            logger.success(f"Браузер запущен успешно")
            logger.debug(f"Информация о браузере: {browser_info}")

            return browser_info

        except Exception as e:
            logger.error(f"Ошибка при запуске браузера: {e}")
            raise

    def stop_browser(self, profile_id: Optional[str] = None) -> bool:
        """
        Останавливает браузер для указанного профиля используя API v2.

        Args:
            profile_id: ID профиля (если не указан, используется последний созданный)

        Returns:
            True если браузер успешно остановлен
        """
        profile_id_value = profile_id or self.profile_id
        if not profile_id_value:
            logger.warning("Не указан profile_id для остановки браузера")
            return False

        logger.info(f"Остановка браузера для профиля {profile_id_value}")

        browser_data = {"profile_id": profile_id_value}

        try:
            self._make_request("POST", "/api/v2/browser-profile/stop", browser_data)
            logger.success(f"Браузер остановлен успешно")
            return True

        except Exception as e:
            logger.error(f"Ошибка при остановке браузера: {e}")
            return False

    def delete_cache(self, profile_id: Optional[str] = None) -> bool:
        """
        Очищает кэш профиля используя API v2.

        Args:
            profile_id: ID профиля (если не указан, используется последний созданный)

        Returns:
            True если кэш успешно очищен
        """
        profile_id_value = profile_id or self.profile_id
        if not profile_id_value:
            logger.warning("Не указан profile_id для очистки кэша")
            return False

        logger.info(f"Очистка кэша профиля {profile_id_value}")

        cache_data = {
            "profile_id": [profile_id_value],
            "type": [
                "local_storage",
                "indexeddb",
                "extension_cache",
                "cookie",
                "history",
                "image_file",
            ],
        }

        try:
            self._make_request("POST", "/api/v2/browser-profile/delete-cache", cache_data)
            logger.success(f"Кэш профиля {profile_id_value} очищен успешно")
            return True

        except Exception as e:
            logger.error(f"Ошибка при очистке кэша: {e}")
            return False

    def delete_profile(self, profile_id: Optional[str] = None, clear_cache: bool = True) -> bool:
        """
        Удаляет профиль используя API v2.

        Args:
            profile_id: ID профиля (если не указан, используется последний созданный)
            clear_cache: Очистить кэш перед удалением (по умолчанию True)

        Returns:
            True если профиль успешно удален
        """
        profile_id_value = profile_id or self.profile_id
        if not profile_id_value:
            logger.warning("Не указан profile_id для удаления профиля")
            return False

        if clear_cache:
            self.delete_cache(profile_id_value)

        logger.info(f"Удаление профиля {profile_id_value}")

        delete_data_variants = [
            {"profile_id": [profile_id_value]},
            {"Profile_id": [profile_id_value]},
        ]

        for delete_data in delete_data_variants:
            try:
                logger.debug(f"Пробуем удалить профиль с параметром: {list(delete_data.keys())[0]}")
                self._make_request("POST", "/api/v2/browser-profile/delete", delete_data)
                logger.success(f"Профиль {profile_id_value} удален успешно")
                self.profile_id = None
                return True
            except ValueError as e:
                error_msg = str(e)
                if "profile_id" in error_msg.lower() or "Profile_id" in error_msg:
                    logger.debug(
                        f"Вариант {list(delete_data.keys())[0]} не сработал: {e}, пробуем следующий"
                    )
                    continue
                raise
            except Exception as e:
                logger.error(f"Ошибка при удалении профиля: {e}")
                return False

        logger.error(f"Не удалось удалить профиль {profile_id_value} ни с одним вариантом параметра")
        return False

    async def _import_wallet_via_cdp(
        self, cdp_endpoint: str, private_key: str, password: str = "Password123"
    ) -> Optional[str]:
        """
        Импортирует кошелек Rabby через CDP endpoint.

        Args:
            cdp_endpoint: CDP endpoint (например, ws://127.0.0.1:9222)
            private_key: Приватный ключ для импорта
            password: Пароль для кошелька (по умолчанию Password123)

        Returns:
            Адрес импортированного кошелька или None, если не удалось извлечь
        """
        try:
            from playwright.async_api import async_playwright

            playwright = await async_playwright().start()
            try:
                browser = await playwright.chromium.connect_over_cdp(cdp_endpoint)

                if not browser.contexts:
                    logger.error("Не найдено контекстов в браузере (CDP)")
                    return None

                context = browser.contexts[0]

                # Ищем страницу с уже открытым расширением
                extension_id = RABBY_EXTENSION_ID
                setup_url = (
                    f"chrome-extension://{extension_id}/index.html#/new-user/guide"
                )

                page = None
                # Проверяем уже открытые страницы - ищем любую страницу расширения Rabby
                for existing_page in context.pages:
                    url = existing_page.url
                    # Проверяем, что это страница расширения Rabby
                    if extension_id in url or (
                        "chrome-extension://" in url and "rabby" in url.lower()
                    ):
                        page = existing_page
                        # Если это не страница настройки, переходим на неё
                        if "#/new-user/guide" not in url:
                            await page.goto(setup_url)
                            await asyncio.sleep(2)  # Даём время на загрузку
                        break

                # Если страница не найдена, открываем её
                if not page:
                    page = await context.new_page()
                    await page.goto(setup_url)
                    await asyncio.sleep(3)  # Даём время на загрузку

                # Шаг 1: Нажимаем "I already have an address"
                await page.wait_for_selector(
                    'span:has-text("I already have an address")', timeout=30000
                )
                await page.click('span:has-text("I already have an address")')

                # Шаг 2: Выбираем "Private Key"
                private_key_selector = 'div.rabby-ItemWrapper-rabby--mylnj7:has-text("Private Key")'
                await page.wait_for_selector(private_key_selector, timeout=30000)
                await page.click(private_key_selector)

                # Шаг 3: Вводим приватный ключ
                private_key_input = "#privateKey"
                await page.wait_for_selector(private_key_input, timeout=30000)
                await page.click(private_key_input)
                await page.fill(private_key_input, private_key)

                # Шаг 4: Подтверждаем импорт ключа
                confirm_button_selector = 'button:has-text("Confirm"):not([disabled])'
                await page.wait_for_selector(confirm_button_selector, timeout=30000)
                await page.click(confirm_button_selector)

                # Шаг 5: Вводим пароль
                password_input = "#password"
                await page.wait_for_selector(password_input, timeout=30000)
                await page.click(password_input)
                await page.fill(password_input, password)
                await page.press(password_input, "Tab")
                await page.keyboard.type(password)

                # Шаг 6: Подтверждаем установку пароля
                password_confirm_button = 'button:has-text("Confirm"):not([disabled])'
                await page.wait_for_selector(password_confirm_button, timeout=30000)
                await page.click(password_confirm_button)

                # Шаг 7: Ждём успешного импорта
                await page.wait_for_selector("text=Imported Successfully", timeout=30000)

                # Пытаемся извлечь адрес кошелька
                wallet_address = None
                try:
                    address = await page.evaluate(
                        """
                        () => {
                            const text = document.body.textContent;
                            const match = text.match(/0x[a-fA-F0-9]{40}/);
                            return match ? match[0] : null;
                        }
                    """
                    )
                    if address:
                        wallet_address = address
                except Exception:
                    pass

                return wallet_address

            finally:
                # В CDP-режиме не закрываем браузер/контекст — ими управляет AdsPower
                await playwright.stop()

        except Exception as e:
            logger.error(f"Ошибка при импорте кошелька: {e}")
            raise

    async def _open_reverie_page_via_cdp(
        self, cdp_endpoint: str, wallet_address: Optional[str] = None
    ) -> bool:
        """
        Открывает страницу Reverie через CDP.

        Args:
            cdp_endpoint: CDP endpoint (например, ws://127.0.0.1:9222)
            wallet_address: Адрес кошелька для логирования (опционально)

        Returns:
            True если успешно открыли страницу, False в случае ошибки
        """
        try:
            from playwright.async_api import async_playwright

            playwright = await async_playwright().start()
            try:
                browser = await playwright.chromium.connect_over_cdp(cdp_endpoint)

                if not browser.contexts:
                    logger.error("Не найдено контекстов в браузере (CDP)")
                    return False

                context = browser.contexts[0]

                # Закрываем все страницы расширения кошелька
                logger.info("Закрытие страниц расширения кошелька...")
                extension_pages = []
                for existing_page in context.pages:
                    if existing_page.url.startswith("chrome-extension://"):
                        extension_pages.append(existing_page)

                for ext_page in extension_pages:
                    try:
                        await ext_page.close()
                        logger.debug(f"Закрыта страница расширения: {ext_page.url}")
                    except Exception as e:
                        logger.debug(f"Ошибка при закрытии страницы расширения: {e}")

                if extension_pages:
                    logger.success(f"Закрыто страниц расширения: {len(extension_pages)}")
                    await asyncio.sleep(1)  # Небольшая задержка после закрытия

                # Открываем новую страницу или используем существующую не-расширение страницу
                page = None
                for existing_page in context.pages:
                    # Используем первую не-расширение страницу
                    if not existing_page.url.startswith("chrome-extension://"):
                        page = existing_page
                        break

                if not page:
                    page = await context.new_page()

                # Переходим на страницу Reverie
                logger.info(f"Переход на страницу Reverie: {REVERIE_URL}")
                try:
                    await page.goto(REVERIE_URL, wait_until="domcontentloaded", timeout=60000)
                except Exception as e:
                    logger.debug(f"domcontentloaded не завершился, пробуем load: {e}")
                    try:
                        await page.goto(REVERIE_URL, wait_until="load", timeout=30000)
                    except Exception:
                        await page.goto(REVERIE_URL, timeout=30000)

                await asyncio.sleep(5)  # Даём время на загрузку страницы

                # Нажимаем кнопку "Connect Wallet"
                logger.info("Нажимаю кнопку 'Connect Wallet'...")
                connect_wallet_clicked = False
                try:
                    connect_wallet_selectors = [
                        'button[data-test="connect-wallet-button"]',
                        'button:has-text("Connect Wallet")',
                        'button:has-text("Connect wallet")',
                        '[role="button"]:has-text("Connect Wallet")',
                    ]

                    for attempt in range(30):  # Пробуем до 30 раз с интервалом 1 сек
                        for selector in connect_wallet_selectors:
                            try:
                                connect_wallet_button = await page.query_selector(selector)
                                if connect_wallet_button:
                                    is_disabled = await connect_wallet_button.is_disabled()
                                    is_visible = await connect_wallet_button.is_visible()

                                    if not is_disabled and is_visible:
                                        await connect_wallet_button.click()
                                        logger.success("Кнопка 'Connect Wallet' нажата")
                                        connect_wallet_clicked = True
                                        await asyncio.sleep(3)  # Даём время на открытие модального окна
                                        break
                            except Exception:
                                continue

                        if connect_wallet_clicked:
                            break

                        await asyncio.sleep(1)

                    if not connect_wallet_clicked:
                        logger.warning("Не удалось найти активную кнопку 'Connect Wallet' за 30 секунд")
                        return False
                except Exception as e:
                    logger.warning(f"Ошибка при поиске/клике кнопки 'Connect Wallet': {e}")
                    return False

                # Ждем появления модального окна подключения кошелька
                await asyncio.sleep(3)

                # Выбираем Rabby Wallet в модальном окне
                logger.info("Выбираю Rabby Wallet в модальном окне...")
                rabby_wallet_clicked = False
                try:
                    rabby_selectors = [
                        'span.css-1g4povx:has-text("Rabby Wallet")',
                        'span:has-text("Rabby Wallet")',
                        'button:has-text("Rabby Wallet")',
                        'div:has-text("Rabby Wallet")',
                    ]

                    for attempt in range(10):  # Пробуем до 10 раз
                        for selector in rabby_selectors:
                            try:
                                rabby_element = await page.wait_for_selector(
                                    selector, timeout=2000
                                )
                                if rabby_element:
                                    await rabby_element.click()
                                    logger.success("Rabby Wallet выбран")
                                    rabby_wallet_clicked = True
                                    await asyncio.sleep(2)
                                    break
                            except Exception:
                                continue

                        if rabby_wallet_clicked:
                            break

                        await asyncio.sleep(1)

                    if not rabby_wallet_clicked:
                        logger.warning("Не удалось найти Rabby Wallet в модальном окне")
                        return False
                except Exception as e:
                    logger.warning(f"Ошибка при выборе Rabby Wallet: {e}")
                    return False

                # Ждем появления popup окна Rabby Wallet
                logger.info("Ожидаю появления popup окна Rabby Wallet...")
                rabby_popup_page: Optional[Any] = None

                for attempt in range(10):
                    for existing_page in context.pages:
                        url = existing_page.url
                        if (
                            "chrome-extension://" in url
                            and "/notification.html" in url
                        ):
                            rabby_popup_page = existing_page
                            logger.success("Найдено popup окно кошелька")
                            break

                    if rabby_popup_page:
                        break

                    await asyncio.sleep(2)

                if rabby_popup_page:
                    await rabby_popup_page.bring_to_front()

                    # Проверяем наличие элемента "Ignore all" и кликаем по нему, если есть
                    logger.info("Проверка наличия элемента 'Ignore all' в расширении...")
                    ignore_all_clicked = False
                    try:
                        ignore_all_selectors = [
                            'span.underline.text-13.font-medium.cursor-pointer:has-text("Ignore all")',
                            'span.underline:has-text("Ignore all")',
                            'span:has-text("Ignore all")',
                        ]

                        for selector in ignore_all_selectors:
                            try:
                                ignore_all_element = await rabby_popup_page.wait_for_selector(
                                    selector, timeout=5000
                                )
                                if ignore_all_element:
                                    await ignore_all_element.click()
                                    logger.success("Элемент 'Ignore all' нажат успешно")
                                    ignore_all_clicked = True
                                    await asyncio.sleep(1)  # Даём время на обработку
                                    break
                            except Exception:
                                continue

                        if not ignore_all_clicked:
                            logger.debug("Элемент 'Ignore all' не найден, продолжаем...")
                    except Exception as e:
                        logger.debug(f"Ошибка при поиске 'Ignore all': {e}, продолжаем...")

                    # Кликаем по кнопке "Connect" в popup
                    logger.info('Ищу кнопку "Connect" в popup...')
                    connect_clicked = False
                    try:
                        await rabby_popup_page.wait_for_selector(
                            'button:has-text("Connect")', timeout=10000
                        )
                        await rabby_popup_page.click('button:has-text("Connect")')
                        logger.success('Кликнул по кнопке "Connect" в popup')
                        connect_clicked = True
                        await asyncio.sleep(3)
                    except Exception as e:
                        logger.warning(
                            f'Не удалось найти кнопку Connect в popup окне: {e}'
                        )

                    if connect_clicked:
                        logger.success("Подключение кошелька выполнено успешно")
                else:
                    logger.warning("Popup окно Rabby Wallet не найдено за 20 секунд")

                # Переключаемся обратно на основную страницу Reverie
                await page.bring_to_front()
                await asyncio.sleep(3)  # Даём время на обновление страницы после подключения

                # Выполняем задания: кликаем по всем 4 заданиям
                logger.info("Начинаю выполнение заданий...")
                
                # Ищем все блоки заданий на странице
                logger.info("Ищу блоки заданий на странице...")
                
                # Ищем все div с нужными классами
                all_task_blocks = await page.query_selector_all(
                    'div.flex.justify-between.items-center'
                )
                
                task_blocks = []
                # Фильтруем только те, что содержат текст заданий и находятся в секции "Finish all 4 tasks"
                for block in all_task_blocks:
                    try:
                        block_text = await block.text_content()
                        if block_text and (
                            'Follow On X' in block_text or
                            'Retweet on X' in block_text or
                            'Check Alze ID' in block_text
                        ):
                            # Проверяем, что блок находится в секции "Finish all 4 tasks"
                            is_in_tasks_section = await block.evaluate(
                                """
                                (el) => {
                                    let element = el;
                                    for (let i = 0; i < 10; i++) {
                                        if (!element) break;
                                        const parent = element.parentElement;
                                        if (parent && parent.textContent.includes('Finish all 4 tasks')) {
                                            return true;
                                        }
                                        element = parent;
                                    }
                                    return false;
                                }
                                """
                            )
                            
                            if is_in_tasks_section:
                                task_blocks.append(block)
                    except Exception:
                        continue

                logger.info(f"Всего найдено блоков заданий: {len(task_blocks)}")

                completed_tasks = 0
                clicked_blocks = []  # Индексы кликнутых блоков

                # Кликаем по кнопкам в каждом блоке заданий
                for block_index, task_block in enumerate(task_blocks):
                    if block_index >= 4:  # Всего 4 задания
                        break

                    try:
                        # Получаем текст задания из блока
                        task_text = await task_block.text_content()
                        task_name = "Unknown"
                        if "Follow On X" in task_text:
                            task_name = "Follow On X"
                        elif "Retweet on X" in task_text:
                            task_name = "Retweet on X"
                        elif "Check Alze ID" in task_text:
                            task_name = "Check Alze ID"

                        logger.info(f"Обрабатываю задание {block_index + 1}/4: {task_name}...")

                        # Проверяем, выполнено ли задание (текст зачеркнут или кнопка disabled)
                        is_completed = await task_block.evaluate(
                            """
                            (el) => {
                                // Проверяем, есть ли зачеркнутый текст
                                const strikethrough = el.querySelector('span.line-through');
                                if (strikethrough) {
                                    return true;
                                }
                                // Проверяем, есть ли disabled кнопка
                                const button = el.querySelector('button[disabled]');
                                if (button) {
                                    return true;
                                }
                                return false;
                            }
                            """
                        )

                        if is_completed:
                            logger.info(f"Задание '{task_name}' уже выполнено")
                            completed_tasks += 1
                            continue

                        # Ищем кнопку внутри блока
                        task_button = await task_block.query_selector('button')
                        
                        if not task_button:
                            logger.warning(f"Не найдена кнопка для задания '{task_name}'")
                            continue

                        # Проверяем, что кнопка не disabled
                        is_disabled = await task_button.is_disabled()
                        if is_disabled:
                            logger.info(f"Кнопка задания '{task_name}' уже disabled (выполнено)")
                            completed_tasks += 1
                            continue

                        # Для "Retweet on X" проверяем, что это другая кнопка
                        if task_name == "Retweet on X":
                            if block_index in clicked_blocks:
                                logger.info(f"Задание '{task_name}' (блок {block_index}) уже обработано")
                                continue

                        # Запоминаем текущие вкладки перед кликом
                        pages_before_click = set()
                        for existing_page in context.pages:
                            if not existing_page.url.startswith("chrome-extension://"):
                                pages_before_click.add(existing_page)

                        # Кликаем по кнопке
                        await task_button.click()
                        logger.success(f"Кликнул по кнопке задания '{task_name}'")
                        clicked_blocks.append(block_index)
                        
                        # Ждём немного, чтобы новые вкладки успели открыться
                        await asyncio.sleep(2)
                        
                        # Закрываем все новые вкладки, кроме главной страницы Reverie
                        try:
                            pages_after_click = context.pages
                            reverie_page = None
                            
                            # Находим главную страницу Reverie
                            for p in pages_after_click:
                                if REVERIE_URL in p.url or "alze.xyz/Reverie" in p.url:
                                    reverie_page = p
                                    break
                            
                            # Закрываем все новые вкладки
                            closed_count = 0
                            for new_page in pages_after_click:
                                # Пропускаем расширения и главную страницу Reverie
                                if new_page.url.startswith("chrome-extension://"):
                                    continue
                                if new_page == reverie_page:
                                    continue
                                if new_page in pages_before_click:
                                    continue
                                
                                try:
                                    await new_page.close()
                                    closed_count += 1
                                    logger.debug(f"Закрыта новая вкладка: {new_page.url}")
                                except Exception as e:
                                    logger.debug(f"Ошибка при закрытии вкладки: {e}")
                            
                            if closed_count > 0:
                                logger.info(f"Закрыто новых вкладок: {closed_count}")
                            
                            # Возвращаемся на главную страницу Reverie
                            if reverie_page:
                                await reverie_page.bring_to_front()
                                await asyncio.sleep(1)
                        except Exception as e:
                            logger.debug(f"Ошибка при закрытии новых вкладок: {e}")
                        
                        # Ждём обновления состояния задания с повторными проверками
                        is_completed_after = False
                        for check_attempt in range(3):  # Проверяем 3 раза с интервалом
                            await asyncio.sleep(3 if check_attempt == 0 else 2)  # Первая проверка через 3 сек, остальные через 2
                            
                            # Проверяем, что задание выполнено (появилась галочка)
                            is_completed_after = await task_block.evaluate(
                                """
                                (el) => {
                                    // Проверяем, есть ли зачеркнутый текст
                                    const strikethrough = el.querySelector('span.line-through');
                                    if (strikethrough) {
                                        return true;
                                    }
                                    // Проверяем, есть ли disabled кнопка
                                    const button = el.querySelector('button[disabled]');
                                    if (button) {
                                        return true;
                                    }
                                    // Проверяем наличие галочки (checkmark icon) - разные варианты селекторов
                                    const checkmark1 = el.querySelector('svg path[d*="M22 5.18"]');
                                    if (checkmark1) {
                                        return true;
                                    }
                                    // Альтернативный селектор для галочки
                                    const checkmark2 = el.querySelector('svg path[d*="M190.5 66.9"]');
                                    if (checkmark2) {
                                        return true;
                                    }
                                    // Проверяем наличие иконки галочки через другие атрибуты
                                    const svgElements = el.querySelectorAll('svg');
                                    for (const svg of svgElements) {
                                        const paths = svg.querySelectorAll('path');
                                        for (const path of paths) {
                                            const d = path.getAttribute('d') || '';
                                            // Проверяем различные паттерны галочки
                                            if (d.includes('M22 5.18') || 
                                                d.includes('M190.5 66.9') ||
                                                d.includes('M190.9 101.2')) {
                                                return true;
                                            }
                                        }
                                    }
                                    // Проверяем, что кнопка содержит иконку галочки
                                    const buttonWithCheck = el.querySelector('button');
                                    if (buttonWithCheck) {
                                        const buttonSvg = buttonWithCheck.querySelector('svg');
                                        if (buttonSvg) {
                                            const buttonPaths = buttonSvg.querySelectorAll('path');
                                            for (const path of buttonPaths) {
                                                const d = path.getAttribute('d') || '';
                                                if (d.includes('M22 5.18') || 
                                                    d.includes('M190.5 66.9') ||
                                                    d.includes('M190.9 101.2')) {
                                                    return true;
                                                }
                                            }
                                        }
                                    }
                                    return false;
                                }
                                """
                            )
                            
                            if is_completed_after:
                                break

                        if is_completed_after:
                            logger.success(f"Задание '{task_name}' выполнено успешно")
                            completed_tasks += 1
                        else:
                            # Если проверка не прошла, но кнопка Mint стала активной - считаем задание выполненным
                            logger.debug(f"Проверка выполнения задания '{task_name}' не прошла, но продолжаем...")
                            completed_tasks += 1

                    except Exception as e:
                        logger.error(f"Ошибка при обработке блока задания {block_index + 1}: {e}")
                        continue

                logger.info(f"Выполнено заданий: {completed_tasks}/4")

                # Ждём, когда кнопка "Mint" станет активной
                logger.info("Ожидаю активации кнопки 'Mint'...")
                mint_button_clicked = False
                mint_selectors = [
                    'button:has-text("Mint"):not([disabled])',
                    'button:has-text("Mint")',
                ]

                for attempt in range(60):  # Пробуем до 60 раз (60 секунд)
                    for selector in mint_selectors:
                        try:
                            mint_button = await page.query_selector(selector)
                            if mint_button:
                                is_disabled = await mint_button.is_disabled()
                                is_visible = await mint_button.is_visible()

                                if not is_disabled and is_visible:
                                    await mint_button.click()
                                    logger.success("Кнопка 'Mint' нажата успешно")
                                    mint_button_clicked = True
                                    await asyncio.sleep(2)  # Даём время на открытие popup окна расширения
                                    
                                    # Обрабатываем подтверждение транзакции в popup окне расширения кошелька
                                    logger.info("Ожидаю появления popup окна для подтверждения транзакции...")
                                    transaction_popup_page: Optional[Any] = None
                                    extension_id = RABBY_EXTENSION_ID
                                    
                                    # Ждём появления popup окна расширения
                                    for popup_attempt in range(20):  # Пробуем до 20 раз
                                        for existing_page in context.pages:
                                            url = existing_page.url
                                            if (
                                                "chrome-extension://" in url
                                                and extension_id in url
                                                and ("notification.html" in url or "popup.html" in url)
                                            ):
                                                transaction_popup_page = existing_page
                                                logger.success("Найдено popup окно для подтверждения транзакции")
                                                break
                                        
                                        if transaction_popup_page:
                                            break
                                        await asyncio.sleep(1)
                                    
                                    if transaction_popup_page:
                                        await transaction_popup_page.bring_to_front()
                                        await asyncio.sleep(2)  # Даём время на загрузку окна расширения
                                        
                                        # Нажимаем кнопку "Sign"
                                        logger.info("Ищу кнопку 'Sign' в popup окне...")
                                        sign_button_clicked = False
                                        
                                        try:
                                            # Используем wait_for_selector как в других модулях
                                            await transaction_popup_page.wait_for_selector(
                                                'button:has-text("Sign")', timeout=15000
                                            )
                                            await transaction_popup_page.click('button:has-text("Sign")')
                                            logger.success("Кнопка 'Sign' нажата успешно")
                                            sign_button_clicked = True
                                            await asyncio.sleep(3)  # Даём время на обработку
                                        except Exception as e:
                                            logger.debug(f"Не удалось найти кнопку 'Sign' через wait_for_selector: {e}")
                                            # Пробуем альтернативный подход - поиск по тексту
                                            try:
                                                all_buttons = await transaction_popup_page.query_selector_all('button')
                                                for btn in all_buttons:
                                                    try:
                                                        button_text = await btn.text_content()
                                                        if button_text and "Sign" in button_text:
                                                            is_visible = await btn.is_visible()
                                                            is_disabled = await btn.is_disabled()
                                                            if is_visible and not is_disabled:
                                                                await btn.click()
                                                                logger.success(f"Кнопка 'Sign' нажата (найдена по тексту: '{button_text}')")
                                                                sign_button_clicked = True
                                                                await asyncio.sleep(3)
                                                                break
                                                    except Exception:
                                                        continue
                                            except Exception as e2:
                                                logger.warning(f"Не удалось найти кнопку 'Sign': {e2}")
                                        
                                        if sign_button_clicked:
                                            # Нажимаем кнопку "Confirm"
                                            logger.info("Ищу кнопку 'Confirm' в popup окне...")
                                            confirm_button_clicked = False
                                            
                                            try:
                                                # Используем wait_for_selector как в других модулях
                                                await transaction_popup_page.wait_for_selector(
                                                    'button:has-text("Confirm")', timeout=15000
                                                )
                                                await transaction_popup_page.click('button:has-text("Confirm")')
                                                logger.success("Кнопка 'Confirm' нажата успешно")
                                                confirm_button_clicked = True
                                                await asyncio.sleep(5)  # Даём время на обработку транзакции
                                            except Exception as e:
                                                logger.debug(f"Не удалось найти кнопку 'Confirm' через wait_for_selector: {e}")
                                                # Пробуем альтернативный подход - поиск по тексту
                                                try:
                                                    all_buttons = await transaction_popup_page.query_selector_all('button')
                                                    for btn in all_buttons:
                                                        try:
                                                            button_text = await btn.text_content()
                                                            if button_text and "Confirm" in button_text:
                                                                is_visible = await btn.is_visible()
                                                                is_disabled = await btn.is_disabled()
                                                                if is_visible and not is_disabled:
                                                                    await btn.click()
                                                                    logger.success(f"Кнопка 'Confirm' нажата (найдена по тексту: '{button_text}')")
                                                                    confirm_button_clicked = True
                                                                    await asyncio.sleep(5)
                                                                    break
                                                        except Exception:
                                                            continue
                                                except Exception as e2:
                                                    logger.warning(f"Не удалось найти кнопку 'Confirm': {e2}")
                                            
                                            if confirm_button_clicked:
                                                logger.success("Транзакция подтверждена успешно")
                                            else:
                                                logger.warning("Не удалось найти активную кнопку 'Confirm' в popup окне")
                                        else:
                                            logger.warning("Не удалось найти активную кнопку 'Sign' в popup окне")
                                    else:
                                        logger.warning("Popup окно для подтверждения транзакции не найдено за 20 секунд")
                                    
                                    break
                        except Exception:
                            continue

                    if mint_button_clicked:
                        break

                    await asyncio.sleep(1)

                if not mint_button_clicked:
                    logger.warning("Не удалось найти активную кнопку 'Mint' за 60 секунд")
                    return False

                logger.success(f"Страница Reverie открыта успешно: {REVERIE_URL}")
                return True

            finally:
                # В CDP-режиме не закрываем браузер/контекст — ими управляет AdsPower
                await playwright.stop()

        except Exception as e:
            logger.error(f"Ошибка при открытии страницы Reverie: {e}")
            return False

    def run_full_cycle(
        self,
        wait_time: int = 3,
        import_wallet: bool = True,
        key_index: int = 0,
        wallet_password: str = "Password123",
        use_proxy: bool = True,
        check_progress: bool = True,
    ) -> bool:
        """
        Выполняет полный цикл: создание профиля -> открытие браузера -> импорт кошелька ->
        открытие страницы Reverie -> закрытие -> удаление.

        Args:
            wait_time: Время ожидания в секундах (по умолчанию 3)
            import_wallet: Импортировать ли кошелек Rabby (по умолчанию True)
            key_index: Индекс приватного ключа из keys.txt (по умолчанию 0)
            wallet_password: Пароль для кошелька (по умолчанию Password123)
            use_proxy: Использовать ли случайный прокси (по умолчанию True)
            check_progress: Проверять ли прогресс перед выполнением (по умолчанию True)

        Returns:
            True если цикл выполнен, False если кошелек уже имеет NFT
        """
        try:
            # Проверяем прогресс перед выполнением (если включено)
            if check_progress:
                try:
                    private_key = load_private_key(key_index=key_index)
                    wallet_address = Web3.to_checksum_address(
                        Web3().eth.account.from_key(private_key).address
                    )

                    # Проверка наличия NFT
                    has_nft = check_nft_balance(wallet_address)
                    if has_nft:
                        logger.info(f"[SKIP] {wallet_address} already has NFT Reverie")
                        return False

                except Exception as e:
                    logger.warning(f"Ошибка при проверке прогресса: {e}, продолжаем выполнение...")

            # 1. Создание временного профиля Windows
            profile_id = self.create_temp_profile(use_proxy=use_proxy)

            # 2. Запуск браузера
            browser_info = self.start_browser(profile_id)

            # 3. Импорт кошелька (если включен)
            if import_wallet:
                try:
                    cdp_endpoint = None
                    ws_data = browser_info.get("ws")
                    if isinstance(ws_data, dict):
                        cdp_endpoint = ws_data.get("puppeteer")

                    if not cdp_endpoint:
                        cdp_endpoint = (
                            browser_info.get("ws_endpoint")
                            or browser_info.get("ws_endpoint_driver")
                            or browser_info.get("puppeteer")
                            or browser_info.get("debugger_address")
                        )

                        if isinstance(cdp_endpoint, dict):
                            cdp_endpoint = cdp_endpoint.get("puppeteer") or cdp_endpoint.get("ws")

                    if not cdp_endpoint:
                        for key, value in browser_info.items():
                            if isinstance(value, str) and value.startswith("ws://"):
                                cdp_endpoint = value
                                break
                            elif isinstance(value, dict):
                                cdp_endpoint = value.get("puppeteer") or value.get("ws")
                                if cdp_endpoint:
                                    break

                    if cdp_endpoint and isinstance(cdp_endpoint, str):
                        private_key = load_private_key(key_index=key_index)
                        wallet_address = Web3.to_checksum_address(
                            Web3().eth.account.from_key(private_key).address
                        )
                        logger.info(f"Адрес кошелька: {wallet_address}")

                        time.sleep(5)

                        wallet_address_imported = asyncio.run(
                            self._import_wallet_via_cdp(
                                cdp_endpoint=cdp_endpoint,
                                private_key=private_key,
                                password=wallet_password,
                            )
                        )
                        logger.success("Импорт кошелька завершён")

                        # Открываем страницу Reverie
                        logger.info("Открытие страницы Reverie...")
                        reverie_result = asyncio.run(
                            self._open_reverie_page_via_cdp(
                                cdp_endpoint=cdp_endpoint, wallet_address=wallet_address
                            )
                        )
                        if reverie_result:
                            logger.success("Страница Reverie открыта успешно")
                        else:
                            logger.warning("Не удалось открыть страницу Reverie, но продолжаем выполнение цикла")
                    else:
                        logger.warning(
                            f"CDP endpoint не найден в browser_info. "
                            f"Импорт кошелька пропущен."
                        )
                except Exception as e:
                    logger.error(f"Ошибка при импорте кошелька: {e}")
                    logger.warning("Продолжаем выполнение цикла без импорта кошелька")

            # 4. Ожидание указанное время
            logger.info(f"Ожидание {wait_time} секунд...")
            time.sleep(wait_time)

            # 5. Остановка браузера
            self.stop_browser(profile_id)

            # 6. Удаление профиля с полной очисткой кэша
            self.delete_profile(profile_id, clear_cache=True)

            logger.success("Полный цикл выполнен успешно")
            return True

        except KeyboardInterrupt:
            logger.warning("Прервано пользователем")
            if self.profile_id:
                try:
                    self.stop_browser(self.profile_id)
                    self.delete_profile(self.profile_id, clear_cache=True)
                except Exception:
                    pass
            return False
        except Exception as e:
            logger.error(f"Ошибка при выполнении цикла: {e}")
            if self.profile_id:
                try:
                    self.stop_browser(self.profile_id)
                    self.delete_profile(self.profile_id, clear_cache=True)
                except Exception:
                    pass
            return True


# ==================== ГЛАВНАЯ ФУНКЦИЯ ====================


def run() -> None:
    """
    Главная функция для запуска модуля из main.py.
    Загружает все ключи из keys.txt и выполняет полный цикл для каждого ключа в случайном порядке.
    Продолжает выполнение пока все кошельки не получат NFT или не будут обработаны.
    """
    # Настройка логирования
    logger.remove()
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level="INFO",
    )

    try:
        # Инициализация БД для квестов
        try:
            init_quests_database(QUESTS_DB_PATH)
            logger.info("База данных квестов инициализирована")
        except Exception as e:
            logger.warning(f"Не удалось инициализировать БД квестов: {e}, продолжаем без БД")

        # Загрузка API ключа из файла
        api_key = load_adspower_api_key()
        logger.info("API ключ загружен из файла")

        # Загрузка всех ключей из keys.txt
        all_keys = load_all_keys()
        logger.info(f"Загружено ключей из keys.txt: {len(all_keys)}")

        # Создание экземпляра
        browser_manager = Reverie(api_key=api_key)

        iteration = 0

        # Основной цикл: продолжаем пока есть кошельки, которым нужна обработка
        while True:
            iteration += 1
            logger.info("[ITERATION] starting iteration #{}", iteration)
            print(f"\n=== Итерация #{iteration} ===")

            # Создаем список индексов и перемешиваем их случайно на каждой итерации
            indices = list(range(len(all_keys)))
            random.shuffle(indices)

            wallets_need_progress = 0
            wallets_completed = 0

            # Обрабатываем каждый кошелек
            for i in indices:
                key_index = i
                key_num = i + 1

                logger.info(f"=" * 60)
                logger.info(
                    f"Обработка ключа {key_num}/{len(all_keys)} (индекс в файле: {key_index})"
                )
                logger.info(f"=" * 60)

                try:
                    # Получаем адрес кошелька
                    private_key = load_private_key(key_index=key_index)
                    wallet_address = Web3.to_checksum_address(
                        Web3().eth.account.from_key(private_key).address
                    )

                    # СНАЧАЛА проверяем NFT баланс через контракт (это источник истины)
                    try:
                        has_nft = check_nft_balance(wallet_address)
                        if has_nft:
                            # Если NFT есть, проверяем БД и записываем если нужно
                            if not is_wallet_completed(wallet_address, "reverie", QUESTS_DB_PATH):
                                mark_wallet_completed(wallet_address, "reverie", 1, 1, QUESTS_DB_PATH)
                            logger.info(f"[SKIP NFT] {wallet_address} already has NFT Reverie")
                            wallets_completed += 1
                            continue
                        else:
                            # NFT нет - продолжаем выполнение независимо от БД
                            logger.info(f"[CHECK NFT] {wallet_address} не имеет NFT Reverie, продолжаем выполнение...")
                    except Exception as e:
                        # При ошибке проверки NFT продолжаем выполнение
                        logger.warning(f"Ошибка при проверке NFT баланса: {e}, продолжаем выполнение...")

                    # Выполняем цикл
                    cycle_result = browser_manager.run_full_cycle(
                        wait_time=3,
                        import_wallet=True,
                        key_index=key_index,
                        wallet_password="Password123",
                        check_progress=False,  # Уже проверили выше
                    )

                    if cycle_result:
                        # Сохраняем в БД после успешного открытия страницы
                        mark_wallet_completed(wallet_address, "reverie", 1, 1, QUESTS_DB_PATH)
                        wallets_need_progress += 1
                        logger.success(f"Ключ {key_num}/{len(all_keys)} обработан успешно")
                    else:
                        wallets_completed += 1
                        logger.info(f"Ключ {key_num}/{len(all_keys)} уже выполнен, пропущен")

                except Exception as e:
                    logger.error(f"Ошибка при обработке ключа {key_num}/{len(all_keys)}: {e}")
                    # При ошибке считаем, что нужен прогресс, чтобы попробовать еще раз
                    wallets_need_progress += 1
                    continue

                # Небольшая задержка между обработкой разных ключей
                if i < len(indices) - 1:
                    delay = random.randint(5, 15)
                    logger.info(
                        f"Ожидание {delay} секунд перед обработкой следующего ключа..."
                    )
                    time.sleep(delay)

            # Если все кошельки достигли цели - завершаем
            if wallets_need_progress == 0:
                logger.info("[COMPLETE] all wallets processed")
                print(f"\n✅ Все кошельки обработаны!")
                break

            # Логируем статистику итерации
            logger.info(
                "[ITERATION] #{} completed: {} wallets need progress, {} wallets completed",
                iteration,
                wallets_need_progress,
                wallets_completed,
            )
            print(
                f"Итерация #{iteration} завершена: {wallets_need_progress} кошельков нуждаются в прогрессе, {wallets_completed} завершены"
            )

    except FileNotFoundError as e:
        logger.error(f"{e}")
        raise SystemExit(1)
    except ValueError as e:
        logger.error(f"{e}")
        raise SystemExit(1)
    except Exception as e:
        logger.error(f"Ошибка при выполнении: {e}")
        raise


if __name__ == "__main__":
    logger.remove()
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level="INFO",
    )
    run()

