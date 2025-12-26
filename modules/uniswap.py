#!/usr/bin/env python3
from __future__ import annotations

import random
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import requests
from loguru import logger
from web3 import Web3

# Позволяет запускать файл напрямую: `python modules/uniswap.py`
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if __name__ == "__main__":
    root_s = str(PROJECT_ROOT)
    if root_s not in sys.path:
        sys.path.insert(0, root_s)

# Импорт функций для работы с БД
try:
    from modules.db_utils import (
        init_quests_database,
        is_wallet_completed,
        mark_wallet_completed,
        QUESTS_DB_PATH,
    )
except ImportError:
    # Fallback если модуль не найден
    def init_quests_database(*args, **kwargs):
        pass

    def is_wallet_completed(*args, **kwargs):
        return False

    def mark_wallet_completed(*args, **kwargs):
        pass

    QUESTS_DB_PATH = PROJECT_ROOT / "quests.db"


# ==================== КОНФИГУРАЦИЯ ====================
# Конфиг RPC для Soneium
RPC_URL_DEFAULT = "https://soneium-rpc.publicnode.com"
CHAIN_ID = 1868

# Адреса контрактов Uniswap v4 на Soneium
POOL_MANAGER_ADDRESS = "0x360e68faccca8ca495c1b759fd9eee466db9fb32"
QUOTER_ADDRESS = "0x3972c00f7ed4885e145823eb7c655375d275a1c5"
UNIVERSAL_ROUTER_ADDRESS = "0x0e2850543f69f678257266e0907ff9a58b3f13de"

# Адрес USDCE на Soneium
USDCE_ADDRESS = "0xbA9986D2381edf1DA03B0B9c1f8b00dc4AacC369"

# NATIVE ETH адрес (используется в v4 для нативного ETH)
NATIVE_ETH_ADDRESS = "0x0000000000000000000000000000000000000000"

# Параметры пула (из реальной транзакции)
FEE_TIER = 500  # 0.05%
TICK_SPACING = 10

# ABI для Quoter (реальный ABI с Soneium Blockscout)
QUOTER_ABI = [
    {
        "inputs": [
            {
                "components": [
                    {
                        "components": [
                            {"internalType": "address", "name": "currency0", "type": "address"},
                            {"internalType": "address", "name": "currency1", "type": "address"},
                            {"internalType": "uint24", "name": "fee", "type": "uint24"},
                            {"internalType": "int24", "name": "tickSpacing", "type": "int24"},
                            {"internalType": "contract IHooks", "name": "hooks", "type": "address"},
                        ],
                        "internalType": "struct PoolKey",
                        "name": "poolKey",
                        "type": "tuple",
                    },
                    {"internalType": "bool", "name": "zeroForOne", "type": "bool"},
                    {"internalType": "uint128", "name": "exactAmount", "type": "uint128"},
                    {"internalType": "bytes", "name": "hookData", "type": "bytes"},
                ],
                "internalType": "struct IV4Quoter.QuoteExactSingleParams",
                "name": "params",
                "type": "tuple",
            }
        ],
        "name": "quoteExactInputSingle",
        "outputs": [
            {"internalType": "uint256", "name": "amountOut", "type": "uint256"},
            {"internalType": "uint256", "name": "gasEstimate", "type": "uint256"},
        ],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]

# ABI для Universal Router (упрощенный, только execute)
UNIVERSAL_ROUTER_ABI = [
    {
        "inputs": [
            {"internalType": "bytes", "name": "commands", "type": "bytes"},
            {"internalType": "bytes[]", "name": "inputs", "type": "bytes[]"},
            {"internalType": "uint256", "name": "deadline", "type": "uint256"},
        ],
        "name": "execute",
        "outputs": [],
        "stateMutability": "payable",
        "type": "function",
    }
]

# === Конфиг Portal API ===
PORTAL_PROFILE_URL = "https://portal.soneium.org/api/profile/bonus-dapp"
PROXY_FILE = PROJECT_ROOT / "proxy.txt"


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


@dataclass(frozen=True)
class ProxyEntry:
    host: str
    port: int
    username: str
    password: str

    @property
    def http_url(self) -> str:
        # Прокси в формате http://user:pass@host:port
        user = self.username.replace("@", "%40")
        pwd = self.password.replace("@", "%40")
        return f"http://{user}:{pwd}@{self.host}:{self.port}"

    @property
    def safe_label(self) -> str:
        return f"{self.host}:{self.port}"


def _parse_proxy_line(line: str) -> ProxyEntry | None:
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    parts = line.split(":", 3)
    if len(parts) != 4:
        return None
    host, port_s, username, password = [p.strip() for p in parts]
    if not host or not port_s:
        return None
    try:
        port = int(port_s)
    except ValueError:
        return None
    return ProxyEntry(host=host, port=port, username=username, password=password)


def load_proxies() -> list[ProxyEntry]:
    """Загружает прокси из файла proxy.txt"""
    if not PROXY_FILE.exists():
        return []
    proxies: list[ProxyEntry] = []
    for raw in PROXY_FILE.read_text(encoding="utf-8", errors="ignore").splitlines():
        p = _parse_proxy_line(raw)
        if p:
            proxies.append(p)
    return proxies


def _fetch_portal_bonus_profile(address: str, max_attempts: int = 30) -> list[dict[str, Any]]:
    """
    Берём СЛУЧАЙНЫЙ прокси из proxy.txt и запрашиваем:
      GET https://portal.soneium.org/api/profile/bonus-dapp?address=0x...
    """
    proxies_all = load_proxies()
    session = requests.Session()

    last_err: Exception | None = None

    attempts = max(1, int(max_attempts))
    # Если прокси есть — будем постоянно ротировать, НЕ используя прямое соединение
    pool: list[ProxyEntry] = proxies_all[:]
    random.shuffle(pool)

    for attempt in range(1, attempts + 1):
        p: Optional[ProxyEntry]
        proxies_cfg: Optional[dict[str, str]]

        if proxies_all:
            if not pool:
                pool = proxies_all[:]
                random.shuffle(pool)
            p = pool.pop()  # гарантированно другой, пока не исчерпаем пул
            proxies_cfg = {"http": p.http_url, "https": p.http_url}
        else:
            # если proxy.txt пуст — работаем без прокси
            p = None
            proxies_cfg = None

        try:
            r = session.get(
                PORTAL_PROFILE_URL,
                params={"address": address},
                timeout=30,
                proxies=proxies_cfg,
                headers={
                    "accept": "application/json",
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                },
            )

            # Иногда возможен rate limit / временные ошибки
            if r.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"portal http {r.status_code}")

            r.raise_for_status()
            data = r.json()
            if not isinstance(data, list):
                raise RuntimeError(f"Неожиданный формат ответа portal: {type(data)}")
            return data
        except Exception as e:
            last_err = e
            logger.info(
                "[PORTAL] attempt {}/{} proxy={} err={}",
                attempt,
                attempts,
                (p.safe_label if p else "none"),
                e,
            )
            # небольшой джиттер перед повтором
            time.sleep(random.uniform(0.4, 1.2))

    raise RuntimeError(f"Portal недоступен после {attempts} попыток (прокси ротировались): {last_err}")


def _extract_uniswap_progress(profile: list[dict[str, Any]]) -> tuple[int, int]:
    """
    Возвращает (completed, required) для квеста Uniswap.
    Ищем объект с id вида uniswap или uniswap_* (например, uniswap_5).
    """
    candidates: list[dict[str, Any]] = []
    for item in profile:
        if not isinstance(item, dict):
            continue
        item_id = str(item.get("id", "")).lower()
        if item_id == "uniswap" or item_id.startswith("uniswap_"):
            candidates.append(item)

    if not candidates:
        raise RuntimeError("В ответе portal не найден квест uniswap или uniswap_*")

    # Сортируем по week (самый новый первым)
    candidates.sort(key=lambda x: int(x.get("week", 0) or 0), reverse=True)
    uniswap = candidates[0]
    quests = uniswap.get("quests") or []
    if not isinstance(quests, list) or not quests:
        raise RuntimeError("В uniswap* отсутствует quests[]")

    req = 0
    comp = 0
    for q in quests:
        if not isinstance(q, dict):
            continue
        if str(q.get("unit", "")).lower() != "txs":
            continue
        req = max(req, int(q.get("required", 0) or 0))
        comp = max(comp, int(q.get("completed", 0) or 0))

    if req <= 0:
        q0 = quests[0] if isinstance(quests[0], dict) else {}
        req = int(q0.get("required", 0) or 0)
        comp = int(q0.get("completed", 0) or 0)

    return comp, req


def get_eth_balance(address: str, rpc_url: str = RPC_URL_DEFAULT) -> float:
    """
    Получает баланс ETH на кошельке в ETH (не в Wei).
    
    Args:
        address: Адрес кошелька (checksum format)
        rpc_url: URL RPC ноды (по умолчанию Soneium RPC)
    
    Returns:
        Баланс в ETH как float
    """
    try:
        w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 30}))
        
        if not w3.is_connected():
            raise RuntimeError("RPC недоступен (w3.is_connected() == False)")
        
        # Получаем баланс в Wei
        balance_wei = w3.eth.get_balance(Web3.to_checksum_address(address))
        
        # Конвертируем в ETH
        balance_eth = float(Web3.from_wei(balance_wei, "ether"))
        
        return balance_eth
    except Exception as e:
        logger.error(f"Ошибка при получении баланса ETH для {address}: {e}")
        raise


def calculate_swap_amount(balance_eth: float, min_percent: float = 1.0, max_percent: float = 3.0) -> float:
    """
    Вычисляет случайную сумму для swap от min_percent до max_percent от баланса.
    
    Args:
        balance_eth: Баланс в ETH
        min_percent: Минимальный процент (по умолчанию 1.0)
        max_percent: Максимальный процент (по умолчанию 3.0)
    
    Returns:
        Случайная сумма в ETH
    """
    if balance_eth <= 0:
        raise ValueError(f"Баланс должен быть больше 0, получен: {balance_eth}")
    
    # Вычисляем случайный процент от min_percent до max_percent
    percent = random.uniform(min_percent, max_percent)
    
    # Вычисляем сумму
    amount = balance_eth * (percent / 100.0)
    
    return amount


def format_eth_amount(amount: float, max_decimals: int = 18) -> str:
    """
    Форматирует сумму ETH в обычный десятичный формат (без научной нотации).
    
    Args:
        amount: Сумма в ETH
        max_decimals: Максимальное количество знаков после запятой (по умолчанию 18)
    
    Returns:
        Отформатированная строка без научной нотации и лишних нулей
    """
    # Форматируем с достаточным количеством знаков, затем убираем лишние нули
    formatted = f"{amount:.{max_decimals}f}".rstrip('0').rstrip('.')
    return formatted


def encode_v4_swap_command(
    w3: Web3,
    token_in: str,
    token_out: str,
    amount_in_wei: int,
    recipient: str,
    fee: int,
    tick_spacing: int,
    hooks: str = "0x0000000000000000000000000000000000000000",
) -> tuple[bytes, bytes]:
    """
    Кодирует команду V4_SWAP для Universal Router.

    Формат: команда = 0x10 (V4_SWAP)
    inputs = abi.encode(bytes actions, bytes[] params)

    Actions:
    - 0x06: SWAP_EXACT_IN_SINGLE
    - 0x0b: SETTLE (закрыть дельту входного токена)
    - 0x0e: TAKE (получить выходной токен)

        Args:
        w3: Web3 экземпляр для кодирования
        token_in: Адрес входного токена (NATIVE_ETH_ADDRESS для ETH)
        token_out: Адрес выходного токена
        amount_in_wei: Сумма для swap в Wei
        recipient: Адрес получателя
        fee: Fee tier
        tick_spacing: Tick spacing
        hooks: Адрес hooks контракта
        
        Returns:
        Кортеж (command_bytes, input_bytes)
    """
    # Команда V4_SWAP = 0x10
    command = bytes([0x10])

    # Формируем параметры
    currency0 = Web3.to_checksum_address(token_in)
    currency1 = Web3.to_checksum_address(token_out)
    hooks_addr = Web3.to_checksum_address(hooks)
    recipient_addr = Web3.to_checksum_address(recipient)

    try:
        from eth_abi import encode as abi_encode

        # Actions: SWAP_EXACT_IN_SINGLE (0x06), SETTLE (0x0b), TAKE (0x0e)
        actions = bytes([0x06, 0x0b, 0x0e])

        # Параметры для SWAP_EXACT_IN_SINGLE (0x06)
        zero_for_one = True  # currency0 -> currency1
        amount_out_minimum = 0  # Минимальная сумма выхода (0 для теста)
        hook_data = b""

        # Кодируем PoolKey inline (5 полей по 32 байта каждый)
        pool_key_data = (
            abi_encode(["address"], [currency0]) +
            abi_encode(["address"], [currency1]) +
            abi_encode(["uint24"], [fee]) +
            abi_encode(["int24"], [tick_spacing]) +
            abi_encode(["address"], [hooks_addr])
        )

        # Кодируем ExactInputSingleParams
        hook_data_offset = 5 * 32 + 32 + 32 + 32  # PoolKey (5*32) + zeroForOne + amountIn + amountOutMinimum

        swap_params_encoded = (
            pool_key_data +  # PoolKey inline
            abi_encode(["bool"], [zero_for_one]) +
            abi_encode(["uint128"], [amount_in_wei]) +
            abi_encode(["uint128"], [amount_out_minimum]) +
            abi_encode(["uint256"], [hook_data_offset]) +  # offset к hookData
            abi_encode(["uint256"], [len(hook_data)]) +  # длина hookData
            hook_data
        )

        # Параметры для SETTLE (0x0b): (Currency currency, uint256 amount, bool payerIsUser)
        settle_params_encoded = abi_encode(
            ["address", "uint256", "bool"],
            [currency0, amount_in_wei, True]  # payerIsUser = True (платит пользователь)
        )

        # Параметры для TAKE (0x0e): (Currency currency, address recipient, uint256 amount)
        take_params_encoded = abi_encode(
            ["address", "address", "uint256"],
            [currency1, recipient_addr, 0]  # amount = 0 означает OPEN_DELTA (всю доступную сумму)
        )

        # Кодируем actions и params как (bytes, bytes[])
        params_array = [swap_params_encoded, settle_params_encoded, take_params_encoded]

        # Используем web3.codec для правильного ABI кодирования
        try:
            # Кодируем (bytes, bytes[]) используя web3 codec
            input_bytes = w3.codec.encode(
                ["bytes", "bytes[]"],
                [actions, params_array]
            )
        except Exception as encode_error:
            # Если web3.codec не работает, используем ручное кодирование
            logger.warning(f"web3.codec.encode не сработал: {encode_error}, используем ручное кодирование")

            # Ручное кодирование согласно строгому формату decodeActionsRouterParams
            actions_padded_len = ((len(actions) + 31) // 32) * 32
            actions_padded = actions + b'\x00' * (actions_padded_len - len(actions))

            # params_offset = 0x60 + actions_padded_len
            params_offset = 0x60 + actions_padded_len

            # Вычисляем offsets к данным каждого param (относительно начала params блока)
            param_offsets = []
            tail_offset = len(params_array) * 32  # tailOffset = params.length * 32
            current_offset = tail_offset

            for param in params_array:
                param_offsets.append(current_offset)
                param_padded_len = ((len(param) + 31) // 32) * 32
                param_total_len = 32 + 32 + param_padded_len  # offset + длина + данные
                current_offset += param_total_len

            input_bytes = (
                abi_encode(["uint256"], [0x40]) +  # 0x00: offset к actions = 0x40
                abi_encode(["uint256"], [params_offset]) +  # 0x20: offset к params
                abi_encode(["uint256"], [len(actions)]) +  # 0x40: actions.length
                actions_padded +  # 0x60: actions (выровнено)
                abi_encode(["uint256"], [len(params_array)]) +  # params.length
                b''.join(abi_encode(["uint256"], [offset]) for offset in param_offsets) +  # offsets к данным каждого param
                b''.join(
                    abi_encode(["uint256"], [32]) +  # offset к данным param (всегда 32)
                    abi_encode(["uint256"], [len(param)]) +  # длина param
                    param + b'\x00' * (((len(param) + 31) // 32) * 32 - len(param))  # данные param (выровнено)
                    for param in params_array
                )
            )

    except Exception as e:
        logger.error(f"Ошибка при кодировании: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise RuntimeError(f"Не удалось закодировать параметры swap: {e}")

    return command, input_bytes


def simulate_v4_swap(
    w3: Web3,
    quoter_address: str,
    token_in: str,
    token_out: str,
    amount_in_wei: int,
    fee: int,
    tick_spacing: int,
    hooks: str = "0x0000000000000000000000000000000000000000",
) -> Optional[dict]:
    """
    Симулирует swap через Quoter v4 без реальной транзакции.

        Args:
        w3: Web3 экземпляр
        quoter_address: Адрес контракта Quoter
        token_in: Адрес входного токена (NATIVE_ETH_ADDRESS для ETH)
        token_out: Адрес выходного токена
        amount_in_wei: Сумма для swap в Wei
        fee: Fee tier (500, 3000, 10000)
        tick_spacing: Tick spacing для пула
        hooks: Адрес hooks контракта (по умолчанию 0x0)
        
        Returns:
        Словарь с результатами или None при ошибке
    """
    token_in_checksum = Web3.to_checksum_address(token_in)
    token_out_checksum = Web3.to_checksum_address(token_out)
    quoter_address_checksum = Web3.to_checksum_address(quoter_address)
    hooks_checksum = Web3.to_checksum_address(hooks)

    try:
        quoter = w3.eth.contract(
            address=quoter_address_checksum,
            abi=QUOTER_ABI
        )

        # Формируем PoolKey как вложенный tuple
        pool_key_tuple = (
            token_in_checksum,
            token_out_checksum,
            fee,
            tick_spacing,
            hooks_checksum,
        )

        # Формируем QuoteExactSingleParams как tuple с вложенным PoolKey
        params_tuple = (
            pool_key_tuple,
            True,  # zeroForOne = True (обмениваем currency0 на currency1)
            amount_in_wei,  # uint128
            b"",  # hookData - пустые байты
        )

        result = quoter.functions.quoteExactInputSingle(params_tuple).call()

        # Реальный ABI возвращает (amountOut, gasEstimate)
        amount_out = result[0]
        gas_estimate = result[1]

        # USDCE имеет 6 decimals
        usdce_decimals = 6
        amount_out_formatted = float(amount_out) / (10 ** usdce_decimals)

        return {
            "amount_out": amount_out,
            "amount_out_formatted": amount_out_formatted,
            "gas_estimate": gas_estimate,
        }

    except Exception as e:
        logger.error(f"Ошибка при вызове quoteExactInputSingle: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        return None


def execute_v4_swap(
    w3: Web3,
    private_key: str,
    token_in: str,
    token_out: str,
    amount_in_wei: int,
    fee: int,
    tick_spacing: int,
    recipient: Optional[str] = None,
    hooks: str = "0x0000000000000000000000000000000000000000",
) -> Optional[str]:
    """
    Выполняет реальную транзакцию swap через Universal Router.

        Args:
        w3: Web3 экземпляр
        private_key: Приватный ключ для подписания транзакции
        token_in: Адрес входного токена (NATIVE_ETH_ADDRESS для ETH)
        token_out: Адрес выходного токена
        amount_in_wei: Сумма для swap в Wei
        fee: Fee tier
        tick_spacing: Tick spacing
        recipient: Адрес получателя (по умолчанию отправитель)
        hooks: Адрес hooks контракта
        
        Returns:
        Хеш транзакции или None при ошибке
    """
    account = w3.eth.account.from_key(private_key)
    wallet_address = account.address

    if recipient is None:
        recipient = wallet_address

    try:
        # Кодируем команду V4_SWAP
        command, input_bytes = encode_v4_swap_command(
            w3=w3,
            token_in=token_in,
            token_out=token_out,
            amount_in_wei=amount_in_wei,
            recipient=recipient,
            fee=fee,
            tick_spacing=tick_spacing,
            hooks=hooks,
        )

        # Формируем массив inputs (один элемент для одной команды)
        inputs_array = [input_bytes]

        # Deadline: текущее время + 1 час
        deadline = int(time.time()) + 3600

        # Получаем контракт Universal Router
        router = w3.eth.contract(
            address=Web3.to_checksum_address(UNIVERSAL_ROUTER_ADDRESS),
            abi=UNIVERSAL_ROUTER_ABI
        )

        # Получаем nonce
        nonce = w3.eth.get_transaction_count(wallet_address, "pending")

        # Получаем текущие цены газа
        try:
            gas_price = w3.eth.gas_price
            max_fee_per_gas = gas_price
            max_priority_fee_per_gas = gas_price // 10  # 10% от gas_price
        except Exception:
            # Fallback для сетей без EIP-1559
            gas_price = w3.eth.gas_price
            max_fee_per_gas = None
            max_priority_fee_per_gas = None

        # Строим транзакцию
        tx_params = {
            "chainId": CHAIN_ID,
            "from": wallet_address,
            "nonce": nonce,
            "value": amount_in_wei,  # Для NATIVE ETH отправляем value
        }

        if max_fee_per_gas:
            tx_params["maxFeePerGas"] = max_fee_per_gas
            tx_params["maxPriorityFeePerGas"] = max_priority_fee_per_gas
        else:
            tx_params["gasPrice"] = gas_price

        # Оценка газа
        try:
            estimate_params = {k: v for k, v in tx_params.items() if k != "value"}
            estimate_params["value"] = amount_in_wei

            gas_estimate = router.functions.execute(
                command, inputs_array, deadline
            ).estimate_gas(estimate_params)
            tx_params["gas"] = int(gas_estimate * 1.2)  # +20% запас
            logger.info(f"Оценка газа: {gas_estimate}, установлен лимит: {tx_params['gas']}")
        except Exception as e:
            error_msg = str(e)
            logger.warning(f"Не удалось оценить газ: {error_msg}")
            # Используем фиксированное значение с запасом
            tx_params["gas"] = 200000  # Fallback значение (реальная транзакция ~160000)
            logger.warning(f"Используем фиксированный лимит газа: {tx_params['gas']}")

        # Строим транзакцию
        transaction = router.functions.execute(
            command, inputs_array, deadline
        ).build_transaction(tx_params)

        # Подписываем транзакцию
        signed_txn = account.sign_transaction(transaction)

        # Отправляем транзакцию
        tx_hash = w3.eth.send_raw_transaction(signed_txn.rawTransaction)
        tx_hash_hex = tx_hash.hex()

        logger.success(f"✅ Транзакция отправлена: {tx_hash_hex}")
        logger.info(f"Ссылка: https://soneium.blockscout.com/tx/{tx_hash_hex}")

        # Ожидаем подтверждения
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=300)

        if receipt.status == 1:
            logger.success(f"✅ Транзакция подтверждена в блоке: {receipt.blockNumber}")
            return tx_hash_hex
        else:
            logger.error(f"❌ Транзакция не прошла (status: {receipt.status})")
            return None

    except Exception as e:
        logger.error(f"Ошибка при выполнении swap: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        return None


class Uniswap:
    """
    Класс для выполнения swap через Uniswap v4 напрямую через Web3.
    Использует Universal Router для выполнения транзакций.
    """

    def __init__(self, rpc_url: str = RPC_URL_DEFAULT):
        """
        Инициализация класса Uniswap.

        Args:
            rpc_url: URL RPC ноды (по умолчанию Soneium RPC)
        """
        self.rpc_url = rpc_url
        self.w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 30}))

        if not self.w3.is_connected():
            raise RuntimeError("RPC недоступен (w3.is_connected() == False)")

        network = self.w3.eth.chain_id
        if network != CHAIN_ID:
            raise ValueError(f"Неверный Chain ID: ожидается {CHAIN_ID}, получен {network}")

        logger.info(f"Подключено к сети Soneium (Chain ID: {network})")

    def execute_swap(
        self,
        private_key: str,
        swap_amount_eth: float,
        num_swaps: int = 1,
    ) -> int:
        """
        Выполняет указанное количество swap-транзакций.

        Args:
            private_key: Приватный ключ для подписания транзакций
            swap_amount_eth: Сумма для swap в ETH
            num_swaps: Количество swap-транзакций для выполнения (по умолчанию 1)

        Returns:
            Количество успешно выполненных транзакций
        """
        account = self.w3.eth.account.from_key(private_key)
        wallet_address = account.address

        # Конвертируем сумму в Wei
        swap_amount_wei = int(Web3.to_wei(swap_amount_eth, "ether"))

        successful_swaps = 0

        for swap_num in range(1, num_swaps + 1):
            logger.info(f"Выполнение swap-транзакции #{swap_num}/{num_swaps}...")

            # Пауза между транзакциями (кроме первой)
            if swap_num > 1:
                delay = random.uniform(60, 120)  # 1-2 минуты
                delay_minutes = delay / 60
                logger.info(f"Пауза {delay_minutes:.1f} минут ({delay:.0f} секунд) перед следующей транзакцией...")
                time.sleep(delay)

            # Для каждой транзакции пересчитываем сумму на основе текущего баланса
            current_swap_amount_eth = swap_amount_eth
            try:
                # Получаем текущий баланс кошелька
                current_balance = get_eth_balance(wallet_address, self.rpc_url)
                current_balance_formatted = format_eth_amount(current_balance)
                logger.info(f"Текущий баланс ETH для транзакции #{swap_num}: {current_balance_formatted} ETH")

                if current_balance > 0:
                    # Вычисляем новую сумму для swap (1-3% от текущего баланса)
                    current_swap_amount_eth = calculate_swap_amount(current_balance, min_percent=1.0, max_percent=3.0)
                    swap_amount_formatted = format_eth_amount(current_swap_amount_eth)
                    logger.info(f"Вычислена сумма для swap #{swap_num}: {swap_amount_formatted} ETH ({current_swap_amount_eth/current_balance*100:.2f}% от баланса)")
                else:
                    logger.warning(f"Баланс ETH равен 0 для транзакции #{swap_num}, используем исходную сумму")
                    current_swap_amount_eth = swap_amount_eth
            except Exception as e:
                logger.warning(f"Не удалось получить баланс для транзакции #{swap_num}: {e}, используем исходную сумму")
                current_swap_amount_eth = swap_amount_eth

            # Конвертируем текущую сумму в Wei
            current_swap_amount_wei = int(Web3.to_wei(current_swap_amount_eth, "ether"))

            # Сначала делаем симуляцию для проверки
            logger.info(f"Симуляция swap для транзакции #{swap_num}...")
            simulation_result = simulate_v4_swap(
                w3=self.w3,
                quoter_address=QUOTER_ADDRESS,
                token_in=NATIVE_ETH_ADDRESS,
                token_out=USDCE_ADDRESS,
                amount_in_wei=current_swap_amount_wei,
                fee=FEE_TIER,
                tick_spacing=TICK_SPACING,
            )

            if not simulation_result:
                logger.warning(f"Не удалось выполнить симуляцию для транзакции #{swap_num}, пропускаем")
                continue

            logger.info(f"Ожидаемая выходная сумма: {simulation_result['amount_out_formatted']:.6f} USDCE")

            # Выполняем реальную транзакцию
            tx_hash = execute_v4_swap(
                w3=self.w3,
                private_key=private_key,
                token_in=NATIVE_ETH_ADDRESS,
                token_out=USDCE_ADDRESS,
                amount_in_wei=current_swap_amount_wei,
                fee=FEE_TIER,
                tick_spacing=TICK_SPACING,
                recipient=wallet_address,
            )

            if tx_hash:
                successful_swaps += 1
                logger.success(f"Транзакция #{swap_num}/{num_swaps} выполнена успешно: {tx_hash}")
            else:
                logger.warning(f"Транзакция #{swap_num}/{num_swaps} не выполнена")

        return successful_swaps

    def run_full_cycle(
        self,
        key_index: int = 0,
        target_required: int = 20,
        check_progress: bool = True,
    ) -> bool:
        """
        Выполняет полный цикл: проверка прогресса -> выполнение swap-транзакций.

        Args:
            key_index: Индекс приватного ключа из keys.txt (по умолчанию 0)
            target_required: Целевое количество транзакций (по умолчанию 20)
            check_progress: Проверять ли прогресс перед выполнением (по умолчанию True)

        Returns:
            True если цикл выполнен, False если кошелек уже выполнил задание
        """
        try:
            # Проверяем прогресс перед выполнением (если включено)
            if check_progress:
                try:
                    # Загружаем приватный ключ для получения адреса
                    private_key = load_private_key(key_index=key_index)
                    wallet_address = Web3.to_checksum_address(
                        self.w3.eth.account.from_key(private_key).address
                    )
                    
                    # Получаем профиль через Portal API
                    profile = _fetch_portal_bonus_profile(wallet_address)
                    completed, required = _extract_uniswap_progress(profile)
                    
                    target = int(target_required)
                    done = min(int(completed), target)
                    
                    logger.info(f"{wallet_address} Uniswap {done}/{target}")
                    
                    # Если уже достигли цели - пропускаем
                    if done >= target:
                        logger.info(f"[SKIP] address={wallet_address} already {done}/{target}")
                        return False
                except Exception as e:
                    # При ошибке проверки прогресса продолжаем выполнение
                    logger.warning(f"Ошибка при проверке прогресса: {e}, продолжаем выполнение...")

            # Загружаем приватный ключ
            private_key = load_private_key(key_index=key_index)
            wallet_address = Web3.to_checksum_address(
                self.w3.eth.account.from_key(private_key).address
            )
            logger.info(f"Адрес кошелька: {wallet_address}")

            # Получаем баланс ETH и вычисляем сумму для swap
            try:
                balance_eth = get_eth_balance(wallet_address, self.rpc_url)
                balance_eth_formatted = format_eth_amount(balance_eth)
                logger.info(f"Баланс ETH: {balance_eth_formatted} ETH")

                if balance_eth > 0:
                    swap_amount_eth = calculate_swap_amount(balance_eth, min_percent=1.0, max_percent=3.0)
                    swap_amount_formatted = format_eth_amount(swap_amount_eth)
                    logger.info(f"Вычислена сумма для swap: {swap_amount_formatted} ETH ({swap_amount_eth/balance_eth*100:.2f}% от баланса)")
                else:
                    logger.warning("Баланс ETH равен 0, невозможно выполнить swap")
                    return False
            except Exception as e:
                logger.error(f"Не удалось получить баланс или вычислить сумму для swap: {e}")
                return False

            # Генерируем случайное количество swap-транзакций от 1 до 3
            num_swaps = random.randint(1, 3)
            logger.info(f"Будет выполнено {num_swaps} swap-транзакций")

            # Выполняем swap-транзакции
            successful_swaps = self.execute_swap(
                private_key=private_key,
                swap_amount_eth=swap_amount_eth,
                num_swaps=num_swaps,
            )

            if successful_swaps > 0:
                logger.success(f"Выполнено {successful_swaps}/{num_swaps} swap-транзакций")
                return True
            else:
                logger.warning("Не удалось выполнить ни одной swap-транзакции")
                return False

        except KeyboardInterrupt:
            logger.warning("Прервано пользователем")
            return False
        except Exception as e:
            logger.error(f"Ошибка при выполнении цикла: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return True  # При ошибке возвращаем True, чтобы попробовать еще раз в следующей итерации


def run() -> None:
    """
    Главная функция для запуска модуля из main.py.
    Загружает все ключи из keys.txt и выполняет полный цикл для каждого ключа в случайном порядке.
    Продолжает выполнение пока все кошельки не достигнут целевого количества транзакций.
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
            logger.debug("База данных квестов инициализирована")
        except Exception as e:
            logger.warning(f"Не удалось инициализировать БД квестов: {e}, продолжаем без БД")

        # Загрузка всех ключей из keys.txt
        all_keys = load_all_keys()
        logger.info(f"Загружено ключей из keys.txt: {len(all_keys)}")
        
        # Создание экземпляра Uniswap
        browser_manager = Uniswap()
        
        target_required = 20  # Целевое количество транзакций для Uniswap
        iteration = 0
        
        # Основной цикл: продолжаем пока есть кошельки, которым нужны транзакции
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
                logger.info(f"Обработка ключа {key_num}/{len(all_keys)} (индекс в файле: {key_index})")
                logger.info(f"=" * 60)
                
                try:
                    # Получаем адрес кошелька
                    private_key = load_private_key(key_index=key_index)
                    wallet_address = Web3.to_checksum_address(
                        browser_manager.w3.eth.account.from_key(private_key).address
                    )
                    
                    # Проверяем БД перед запросом к Portal API
                    if is_wallet_completed(wallet_address, "uniswap", QUESTS_DB_PATH):
                        logger.info(f"[SKIP DB] {wallet_address} Uniswap уже выполнен")
                        wallets_completed += 1
                        continue
                    
                    # Проверяем прогресс перед выполнением
                    try:
                        profile = _fetch_portal_bonus_profile(wallet_address)
                        completed, _required = _extract_uniswap_progress(profile)
                        
                        # Используем фиксированный target_required = 20
                        target = int(target_required)
                        done = min(int(completed), target)
                        
                        print(f"{wallet_address} Uniswap {done}/{target}")
                        
                        # Если уже достигли цели - сохраняем в БД и пропускаем
                        if done >= target:
                            mark_wallet_completed(wallet_address, "uniswap", done, target, QUESTS_DB_PATH)
                            logger.info(f"[SKIP] address={wallet_address} already {done}/{target}")
                            wallets_completed += 1
                            continue
                    except Exception as e:
                        # При ошибке проверки прогресса продолжаем выполнение
                        logger.warning(f"Ошибка при проверке прогресса: {e}, продолжаем выполнение...")
                    
                    # Выполняем цикл
                    cycle_result = browser_manager.run_full_cycle(
                        key_index=key_index,
                        target_required=target,
                        check_progress=False  # Уже проверили выше
                    )
                    
                    if cycle_result:
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
                    logger.info(f"Ожидание {delay} секунд перед обработкой следующего ключа...")
                    time.sleep(delay)
            
            # Если все кошельки достигли цели - завершаем
            if wallets_need_progress == 0:
                logger.info("[COMPLETE] all wallets reached target")
                print(f"\n✅ Все кошельки достигли цели!")
                break
            
            # Логируем статистику итерации
            logger.info(
                "[ITERATION] #{} completed: {} wallets need progress, {} wallets completed",
                iteration,
                wallets_need_progress,
                wallets_completed,
            )
            print(f"Итерация #{iteration} завершена: {wallets_need_progress} кошельков нуждаются в прогрессе, {wallets_completed} завершены")

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
    # CLI-аргументы убраны по запросу пользователя.
    # Запуск: `python modules/uniswap.py` или через `python main.py`.
    logger.remove()
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level="INFO",
    )
    run()
