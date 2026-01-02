#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import random
import sys
import time
from pathlib import Path
from typing import List, Optional, Tuple

import requests
from eth_account import Account
from loguru import logger
from web3 import Web3
from web3.types import TxReceipt

# Позволяет запускать файл напрямую: `python modules/redbutton_badge.py`
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

# Импорт функций загрузки
from modules.mint4season import load_private_key, load_all_keys

# ==================== КОНФИГУРАЦИЯ ====================
RPC_URL_DEFAULT = "https://soneium-rpc.publicnode.com"
CHAIN_ID = 1868

# Адреса контрактов
MAIN_CONTRACT_ADDRESS = Web3.to_checksum_address("0x39B4a19C687a3b9530EFE28752a81E41FdD398fa")
ITEM_CONTRACT_ADDRESS = Web3.to_checksum_address("0xfa9d64411a6fD7C112BE9D61040a5B4eA0252a8e")
REWARD_POOL_ADDRESS = Web3.to_checksum_address("0xa486534fc0f0fb22aa29a80a0bb18c5c681c02d2")
RBTN_TOKEN_ADDRESS = Web3.to_checksum_address("0xee28813b8292d47c81e8e6f51c1f1358573ed615")
SBT_BADGE_ADDRESS = Web3.to_checksum_address("0x2303aee937195abca91af6929c8ac51693c4c303")

# Целевая стоимость для накопления
TARGET_VALUE_RBTN = 1300

# LI.FI API конфигурация
LI_FI_API_BASE = "https://li.quest/v1"
LI_FI_API_KEY = "aeaa4f26-c3c3-4b71-aad3-50bd82faf815.1e83cb78-2d75-412d-a310-57272fd0e622"
ETH_ADDRESS = "0x0000000000000000000000000000000000000000"

# Индексы режимов
GACHA_NOOB = 0
GACHA_OG = 3

# Стоимость NFT по редкости (в RBTN, где 1 RBTN = 1 ether)
RARITY_VALUES = {
    0: 15,      # Common
    1: 80,      # Rare
    2: 800,     # Epic
    3: 3000,    # Unique
    4: 18000,   # Legendary
    5: 400000   # Degendary
}

# Индекс редкости Unique
RARITY_UNIQUE = 3

# ==================== ABI КОНТРАКТОВ ====================

# ABI для Main контракта
MAIN_ABI = [
    {
        "inputs": [
            {"internalType": "uint8", "name": "_gachaTypeIndex", "type": "uint8"},
            {"internalType": "uint256", "name": "_deadline", "type": "uint256"},
            {"internalType": "bytes", "name": "_permitSig", "type": "bytes"},
        ],
        "name": "drawItem",
        "outputs": [],
        "stateMutability": "payable",
        "type": "function",
    },
    {
        "inputs": [{"internalType": "uint256[]", "name": "_itemIds", "type": "uint256[]"}],
        "name": "sellItemBatch",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "mintSBT",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "name": "managedContracts",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# ABI для Item контракта
ITEM_ABI = [
    {
        "inputs": [{"internalType": "uint256", "name": "tokenId", "type": "uint256"}],
        "name": "tokenInfo",
        "outputs": [
            {"internalType": "uint256", "name": "raritiesIndex", "type": "uint256"},
            {"internalType": "uint256", "name": "partsIndex", "type": "uint256"},
            {"internalType": "uint256", "name": "setNum", "type": "uint256"},
            {"internalType": "bytes32", "name": "typeHash", "type": "bytes32"},
            {"internalType": "string", "name": "typeName", "type": "string"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"internalType": "address", "name": "owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"internalType": "uint256", "name": "tokenId", "type": "uint256"}],
        "name": "ownerOf",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "totalSupply",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"internalType": "address", "name": "", "type": "address"}],
        "name": "hasUniqueMinted",
        "outputs": [{"internalType": "bool", "name": "", "type": "bool"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "uint256", "name": "tokenId", "type": "uint256"},
            {"indexed": True, "internalType": "address", "name": "owner", "type": "address"},
            {"indexed": False, "internalType": "uint256", "name": "rarityIndex", "type": "uint256"},
            {"indexed": False, "internalType": "uint256", "name": "partIndex", "type": "uint256"},
            {"indexed": False, "internalType": "uint256", "name": "setNum", "type": "uint256"},
            {"indexed": False, "internalType": "uint256", "name": "sequenceNumber", "type": "uint256"},
            {"indexed": False, "internalType": "uint256", "name": "randomSeed", "type": "uint256"},
            {"indexed": False, "internalType": "uint256", "name": "score", "type": "uint256"},
        ],
        "name": "Minted",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "address", "name": "from", "type": "address"},
            {"indexed": True, "internalType": "address", "name": "to", "type": "address"},
            {"indexed": True, "internalType": "uint256", "name": "tokenId", "type": "uint256"},
        ],
        "name": "Transfer",
        "type": "event",
    },
]

# ABI для ERC20 Permit (RBTN токен)
ERC20_PERMIT_ABI = [
    {
        "inputs": [
            {"internalType": "address", "name": "owner", "type": "address"},
            {"internalType": "address", "name": "spender", "type": "address"},
            {"internalType": "uint256", "name": "value", "type": "uint256"},
            {"internalType": "uint256", "name": "deadline", "type": "uint256"},
            {"internalType": "uint8", "name": "v", "type": "uint8"},
            {"internalType": "bytes32", "name": "r", "type": "bytes32"},
            {"internalType": "bytes32", "name": "s", "type": "bytes32"},
        ],
        "name": "permit",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [{"internalType": "address", "name": "owner", "type": "address"}],
        "name": "nonces",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "DOMAIN_SEPARATOR",
        "outputs": [{"internalType": "bytes32", "name": "", "type": "bytes32"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"internalType": "address", "name": "account", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"internalType": "address", "name": "spender", "type": "address"},
            {"internalType": "uint256", "name": "value", "type": "uint256"},
        ],
        "name": "approve",
        "outputs": [{"internalType": "bool", "name": "", "type": "bool"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [
            {"internalType": "address", "name": "owner", "type": "address"},
            {"internalType": "address", "name": "spender", "type": "address"},
        ],
        "name": "allowance",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# ABI для SBT контракта (ERC-721)
SBT_ABI = [
    {
        "inputs": [{"internalType": "address", "name": "owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# ==================== ФУНКЦИИ УТИЛИТЫ ====================


def get_contract_addresses(w3: Web3, main_contract) -> Tuple[str, str, str]:
    """Получить адреса Item, RewardPool и SBT контрактов"""
    try:
        item_address = main_contract.functions.managedContracts(1).call()
        reward_pool_address = main_contract.functions.managedContracts(2).call()
        sbt_address = main_contract.functions.managedContracts(3).call()
        return item_address, reward_pool_address, sbt_address
    except Exception as e:
        logger.warning(f"Ошибка при получении адресов через новые индексы: {e}, пробуем старые")
        try:
            item_address = main_contract.functions.managedContracts(0).call()
            reward_pool_address = main_contract.functions.managedContracts(1).call()
            sbt_address = main_contract.functions.managedContracts(2).call()
            return item_address, reward_pool_address, sbt_address
        except Exception as e2:
            logger.error(f"Ошибка при получении адресов: {e2}")
            raise


def get_minted_token_id(w3: Web3, item_contract, receipt: TxReceipt, wallet_address: str) -> Optional[int]:
    """Получить tokenId из события Minted в receipt транзакции"""
    try:
        minted_event = item_contract.events.Minted()
        
        for log in receipt.logs:
            try:
                decoded = minted_event.process_log(log)
                if decoded["args"]["owner"].lower() == wallet_address.lower():
                    token_id = decoded["args"]["tokenId"]
                    return token_id
            except Exception:
                continue
    except Exception as e:
        logger.error(f"Ошибка при получении tokenId из события Minted: {e}")
    
    return None


def get_user_nfts(w3: Web3, item_contract, wallet_address: str) -> List[int]:
    """Получить список всех NFT токенов кошелька через события Minted"""
    nft_ids = []
    try:
        current_block = w3.eth.block_number
        from_block = max(0, current_block - 100000)
        
        minted_event = item_contract.events.Minted()
        events = minted_event.get_logs(
            fromBlock=from_block,
            toBlock="latest",
            argument_filters={"owner": wallet_address}
        )
        
        for event in events:
            token_id = event["args"]["tokenId"]
            try:
                owner = item_contract.functions.ownerOf(token_id).call()
                if owner.lower() == wallet_address.lower():
                    nft_ids.append(token_id)
            except Exception:
                continue
        
        if len(nft_ids) == 0:
            try:
                balance = item_contract.functions.balanceOf(wallet_address).call()
                if balance > 0 and balance < 1000:
                    total_supply = item_contract.functions.totalSupply().call()
                    max_check = min(total_supply, 100000)
                    for token_id in range(1, max_check + 1):
                        try:
                            owner = item_contract.functions.ownerOf(token_id).call()
                            if owner.lower() == wallet_address.lower():
                                nft_ids.append(token_id)
                                if len(nft_ids) >= balance:
                                    break
                        except Exception:
                            continue
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"Ошибка при получении списка NFT: {e}")
    
    return nft_ids


def estimate_gas_cost(w3: Web3) -> int:
    """Оценивает стоимость газа для транзакции"""
    try:
        latest = w3.eth.get_block("latest")
        base_fee = latest.get("baseFeePerGas")
        
        if base_fee is None:
            gas_price = int(w3.eth.gas_price)
            return gas_price * 200000
        
        try:
            priority_fee = int(getattr(w3.eth, "max_priority_fee", 0) or 0)
        except Exception:
            priority_fee = Web3.to_wei(1, "gwei")
        
        if priority_fee <= 0:
            priority_fee = Web3.to_wei(1, "gwei")
        
        max_fee = int(base_fee) * 2 + priority_fee
        return max_fee * 200000
    except Exception:
        return Web3.to_wei(0.0001, "ether")


def send_transaction(w3: Web3, contract, function_call, private_key: str, value: int = 0) -> str:
    """Отправить транзакцию и вернуть хеш"""
    account = w3.eth.account.from_key(private_key)
    address = account.address
    
    nonce = w3.eth.get_transaction_count(address, "pending")
    latest = w3.eth.get_block("latest")
    base_fee = latest.get("baseFeePerGas")
    
    try:
        estimate_params = {
            "from": address,
            "value": value,
        }
        if base_fee is None:
            estimate_params["gasPrice"] = int(w3.eth.gas_price)
        else:
            try:
                priority_fee = int(getattr(w3.eth, "max_priority_fee", 0) or 0)
            except Exception:
                priority_fee = Web3.to_wei(1, "gwei")
            if priority_fee <= 0:
                priority_fee = Web3.to_wei(1, "gwei")
            max_fee = int(base_fee) * 2 + priority_fee
            estimate_params["maxFeePerGas"] = max_fee
            estimate_params["maxPriorityFeePerGas"] = priority_fee
        
        estimated_gas = int(function_call.estimate_gas(estimate_params))
        gas_limit = int(estimated_gas * 1.2) + 10000
    except Exception as e:
        error_msg = str(e)
        if "execution reverted" in error_msg or "0x" in error_msg:
            logger.warning(f"Контракт вернул ошибку при оценке газа: {error_msg[:200]}")
        else:
            logger.warning(f"Не удалось оценить газ: {error_msg[:200]}")
        gas_limit = 650000
    
    if base_fee is None:
        gas_price = int(w3.eth.gas_price)
        tx = {
            "chainId": CHAIN_ID,
            "from": address,
            "nonce": nonce,
            "value": value,
            "gasPrice": gas_price,
            "gas": gas_limit,
        }
    else:
        try:
            priority_fee = int(getattr(w3.eth, "max_priority_fee", 0) or 0)
        except Exception:
            priority_fee = Web3.to_wei(1, "gwei")
        
        if priority_fee <= 0:
            priority_fee = Web3.to_wei(1, "gwei")
        
        max_fee = int(base_fee) * 2 + priority_fee
        
        tx = {
            "chainId": CHAIN_ID,
            "from": address,
            "nonce": nonce,
            "value": value,
            "maxFeePerGas": max_fee,
            "maxPriorityFeePerGas": priority_fee,
            "gas": gas_limit,
        }
    
    call_data = function_call._encode_transaction_data()
    tx["data"] = call_data
    tx["to"] = contract.address
    
    signed_tx = account.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
    
    return tx_hash.hex()


async def wait_for_confirmation(w3: Web3, tx_hash: str, timeout: int = 180) -> Optional[TxReceipt]:
    """Дождаться подтверждения транзакции"""
    try:
        receipt = await asyncio.to_thread(
            w3.eth.wait_for_transaction_receipt,
            tx_hash,
            timeout=timeout
        )
        return receipt
    except Exception as e:
        logger.warning(f"Не удалось дождаться подтверждения {tx_hash}: {e}")
        return None


def create_permit_signature(
    w3: Web3,
    token_contract,
    private_key: str,
    spender: str,
    amount: int,
    deadline: int
) -> bytes:
    """Создать EIP-2612 permit подпись для RBTN токена"""
    from eth_account.messages import encode_typed_data
    
    account = w3.eth.account.from_key(private_key)
    owner = account.address
    
    try:
        nonce = token_contract.functions.nonces(owner).call()
    except Exception as e:
        logger.error(f"Ошибка при получении nonce: {e}")
        raise
    
    domain = {
        "name": "RedButton Token",
        "version": "1",
        "chainId": CHAIN_ID,
        "verifyingContract": RBTN_TOKEN_ADDRESS
    }
    
    message = {
        "owner": owner,
        "spender": spender,
        "value": amount,
        "nonce": nonce,
        "deadline": deadline
    }
    
    structured_msg = {
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"}
            ],
            "Permit": [
                {"name": "owner", "type": "address"},
                {"name": "spender", "type": "address"},
                {"name": "value", "type": "uint256"},
                {"name": "nonce", "type": "uint256"},
                {"name": "deadline", "type": "uint256"}
            ]
        },
        "domain": domain,
        "primaryType": "Permit",
        "message": message
    }
    
    encoded_msg = encode_typed_data(full_message=structured_msg)
    signed_message = Account.sign_message(encoded_msg, private_key)
    permit_sig = bytes(signed_message.signature)
    
    if len(permit_sig) != 65:
        raise ValueError(f"Invalid permit signature length: {len(permit_sig)}, expected 65")
    
    return permit_sig


def mint_noob(w3: Web3, main_contract, private_key: str) -> str:
    """Минт в режиме NOOB"""
    deadline = int(time.time()) + 3600
    permit_sig = b""
    
    fn = main_contract.functions.drawItem(GACHA_NOOB, deadline, permit_sig)
    tx_hash = send_transaction(w3, main_contract, fn, private_key, value=0)
    return tx_hash


def approve_rbtn(
    w3: Web3,
    token_contract,
    spender: str,
    amount: int,
    private_key: str
) -> str:
    """Approve RBTN токенов для spender"""
    fn = token_contract.functions.approve(spender, amount)
    tx_hash = send_transaction(w3, token_contract, fn, private_key, value=0)
    return tx_hash


async def mint_og(
    w3: Web3,
    main_contract,
    token_contract,
    reward_pool_address: str,
    private_key: str
) -> str:
    """Минт в режиме OG за 1300 RBTN с permit подписью"""
    account = w3.eth.account.from_key(private_key)
    wallet_address = account.address
    
    deadline = int(time.time()) + 3600
    amount = Web3.to_wei(TARGET_VALUE_RBTN, "ether")
    
    try:
        balance = token_contract.functions.balanceOf(wallet_address).call()
        if balance < amount:
            raise Exception(f"Недостаточно RBTN: есть {w3.from_wei(balance, 'ether')}, нужно {TARGET_VALUE_RBTN}")
    except Exception as e:
        logger.error(f"Ошибка при проверке баланса: {e}")
        raise
    
    try:
        allowance = token_contract.functions.allowance(wallet_address, reward_pool_address).call()
        if allowance < amount:
            logger.info(f"Allowance недостаточен ({w3.from_wei(allowance, 'ether')} < {TARGET_VALUE_RBTN}), делаем approve...")
            approve_tx = approve_rbtn(w3, token_contract, reward_pool_address, amount, private_key)
            logger.info(f"Транзакция approve: https://soneium.blockscout.com/tx/{approve_tx}")
            receipt = await wait_for_confirmation(w3, approve_tx)
            if receipt and receipt.status == 1:
                logger.success("Approve подтвержден")
                await asyncio.sleep(2)
            else:
                logger.error("Approve провалился")
                raise Exception("Approve провалился")
        else:
            logger.info(f"Allowance достаточен: {w3.from_wei(allowance, 'ether')} RBTN")
    except Exception as e:
        logger.error(f"Ошибка при approve: {e}")
        raise
    
    try:
        permit_sig = create_permit_signature(
            w3, token_contract, private_key, reward_pool_address, amount, deadline
        )
    except Exception as e:
        logger.error(f"Ошибка при создании permit подписи: {e}")
        raise
    
    fn = main_contract.functions.drawItem(GACHA_OG, deadline, permit_sig)
    tx_hash = send_transaction(w3, main_contract, fn, private_key, value=0)
    return tx_hash


def check_unique_minted(
    w3: Web3,
    item_contract,
    receipt: TxReceipt,
    wallet_address: str
) -> bool:
    """Проверить выпала ли Unique NFT после минта OG"""
    minted_event = item_contract.events.Minted()
    
    for log in receipt.logs:
        try:
            decoded = minted_event.process_log(log)
            if decoded["args"]["owner"].lower() == wallet_address.lower():
                rarity_index = decoded["args"]["rarityIndex"]
                if rarity_index == RARITY_UNIQUE:
                    return True
        except Exception:
            continue
    
    try:
        has_unique = item_contract.functions.hasUniqueMinted(wallet_address).call()
        return has_unique
    except Exception:
        return False


async def sell_nfts_batch(w3: Web3, main_contract, private_key: str, token_ids: List[int]) -> None:
    """Продать NFT батчами (макс 50 за раз)"""
    batch_size = 50
    
    for i in range(0, len(token_ids), batch_size):
        batch = token_ids[i:i + batch_size]
        logger.info(f"Продаем батч {i // batch_size + 1}: {len(batch)} NFT")
        
        fn = main_contract.functions.sellItemBatch(batch)
        tx_hash = send_transaction(w3, main_contract, fn, private_key)
        logger.info(f"Транзакция продажи: https://soneium.blockscout.com/tx/{tx_hash}")
        
        receipt = await wait_for_confirmation(w3, tx_hash)
        if receipt and receipt.status == 1:
            logger.success(f"Батч {i // batch_size + 1} продан успешно")
        else:
            logger.error(f"Ошибка при продаже батча {i // batch_size + 1}")


def mint_sbt(w3: Web3, main_contract, private_key: str) -> str:
    """Замнтить SBT бейдж"""
    fn = main_contract.functions.mintSBT()
    tx_hash = send_transaction(w3, main_contract, fn, private_key)
    return tx_hash


def check_sbt_minted(w3: Web3, sbt_contract, wallet_address: str) -> bool:
    """Проверить заминчен ли SBT бейдж (ERC-721)"""
    try:
        balance = sbt_contract.functions.balanceOf(wallet_address).call()
        return balance > 0
    except Exception:
        return False


async def swap_rbtn_to_eth(
    w3: Web3,
    token_contract,
    private_key: str,
    wallet_address: str
) -> Optional[str]:
    """Обменять все RBTN токены на ETH через LI.FI API"""
    try:
        balance = token_contract.functions.balanceOf(wallet_address).call()
        if balance == 0:
            logger.info("Баланс RBTN равен 0, свап не требуется")
            return None
        
        balance_ether = w3.from_wei(balance, "ether")
        logger.info(f"Баланс RBTN для свапа: {balance_ether}")
        
        params = {
            "fromChain": str(CHAIN_ID),
            "toChain": str(CHAIN_ID),
            "fromToken": RBTN_TOKEN_ADDRESS,
            "toToken": ETH_ADDRESS,
            "fromAmount": str(balance),
            "fromAddress": wallet_address,
            "slippage": "0.05",
            "order": "RECOMMENDED"
        }
        
        headers = {
            "x-lifi-api-key": LI_FI_API_KEY,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        logger.info("Получаем котировку от LI.FI...")
        response = requests.get(
            f"{LI_FI_API_BASE}/quote",
            params=params,
            headers=headers,
            timeout=30
        )
        
        if response.status_code != 200:
            logger.error(f"Ошибка LI.FI API: {response.status_code} - {response.text}")
            return None
        
        quote = response.json()
        
        if not quote.get("transactionRequest"):
            logger.error("LI.FI не предоставил данные транзакции")
            return None
        
        tx_request = quote["transactionRequest"]
        
        if not tx_request.get("to") or not tx_request.get("data"):
            logger.error("Котировка от LI.FI содержит некорректные данные транзакции")
            return None
        
        router_address = Web3.to_checksum_address(tx_request["to"])
        allowance = token_contract.functions.allowance(wallet_address, router_address).call()
        
        if allowance < balance:
            logger.info("Даем безлимитный approve для роутера LI.FI...")
            max_approve = (2 ** 256) - 1
            approve_tx_hash = approve_rbtn(w3, token_contract, router_address, max_approve, private_key)
            receipt = await wait_for_confirmation(w3, approve_tx_hash)
            if not receipt or receipt.status != 1:
                logger.error("Approve провалился")
                return None
            await asyncio.sleep(2)
        
        account = w3.eth.account.from_key(private_key)
        nonce = w3.eth.get_transaction_count(account.address, "pending")
        latest = w3.eth.get_block("latest")
        base_fee = latest.get("baseFeePerGas")
        
        tx_params = {
            "from": account.address,
            "to": Web3.to_checksum_address(tx_request["to"]),
            "data": tx_request["data"],
            "nonce": nonce,
            "value": int(tx_request.get("value", "0"), 16) if isinstance(tx_request.get("value"), str) else tx_request.get("value", 0),
            "chainId": CHAIN_ID
        }
        
        if base_fee is None:
            tx_params["gasPrice"] = int(w3.eth.gas_price)
        else:
            try:
                priority_fee = int(getattr(w3.eth, "max_priority_fee", 0) or 0)
            except Exception:
                priority_fee = Web3.to_wei(1, "gwei")
            if priority_fee <= 0:
                priority_fee = Web3.to_wei(1, "gwei")
            max_fee = int(base_fee) * 2 + priority_fee
            tx_params["maxFeePerGas"] = max_fee
            tx_params["maxPriorityFeePerGas"] = priority_fee
        
        try:
            estimated_gas = w3.eth.estimate_gas(tx_params)
            tx_params["gas"] = int(estimated_gas * 1.2) + 10000
        except Exception:
            tx_params["gas"] = int(tx_request.get("gasLimit", "500000"), 16) if isinstance(tx_request.get("gasLimit"), str) else tx_request.get("gasLimit", 500000)
        
        signed_txn = account.sign_transaction(tx_params)
        tx_hash = w3.eth.send_raw_transaction(signed_txn.rawTransaction)
        
        logger.info(f"Транзакция свапа отправлена: https://soneium.blockscout.com/tx/{tx_hash.hex()}")
        
        receipt = await wait_for_confirmation(w3, tx_hash.hex())
        if receipt and receipt.status == 1:
            logger.success(f"✅ RBTN успешно обменян на ETH")
            return tx_hash.hex()
        else:
            logger.error("Свап провалился")
            return None
            
    except Exception as e:
        logger.error(f"Ошибка при свапе RBTN на ETH: {e}")
        return None


async def process_wallet(
    w3: Web3,
    main_contract,
    item_contract,
    sbt_contract,
    token_contract,
    reward_pool_address: str,
    private_key: str
) -> None:
    """Обработать один кошелек до получения SBT"""
    account = w3.eth.account.from_key(private_key)
    wallet_address = Web3.to_checksum_address(account.address)
    
    logger.info(f"=" * 60)
    logger.info(f"Обработка кошелька: {wallet_address}")
    logger.info(f"=" * 60)
    
    # Проверка через БД: если SBT уже заминчен - пропустить
    if is_wallet_completed(wallet_address, "redbutton_badge", QUESTS_DB_PATH):
        logger.info(f"Кошелек {wallet_address}: SBT уже заминчен (проверка БД), пропускаем")
        return
    
    # Проверка через контракт
    if check_sbt_minted(w3, sbt_contract, wallet_address):
        logger.info(f"Кошелек {wallet_address}: SBT уже заминчен (проверка контракта), пропускаем")
        mark_wallet_completed(wallet_address, "redbutton_badge", 1, 1, QUESTS_DB_PATH)
        return
    
    iteration = 0
    
    while True:
        iteration += 1
        logger.info(f"\n--- Итерация {iteration} ---")
        
        logger.info("Проверка баланса RBTN...")
        nft_ids = []
        total_value = 0
        needed_value = TARGET_VALUE_RBTN
        
        try:
            initial_rbtn_balance = token_contract.functions.balanceOf(wallet_address).call()
            initial_rbtn_balance_ether = float(w3.from_wei(initial_rbtn_balance, "ether"))
            logger.info(f"Начальный баланс RBTN: {initial_rbtn_balance_ether:.2f} RBTN")
            
            if initial_rbtn_balance >= Web3.to_wei(TARGET_VALUE_RBTN, "ether"):
                logger.info(f"Баланс RBTN достаточен ({initial_rbtn_balance_ether:.2f} >= {TARGET_VALUE_RBTN}), пропускаем накопление")
            else:
                needed_value = TARGET_VALUE_RBTN - initial_rbtn_balance_ether
                logger.info(f"Нужно накопить NFT на сумму: {needed_value:.2f} RBTN")
                
                logger.info("Шаг 1: Накопление NFT в режиме NOOB...")
                mint_count = 0
                
                while total_value < needed_value:
                    logger.info(f"Минт NOOB #{mint_count + 1}...")
                    tx_hash = mint_noob(w3, main_contract, private_key)
                    logger.info(f"Транзакция: https://soneium.blockscout.com/tx/{tx_hash}")
                    
                    receipt = await wait_for_confirmation(w3, tx_hash)
                    if receipt and receipt.status == 1:
                        logger.success(f"Минт NOOB #{mint_count + 1} подтвержден")
                        mint_count += 1
                    else:
                        logger.error(f"Минт NOOB #{mint_count + 1} провалился")
                        await asyncio.sleep(2)
                        continue
                    
                    token_id = get_minted_token_id(w3, item_contract, receipt, wallet_address)
                    if token_id is None:
                        logger.warning(f"Не удалось получить tokenId из транзакции, пропускаем...")
                        await asyncio.sleep(2)
                        continue
                    
                    try:
                        token_info = item_contract.functions.tokenInfo(token_id).call()
                        rarity_index = token_info[0]
                        value = RARITY_VALUES.get(rarity_index, 0)
                        
                        nft_ids.append(token_id)
                        total_value += value
                        
                        rarity_names = ["Common", "Rare", "Epic", "Unique", "Legendary", "Degendary"]
                        rarity_name = rarity_names[rarity_index] if rarity_index < len(rarity_names) else "Unknown"
                        
                        logger.info(f"Получен NFT #{token_id}: {rarity_name} ({value} RBTN)")
                        logger.info(f"Накоплено: {total_value:.2f}/{needed_value:.2f} RBTN ({len(nft_ids)} NFT)")
                    except Exception as e:
                        logger.error(f"Ошибка при получении информации о токене {token_id}: {e}")
                        await asyncio.sleep(2)
                        continue
                    
                    delay = random.uniform(5, 10)
                    await asyncio.sleep(delay)
        except Exception as e:
            logger.error(f"Ошибка при проверке баланса RBTN: {e}")
            await asyncio.sleep(5)
            continue
        
        if nft_ids:
            logger.info(f"\nШаг 2: Продаем {len(nft_ids)} NFT...")
            await sell_nfts_batch(w3, main_contract, private_key, nft_ids)
            
            logger.info("Ожидание обновления баланса RBTN...")
            await asyncio.sleep(3)
            
            try:
                rbtn_balance = token_contract.functions.balanceOf(wallet_address).call()
                rbtn_balance_ether = float(w3.from_wei(rbtn_balance, "ether"))
                logger.info(f"Баланс RBTN после продажи: {rbtn_balance_ether:.2f} RBTN")
            except Exception as e:
                logger.warning(f"Не удалось проверить баланс после продажи: {e}")
        
        logger.info("\nШаг 3: Проверка баланса RBTN...")
        try:
            rbtn_balance = token_contract.functions.balanceOf(wallet_address).call()
            rbtn_balance_ether = float(w3.from_wei(rbtn_balance, "ether"))
            logger.info(f"Баланс RBTN: {rbtn_balance_ether:.2f} RBTN")
            
            if rbtn_balance < Web3.to_wei(TARGET_VALUE_RBTN, "ether"):
                logger.error(f"Недостаточно RBTN для минта OG. Нужно: {TARGET_VALUE_RBTN}, есть: {rbtn_balance_ether:.2f}")
                await asyncio.sleep(5)
                continue
        except Exception as e:
            logger.error(f"Ошибка при проверке баланса RBTN: {e}")
            await asyncio.sleep(5)
            continue
        
        logger.info("Минт в режиме OG...")
        tx_hash = await mint_og(w3, main_contract, token_contract, reward_pool_address, private_key)
        logger.info(f"Транзакция: https://soneium.blockscout.com/tx/{tx_hash}")
        
        receipt = await wait_for_confirmation(w3, tx_hash)
        if not receipt or receipt.status != 1:
            logger.error("Минт OG провалился, продолжаем цикл...")
            await asyncio.sleep(5)
            continue
        
        logger.success("Минт OG подтвержден")
        
        logger.info("\nШаг 4: Проверка выпадения Unique...")
        is_unique = check_unique_minted(w3, item_contract, receipt, wallet_address)
        
        if is_unique:
            unique_token_id = get_minted_token_id(w3, item_contract, receipt, wallet_address)
            
            logger.success("✅ Выпала Unique! Минтим SBT...")
            tx_hash = mint_sbt(w3, main_contract, private_key)
            logger.info(f"Транзакция SBT: https://soneium.blockscout.com/tx/{tx_hash}")
            
            receipt = await wait_for_confirmation(w3, tx_hash)
            if receipt and receipt.status == 1:
                logger.success(f"🎉 Кошелек {wallet_address}: SBT успешно заминчен!")
                
                # Сохраняем в БД
                mark_wallet_completed(wallet_address, "redbutton_badge", 1, 1, QUESTS_DB_PATH)
                
                if unique_token_id is not None:
                    try:
                        owner = item_contract.functions.ownerOf(unique_token_id).call()
                        if owner.lower() == wallet_address.lower():
                            logger.info(f"Продаем Unique NFT #{unique_token_id}...")
                            await sell_nfts_batch(w3, main_contract, private_key, [unique_token_id])
                            logger.success(f"Unique NFT #{unique_token_id} успешно продан")
                            await asyncio.sleep(3)
                            
                            logger.info("Обмениваем все RBTN на ETH...")
                            swap_tx_hash = await swap_rbtn_to_eth(w3, token_contract, private_key, wallet_address)
                            if swap_tx_hash:
                                logger.success("✅ Все RBTN успешно обменяны на ETH")
                            else:
                                logger.warning("Не удалось обменять RBTN на ETH, но продолжаем")
                        else:
                            logger.warning(f"Unique NFT #{unique_token_id} больше не принадлежит кошельку")
                    except Exception as e:
                        logger.warning(f"Не удалось продать Unique NFT: {e}")
                else:
                    logger.warning("Не удалось получить tokenId Unique NFT")
                
                break
            else:
                logger.error("Ошибка при минте SBT, продолжаем...")
                await asyncio.sleep(5)
                continue
        else:
            logger.warning("❌ Unique не выпала, продаем NFT из OG транзакции...")
            og_token_id = get_minted_token_id(w3, item_contract, receipt, wallet_address)
            if og_token_id is not None:
                try:
                    owner = item_contract.functions.ownerOf(og_token_id).call()
                    if owner.lower() == wallet_address.lower():
                        logger.info(f"Продаем NFT #{og_token_id} из OG транзакции...")
                        await sell_nfts_batch(w3, main_contract, private_key, [og_token_id])
                        logger.info("NFT из OG транзакции продан")
                        await asyncio.sleep(3)
                except Exception as e:
                    logger.warning(f"Не удалось продать NFT из OG транзакции: {e}")
            else:
                logger.warning("Не удалось получить tokenId из OG транзакции")
            
            await asyncio.sleep(2)


async def _run_async() -> None:
    """Асинхронная часть главной функции"""
    # Инициализация БД
    init_quests_database(QUESTS_DB_PATH)
    logger.info("База данных инициализирована")
    
    # Загрузка всех ключей
    all_keys = load_all_keys()
    logger.info(f"Загружено ключей: {len(all_keys)}")
    
    if not all_keys:
        logger.error("Не найдено приватных ключей")
        return
    
    # Подключение к RPC
    w3 = Web3(Web3.HTTPProvider(RPC_URL_DEFAULT, request_kwargs={"timeout": 60}))
    
    if not w3.is_connected():
        logger.error("Не удалось подключиться к RPC")
        return
    
    chain_id = int(w3.eth.chain_id)
    if chain_id != CHAIN_ID:
        logger.warning(f"Неожиданный chainId: {chain_id} (ожидали {CHAIN_ID})")
    
    # Получение контрактов
    main_contract = w3.eth.contract(MAIN_CONTRACT_ADDRESS, abi=MAIN_ABI)
    item_contract = w3.eth.contract(ITEM_CONTRACT_ADDRESS, abi=ITEM_ABI)
    reward_pool_address = REWARD_POOL_ADDRESS
    sbt_contract = w3.eth.contract(SBT_BADGE_ADDRESS, abi=SBT_ABI)
    token_contract = w3.eth.contract(RBTN_TOKEN_ADDRESS, abi=ERC20_PERMIT_ABI)
    
    logger.info(f"Item контракт: {ITEM_CONTRACT_ADDRESS}")
    logger.info(f"RewardPool контракт: {REWARD_POOL_ADDRESS}")
    logger.info(f"SBT контракт: {SBT_BADGE_ADDRESS}")
    logger.info(f"RBTN токен: {RBTN_TOKEN_ADDRESS}")
    
    # Перемешиваем ключи случайно
    indices = list(range(len(all_keys)))
    random.shuffle(indices)
    
    wallets_completed = 0
    wallets_failed = 0
    
    # Обработка каждого кошелька
    for i, key_index in enumerate(indices, 1):
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"Кошелек {i}/{len(all_keys)} (индекс: {key_index})")
            logger.info(f"{'='*60}")
            
            private_key = load_private_key(key_index=key_index)
            
            await process_wallet(
                w3, main_contract, item_contract, sbt_contract,
                token_contract, reward_pool_address, private_key
            )
            
            wallets_completed += 1
            
        except KeyboardInterrupt:
            logger.info("Остановлено пользователем")
            break
        except Exception as e:
            logger.error(f"Ошибка при обработке кошелька: {e}")
            import traceback
            logger.error(traceback.format_exc())
            wallets_failed += 1
            continue
    
    # Статистика
    logger.info("=" * 60)
    logger.info("СТАТИСТИКА")
    logger.info("=" * 60)
    logger.info(f"Успешно обработано: {wallets_completed}")
    logger.info(f"Ошибок: {wallets_failed}")
    logger.info(f"Всего обработано: {wallets_completed + wallets_failed}")


def run() -> None:
    """Главная функция для запуска модуля из main.py"""
    # Настройка логирования
    logger.remove()
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level="INFO",
    )
    
    try:
        asyncio.run(_run_async())
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
        colorize=True,
    )
    
    try:
        run()
    except KeyboardInterrupt:
        logger.info("Остановлено пользователем")

