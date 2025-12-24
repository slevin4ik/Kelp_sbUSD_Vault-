# Python 3.7
# Скрипт работает на запись без ошибок, но только записывает на current_block по (public rpc)
# В collect_historical_metrics - прописана заглушка с симулякром исторических данных для дальнейшего подключения и забора из архива
import sys
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
import psycopg2
from web3 import Web3
import pandas as pd
from functions import ERC4626_ABI, abi_decimals, engine, config
from datetime import timezone

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('etl.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)




# ===== EXTRACTOR ==========
def get_web3_connection() -> Web3:
    #Подключение к РПС
    for rpc_url in config["rpc_urls"]:
        try:
            w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={'timeout': 10}))
            if w3.isConnected():
                logger.info(f" Connected to {rpc_url}")
                return w3
        except Exception as e:
            logger.warning(f"Failed {rpc_url}: {e}")
    raise Exception("All RPC endpoints scam!")


def extract_vault_metrics(vault_config: Dict, w3: Web3, block_number: int) -> Dict[str, Any]:
    #Извлекает метрики по одному волту
    try:
        vault = w3.eth.contract(address=vault_config["address"], abi=ERC4626_ABI)

        # Используем block_number или текущий блок
        current_block = block_number
        
        total_assets_raw = vault.functions.totalAssets().call(block_identifier=current_block)
        total_supply_raw = vault.functions.totalSupply().call(block_identifier=current_block)
        asset_addr = vault.functions.asset().call(block_identifier=current_block)

        # Decimals базового актива
        asset_contract = w3.eth.contract(address=asset_addr, abi=abi_decimals)
        asset_decimals = asset_contract.functions.decimals().call()

        return {
            "address": vault_config["address"],
            "total_assets_raw": total_assets_raw,
            "total_supply_raw": total_supply_raw,
            "asset_address":asset_addr,
            "asset_decimals": asset_decimals,
            'block_number': block_number
        }
    except Exception as e:
        logger.error(f"Extract failed for {vault_config['address']}: {e}")
        return None


# ================ TRANSFORMER ===========
def transform_metrics(raw_data: Dict, vault_config: Dict, block_timestamp) -> Dict[str, Any]:
    #Нормализация метрик
    if not raw_data:
        return None

    asset_decimals = raw_data["asset_decimals"]
    total_assets = raw_data["total_assets_raw"] / (10 ** asset_decimals)
    total_supply = raw_data["total_supply_raw"] / (10 ** asset_decimals)

    share_price = total_assets / total_supply if total_supply > 0 else 0


    return {
        "vault_address": vault_config["address"],
        "vault_id": None,  # Заполнится при загрузке
        "block_number": raw_data["block_number"],
        "utc_time": block_timestamp,
        "tvl_usd": float(total_assets),
        "share_price": float(share_price),
        "total_supply": float(total_supply),
        "asset_decimals": asset_decimals
    }


# ============= LOAD =====================
def get_db_connection():
    #Подключение к Postgres бд
    return psycopg2.connect(**config["db"])


def ensure_vault_exists(cursor, vault_config: Dict) -> int:
    #Создает/возвращает vault_id

    cursor.execute("""
        INSERT INTO defi_vaults.vaults (chain_id, address, name, symbol, asset_address)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (chain_id, address) DO UPDATE SET name = EXCLUDED.name, symbol = EXCLUDED.symbol
        RETURNING id
    """, (vault_config["chain_id"], vault_config["address"], vault_config["name"],
          vault_config["symbol"], vault_config.get("asset_address")))
    return cursor.fetchone()[0]


def load_metrics(metrics_batch: List[Dict]):

    if not metrics_batch:
        return

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            # 1. Обеспечиваем chain и vault_id
            vault_ids = {}
            for metric in metrics_batch:
                vault_config = next(v for v in config["vaults"] if v["address"] == metric["vault_address"])
                vault_id = ensure_vault_exists(cursor, vault_config)
                vault_ids[metric["vault_address"]] = vault_id

            # 2. INSERT по одной записи

            df = pd.DataFrame()

            for metric in metrics_batch:
                metric["vault_id"] = vault_ids[metric["vault_address"]]
                utc_time = pd.to_datetime(metric["utc_time"], utc=True)

                utc_time_naive = pd.to_datetime(metric["utc_time"]).tz_localize(None) # чтоб серверное время не завписал в PostgreSQL


                raw_df = pd.DataFrame({'vault_id': metric["vault_id"],
                                       'block_number': metric["block_number"],
                                       'utc_datetime': [utc_time_naive],
                                       'tvl_usd': [float(metric["tvl_usd"])],
                                       'share_price': [float(metric["share_price"])],
                                       'total_supply': [float(metric["total_supply"])],
                                       'asset_decimals': [int(metric["asset_decimals"])]
                })

                df = df.append(raw_df, ignore_index=True)


            df.to_sql(schema='defi_vaults', name='vaults_performance', con=engine, if_exists='append', index=False)


            logger.info(f" Saved/Updated {len(df)} metrics")


#============= MAIN ETL funcs =========
def collect_current_metrics() -> List[Dict]:
    #блок → timestamp → метрики на блоке

    w3 = get_web3_connection()

    current_block = w3.eth.blockNumber
    block_info = w3.eth.getBlock(current_block)
    block_timestamp = datetime.fromtimestamp(block_info.timestamp, tz=timezone.utc)

    logger.info(f" Current block: {current_block}")
    logger.info(f" Block timestamp: {block_timestamp}")


    metrics = []
    for vault_config in config["vaults"]: # пробегаемся по всем волтам, но там сейчас один
        logger.info(f"Extracting {vault_config['name']} at block {current_block}")
        raw_data = extract_vault_metrics(vault_config, w3, current_block)
        transformed = transform_metrics(raw_data, vault_config, block_timestamp)

        if transformed:
            metrics.append(transformed)

    return metrics


def collect_historical_metrics(hours_back: int, times_in_a_hour: int) -> List[Dict]:
    logger.info(f"Loading {hours_back}h history for {len(config['vaults'])} vaults...")

    metrics = []
    for i in range(hours_back * times_in_a_hour):
        utc_time = datetime.utcnow() - timedelta(minutes=60 / times_in_a_hour * i)

        for vault_config in config["vaults"]:
            # ФИКСИРОВАННЫЕ данные вместо RPC
            base_tvl = 150_000_000 * 10 ** 6
            base_supply = 100_000_000 * 10 ** 6

            # Имитация падения TVL
            growth_factor = 1 - (i * 0.00005)
            simulated_raw = {
                "address": vault_config["address"],
                "total_assets_raw": int(base_tvl * growth_factor),
                "total_supply_raw": base_supply,
                "asset_address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",  # USDC
                "asset_decimals": 6,
                "block_number": 22070000 - i * 12
            }

            transformed = transform_metrics(simulated_raw, vault_config, utc_time)
            if transformed:
                metrics.append(transformed)

    logger.info(
        f" Generated {len(metrics)} records (TVL: ${base_tvl / 1e6:.1f}M → ${base_tvl * growth_factor / 1e6:.1f}M)")
    return metrics


def needs_history_for_vault(cursor, vault_address, hours_back) -> bool:
    #Проверяет историю конкретного волта
    cursor.execute("""
        SELECT COUNT(*)
        FROM defi_vaults.vaults_performance vp
        JOIN defi_vaults.vaults v ON vp.vault_id = v.id
        WHERE v.address = %s AND vp.utc_datetime > NOW() - (%s * INTERVAL '1 hour')
        AND vp.utc_datetime < NOW() - INTERVAL '5 minutes'  -- Исключаем свежие 5 мин. Тут возможны изменения в логике при масштабировании 
    """, (vault_address,hours_back))

    result = cursor.fetchone()
    count = result[0] if result else 0
    return count == 0


def main():
    hours_back = config['load_history_hours']
    times_in_a_hour = config['times_in_a_hour']

    try:
        logger.info("Starting DeFi Vaults ETL")

        # 1. Текущие метрики
        current_metrics = collect_current_metrics()

        load_metrics(current_metrics)

        # 2. История для волтов без данных
        with get_db_connection() as conn:
            with conn.cursor() as cursor:

                for vault_config in config["vaults"]:

                    if needs_history_for_vault(cursor, vault_config["address"], hours_back):
                        logger.info(f"History needed for {vault_config['name']}")

                        history_metrics = collect_historical_metrics(hours_back, times_in_a_hour)
                        load_metrics(history_metrics)


        logger.info("ETL completed")

    except Exception as e:
        logger.error(f"ETL failed: {e}")
        raise


if __name__ == "__main__":
    main()



