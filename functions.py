from web3 import Web3
import time
import json
from sqlalchemy import create_engine
import os
import requests
import datetime

RPC_ENDPOINTS = {
    1: 'https://eth.llamarpc.com',  # Public ETH RPC
    10: 'https://optimism.publicnode.com',  # Public Optimism RPC
    42161: 'https://arb1.arbitrum.io/rpc'  # Public Arbitrum RPC
}

'''rpc для ротации'''
RPC_ENDPOINTS2 = {
    1:  'https://1rpc.io/eth',  # Public ETH RPC
    10:  'https://rpc.ankr.com/optimism',  # Public Optimism RPC
    42161: 'https://arbitrum.llamarpc.com'  # Public Arbitrum RPC
}

abi_decimals = [{"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "type": "function"}]

ERC4626_ABI = [
    {"inputs": [], "name": "totalAssets", "outputs": [{"name": "", "type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "totalSupply", "outputs": [{"name": "", "type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "asset", "outputs": [{"name": "", "type": "address"}], "stateMutability": "view", "type": "function"}
]


ERC4626_ABI_sbusd = [{"inputs":[{"internalType":"address","name":"_implementation","type":"address"}, {"internalType":"bytes","name":"_data","type":"bytes"}],"stateMutability":"nonpayable","type":"constructor"},
                     {"inputs":[{"internalType":"address","name":"target","type":"address"}],"name":"AddressEmptyCode","type":"error"},
                     {"inputs":[{"internalType":"address","name":"implementation","type":"address"}],"name":"ERC1967InvalidImplementation","type":"error"},
                     {"inputs":[],"name":"ERC1967NonPayable","type":"error"},{"inputs":[],"name":"FailedCall","type":"error"},{"anonymous":False,
                       "inputs":[{"indexed":True,"internalType":"address","name":"implementation","type":"address"}],"name":"Upgraded","type":"event"},{"stateMutability":"payable","type":"fallback"}]


def get_token_info(contract_address, endpoint, chain_id):



    # ABI для decimals и symbol
    abi_decimals = [{"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}],
                     "type": "function"}]
    abi_symbol = [{"constant": True, "inputs": [], "name": "symbol", "outputs": [{"name": "", "type": "string"}],
                   "type": "function"}]


    # decimals()
    time.sleep(0.15)
    decimals_data = safe_rpc_call(endpoint, 'eth_call',
                                  [{'to': contract_address, 'data': Web3.keccak(text='decimals()')[:4].hex()}, 'latest'])
    decimals = int(decimals_data, 16) if decimals_data else 18
    time.sleep(0.15)

    return decimals

def safe_rpc_call(endpoint, method, params):
    try:
        payload = {"jsonrpc": "2.0", "method": method, "params": params, "id": 1}
        resp = requests.post(endpoint, json=payload, timeout=15)

        data = resp.json()

        if 'error' in data:
            print(f" RPC {method}: {data['error']}")
            return None
        return data['result']
    except Exception as e:
        print(f" RPC {method}: {e}")
    return None


def load_config(config_path='config.json'):

    config_path = os.path.join(os.path.dirname(__file__), config_path)
    with open(config_path, 'r') as f:
        return json.load(f)


config = load_config()
db_url = f"postgresql+psycopg2://{config['db']['user']}:{config['db']['password']}@{config['db']['host']}:{config['db']['port']}/{config['db']['dbname']}"
engine = create_engine(db_url, echo=False)


