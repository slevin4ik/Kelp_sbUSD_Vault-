
# Python 3.7
from web3 import Web3
from functions import RPC_ENDPOINTS, ERC4626_ABI, abi_decimals



w3 = Web3(Web3.HTTPProvider(RPC_ENDPOINTS[1]))
vault_address = '0x8ECC0B419dfe3AE197BC96f2a03636b5E1BE91db'


vault = w3.eth.contract(address=vault_address, abi=ERC4626_ABI)

'''2 задание'''


def get_vault_metrics():
    #Получает TVL, Share Price для Vault

    total_assets_raw = vault.functions.totalAssets().call()
    total_supply_raw = vault.functions.totalSupply().call()
    asset_addr = vault.functions.asset().call()


    asset_contract = w3.eth.contract(address=asset_addr, abi=abi_decimals)
    asset_decimals = asset_contract.functions.decimals().call()
    token_decimals = asset_decimals



    total_assets = total_assets_raw / (10 ** asset_decimals)
    total_supply = total_supply_raw / (10 ** token_decimals)


    tvl_usd = total_assets
    share_price = total_assets / total_supply if total_supply > 0 else 0
    print(total_assets)
    return tvl_usd, share_price


def verify_real_assets():

    real_usdc = vault.functions.balanceOf(vault_address).call()
    reported_assets = vault.functions.totalAssets().call()


    return {
        'real_balance_usd': real_usdc / 1e6,
        'reported_total_assets': reported_assets / 1e6,
        'discrepancy_pct': (reported_assets - real_usdc) / real_usdc * 100
    }





if w3.isConnected():
    metrics = get_vault_metrics()

    print(f"tvl_usd {metrics[0]}")
    print(f"share_price {metrics[1]}")







########



