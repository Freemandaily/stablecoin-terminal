import requests
from eth_utils import to_checksum_address  # pip install eth-utils

# STABLES = requests.get("https://stablecoins.llama.fi/stablecoins?includePrices=true").json()["peggedAssets"]

# def trustwallet_url(addr, chain="ethereum"):
#     print
#     return f"https://raw.githubusercontent.com/trustwallet/assets/master/blockchains/{chain}/assets/{to_checksum_address(addr)}/logo.png"

# # per-coin detail has addresses per chain
# detail = requests.get(f"https://stablecoins.llama.fi/stablecoin/{STABLES[0]['id']}").json()
# print(trustwallet_url(detail.get("address")))

chcc = to_checksum_address('0x6e109e9dd7fa1a58bc3eff667e8e41fc3cc07aef')
print(chcc)