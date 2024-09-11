import pandas as pd
# import numpy as np
# import math
#
# # dataset overview
# BABD = pd.read_csv("E:\model project\BTCanalysis\BABD-13.csv")
# print(BABD.label)
# wallet_features = pd.read_csv("E:\model project\BTCanalysis\EllipticPlusPlus-main\Actors Dataset\wallets_features.csv")
# # print("The BABD-13:\n")
#
# # print("The wallet_features of Elliptic++\n")
# # print(wallet_features['address'])
# # for addr in BABD['account']:
# #     if(addr in wallet_features['address']):
# #         print("yes")
# #         break
# print(BABD['account'])


import requests

# headers = {
#     'X-API-TOKEN': 't410bd46f64dc7d7cdc7ae38d38c9a87a7d5868159bdac0186bfd07baab647e59'
# }
#
# response = requests.get('https://tools-gateway.api.btc.com/rest/api/v1.0/nodeapi/tx/standard/'+'7cb8686fe8adec1d5b64e45bb3710ed9fc51694ccee29c570d7b91a993132d3e', headers=headers)
# print(response.json())
response  = requests.get('')