import traceback

import requests
import random
import urllib3
import time
import pandas as pd
import json
import os
# urllib3.util.ssl_.DEFAULT_CIPHERS = 'ALL'



head_2 = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 Edg/127.0.0.0',
        'X-API-TOKEN': 't410bd46f64dc7d7cdc7ae38d38c9a87a7d5868159bdac0186bfd07baab647e59',
        'content-type': "application/json"
}
# head_1 = {
#         "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 Edg/127.0.0.0'
# }
# proxies = {
#     # proxies list
#     # "https": "https://31.43.179.80:443"
#     "https": "https://z81.topzz.cc:41381"
#     # "https": "https://128.199.213.63:443"Failed to establish a new connection: [WinError 10061] 由于目标计算机积极拒绝，无法连接
#
# }
# proxies_list = [{'https':'https://...'},{'https':'https://...'}]
# proxies_list = [{}]
# unable_proxies = []
root_path = 'D:/json_data/'
if not os.path.exists("D:/node_addr/"):
    os.makedirs("D:/node_addr/")
rawaddr_url = 'https://tools-gateway.api.btc.com/rest/api/v1.0/nodeapi/address/txlist/'

rawtx_url = 'https://tools-gateway.api.btc.com/rest/api/v1.0/nodeapi/tx/standard/'
def get_response_1(url):
    global proxies_list
    # random_index = random.randint(0,len(proxies_list)-1)
    # time.sleep(random.uniform(5.0,7.0))
    # response = requests.get(url, headers=head_1, proxies=proxies_list[random_index],verify=False)
    response = requests.get(url, headers=head_2, verify=False,timeout=15)
    response.encoding = 'utf-8'
    status = response.status_code
    if status==200:
        return response
    elif status==429:
        # unable_proxies.append(proxies_list[random_index])
        # proxies_list.pop(random_index)
        get_response_1(url)
    else:
        get_response_1(url)


def get_response_2(url):
    response = requests.get(url, headers=head_2,verify=False)
    response.encoding = 'utf-8'
    status = response.status_code
    if status==200:
        return response

    else:
        get_response_2(url)


def getjson_by_addr(addr, count):
    # by btc.com
    print('正在处理地址:{}'.format(addr))
    url = rawaddr_url + addr + "/1/10000"
    response = get_response_1(url)
    json_data = json.loads(response.text)
    if json_data["msg"]=="success":
        _dir = os.path.join(root_path,'Query_'+str(count))
        if not os.path.exists(_dir):
            os.makedirs(_dir)
        with open(os.path.join(_dir,'%s.json'%addr), 'w+',encoding='utf-8') as f:
            #  addr的交易信息以addr.json命名，存放于Query_.文件夹
            json.dump(json_data, f)
        return json_data

def getjson_by_txid(txid, count):
    # by btc.com
    print('正在处理交易:{}'.format(txid))
    url = rawtx_url + txid
    response = get_response_2(url)
    json_data = json.loads(response.text)
    if json_data["msg"]=="success":
        _dir = os.path.join(root_path,'tx'+str(count))
        if not os.path.exists(_dir):
            os.makedirs(_dir)
        with open(os.path.join(_dir,'%s.json'%txid), 'w+', encoding='utf-8') as f:
            json.dump(json_data, f)
        return json_data

def near_neighbor(addr,k):
    # creat k near neighbor and write endpoint addr in text file.
    addr_list = []
    addr_json_data = getjson_by_addr(addr, count=k)
    txids = [txInfo["txId"] for txInfo in addr_json_data["data"]["list"]]
    for txhash in txids:
        try:
            txhash_json_data = getjson_by_txid(txhash, count=k)
            addr_in = [vin["prevout"]["address"] for vin in txhash_json_data["data"]["txInfo"]["vin"]]
            addr_out = [vout["scriptPubKey"]["address"] for vout in txhash_json_data["data"]["txInfo"]["vout"]]
            addr_list.extend(addr_in)
            addr_list.extend(addr_out)
            addr_list.remove(addr)
        except Exception as e:
            print("An error occur.")
            print(e)
            traceback.print_exc()
            pass
        else:
            with open(os.path.join("D:/node_addr/",str(k)+".txt"), 'a', encoding='utf-8') as f:
                for addr in addr_list:
                    f.write("{0},{1}\n".format(addr,txhash)) # addr is gotten by txhash ,in order to avoid repeating request





file_path = 'D:/raw_addr_csv/CloudBet.com'
file_list = os.listdir(file_path)
if __name__=='__main__':
    for file in file_list:
        with open(os.path.join(file_path,file)) as f:
            df = pd.read_csv(f, skiprows=1)
            # 第一级
            for addr in df['address']:
                near_neighbor(addr,1)
    with open("D:/node_addr/1.txt","r") as f2:
        addrtxhash_list = f2.readline().split(",")
        for addr in addrtxhash_list:
            near_neighbor(addr,2)
    with open("D:/node_addr/1.txt","r") as f3:
        addrtxhash_list = f3.readline().split(",")
        for addr in addrtxhash_list:
            near_neighbor(addr,3)
'''
                for more k neighbors:
    with open("D:/node_addr/1.txt","r") as f4:
            addrtxhash_list = f4.readline().split(",")
            for addr in addrtxhash_list:
                near_neighbor(addr,4)
                
'''





























# with open(os.path.join(file_path,'1.csv'),'r') as f:
#     df = pd.read_csv(f,skiprows=1)
#     # for addr in df['address']:
#     addr = df['address'][2]
#     print(addr)
#     url = rawaddr_url + addr
#     # response = requests.get('https://tools-gateway.api.btc.com/rest/api/v1.0/nodeapi/tx/standard/'+addr, headers=head)
#     response = requests.get(url,headers=head, verify=False, proxies=proxies_list)
#     response.encoding = 'utf-8'
#     print(response.status_code)
#     if response.status_code==200:
#         json_data = json.loads(response.text)
#         print(json_data)
#     elif response.status_code==429:
#         print("Rate limit.")




