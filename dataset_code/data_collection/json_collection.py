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
head_1 = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 Edg/127.0.0.0'
}
# proxies = {
#     # proxies list
#     # "https": "https://31.43.179.80:443"
#     "https": "https://z81.topzz.cc:41381"
#     # "https": "https://128.199.213.63:443"Failed to establish a new connection: [WinError 10061] 由于目标计算机积极拒绝，无法连接
#
# }
# proxies_list = [{'https':'https://...'},{'https':'https://...'}]
proxies_list = [{}]
unable_proxies = []
root_path = 'D:/json_data/'
rawaddr_url = 'https://blockchain.info/rawaddr/'
# rawtx_url = 'https://blockchain.info/rawtx/'
rawtx_url = 'https://tools-gateway.api.btc.com/rest/api/v1.0/nodeapi/tx/standard/'
def get_response_1(url):
    global proxies_list
    random_index = random.randint(0,len(proxies_list)-1)
    time.sleep(random.uniform(2.0,5.0))
    response = requests.get(url, headers=head_1, proxies=proxies_list[random_index])
    response.encoding = 'utf-8'
    status = response.status_code
    if status==200:
        return response
    elif status==429:
        unable_proxies.append(proxies_list[random_index])
        proxies_list.pop(random_index)
        get_response_1(url)
    else:
        get_response_1(url)


def get_response_2(url):
    response = requests.get(url, headers=head_2)
    response.encoding = 'utf-8'
    status = response.status_code
    if status==200:
        return response

    else:
        get_response_2(url)


def getjson_by_addr(addr, count):
    # by blockchain.com
    print('正在处理地址:{}'.format(addr))
    url = rawaddr_url + addr
    response = get_response_1(url)
    json_data = json.loads(response.text)
    _dir = os.path.join(root_path,'Query_'+str(count))
    if not os.path.exists(_dir):
        os.makedirs(_dir)
    with open(os.path.join(_dir,'%s.json'%addr), 'w+',encoding='utf-8') as f:
        json.dump(json_data, f)
    return json_data

def getjson_by_txid(txid, count):
    # by btc.com
    print('正在处理交易:{}'.format(txid))
    url = rawtx_url + txid
    response = get_response_2(url)
    json_data = json.loads(response.text)
    _dir = os.path.join(root_path,'tx'+str(count))
    if not os.path.exists(_dir):
        os.makedirs(_dir)
    with open(os.path.join(_dir,'%s.json'%txid), 'w+', encoding='utf-8') as f:
        json.dump(json_data, f)
    return json_data

def near_neighbor(k):



file_path = 'D:/raw_addr_csv/CloudBet.com'
file_list = os.listdir(file_path)

if __name__=='__main__':
    for file in file_list:
        with open(os.path.join(file_path,file)) as f:
            df = pd.read_csv(f, skiprows=1)
            for addr in df['address']:
                addr_json_data = getjson_by_addr(addr,1)
                txids = [tx["hash"] for tx in addr_json_data["txs"]]
                for txhash in txids:
                    txhash_json_data = getjson_by_txid(txhash,1)
                    addr_in=[vin["prevout"]["address"] for vin in txhash_json_data["vin"]].
                    addr_out=[vout["scriptPubKey"]["address"] for vout in txhash_json_data["vout"]]


























with open(os.path.join(file_path,'1.csv'),'r') as f:
    df = pd.read_csv(f,skiprows=1)
    # for addr in df['address']:
    addr = df['address'][2]
    print(addr)
    url = rawaddr_url + addr
    # response = requests.get('https://tools-gateway.api.btc.com/rest/api/v1.0/nodeapi/tx/standard/'+addr, headers=head)
    response = requests.get(url,headers=head, verify=False, proxies=proxies_list)
    response.encoding = 'utf-8'
    print(response.status_code)
    if response.status_code==200:
        json_data = json.loads(response.text)
        print(json_data)
    elif response.status_code==429:
        print("Rate limit.")




