import traceback
import csv
import requests
import pandas as pd
import os
import json
import random
import time

head = {
    "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 Edg/127.0.0.0'
}
raw_addr_url = "https://blockchain.info/rawaddr/"


def get_response(url):
    time.sleep(random.uniform(2.5, 4.0))
    response = requests.get(url, headers=head)
    response.encoding = "utf-8"
    status = response.status_code
    if status == 200:
        return response
    elif status == 429:
        print("Rate limit.")
        raise ConnectionError
    else:
        get_response(url)


root_path = 'D:/json_data/'


def get_json_by_addr(addr, k):
    print("正在处理地址：{}".format(addr))
    url = raw_addr_url + addr
    response = get_response(url)
    json_data = json.loads(response.text)
    _dir = os.path.join(root_path, "k=" + str(k))
    if not os.path.exists(_dir):
        os.makedirs(_dir)
    with open(os.path.join(_dir, "%s.json" % addr), "w", encoding="utf-8") as f:
        json.dump(json_data, f)
    return json_data


def near_neighbor(k, addr, _txhash=""):
    addr_json_data = get_json_by_addr(addr, k)
    addr_txhash = dict()
    try:
        for tx in addr_json_data["txs"]:
            txhash = tx["hash"]
            if txhash != _txhash:
                for inputs in tx["inputs"]:
                    if inputs["prev_out"]["addr"] != addr:
                        addr_txhash[inputs["prev_out"]["addr"]] = txhash
    except Exception as e:
        traceback.print_exc()
        pass
    else:
        with open(os.path.join("D:/node_addr/", str(k) + ".csv"), "a", encoding="utf-8") as file:
            writer = csv.writer(file)
            for row in addr_txhash.items():
                writer.writerow(row)


'''                
                将 addr,txhash 以.csv文件存储下来
                把查询addr 排除在外，避免重复
                下一次查询，把此次txhash排除
'''

file_path = 'D:/raw_addr_csv/CloudBet.com'
file_list = os.listdir(file_path)
if __name__ == "__main__":
    if not os.path.exists('D:/node_addr'):
        os.makedirs('D:/node_addr')
    # 第一级
    for files in file_list:
        with open(os.path.join(file_path, files)) as f1:
            df = pd.read_csv(f1, skiprows=1)
            for addr in df["address"]:
                near_neighbor(1,addr)

    # # 第二级
    # with open("D:/node_addr/1.csv","r") as f2:
    #     addr_txhash_list = f2.readlines()
    #     for addr_txhash in addr_txhash_list:
    #         addr,txhash = addr_txhash.split(",")
    #         near_neighbor(2,addr,txhash)
    #
    # # 第三级
    # with open("D:/node_addr/2.csv","r") as f3:
    #     addr_txhash_list = f3.readlines()
    #     for addr_txhash in addr_txhash_list:
    #         addr,txhash = addr_txhash.split(",")
    #         near_neighbor(3,addr,txhash)
    #
    # # 第四级
    # with open("D:/node_addr/1.csv","r") as f4:
    #     addr_txhash_list = f4.readlines()
    #     for addr_txhash in addr_txhash_list:
    #         addr,txhash = addr_txhash.split(",")
    #         near_neighbor(4,addr,txhash)
