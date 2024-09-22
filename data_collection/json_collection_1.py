import traceback
import csv
import requests
import time
import pandas as pd
import os
import json


head = {
    "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 Edg/127.0.0.0'
}
raw_addr_url = "https://services.tokenview.io/vipapi/address/btc/"
tail = "/1/50?apikey=We5tjiEsYg7cViu8TlDq"


def get_response(url):
    try:
        response = requests.get(url, headers=head)
        response.encoding = "utf-8"
        status = response.status_code
        if (status == 200) and (response is not None):
            return response
        elif status == 429:
            print("Rate limit.")
            raise ConnectionRefusedError
        else:
            get_response(url)
    except:
        get_response(url)



def get_json_by_addr(addr):
    try:
        print("正在处理地址：{}".format(addr))
        url = raw_addr_url + addr + tail
        response = get_response(url)
        json_data = response.json()
        if json_data["msg"] == "成功":
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(json_data, f)
            return json_data
        else:
            raise ConnectionError
    except:
        with open("F:/log.txt","a")as exc_file:
            exc_file.write(time.ctime())
            exc_file.write(addr)
            exc_file.write(traceback.format_exc())
        traceback.print_exc()
        get_json_by_addr(addr)


def near_neighbor(addr, _txhash=""):
    addr_json_data = get_json_by_addr(addr)
    addr_txhash = dict()
    try:
        for tx in addr_json_data["data"][0]["txs"]:
            txhash = tx["txid"]
            if txhash != _txhash:
                for inputs in tx["inputs"]:
                    if inputs["address"] != addr:
                        addr_txhash[inputs["address"]] = txhash
                for outputs in tx["outputs"]:
                    if outputs["address"] != addr:
                        addr_txhash[outputs["address"]] = txhash
            else:
                continue
    except Exception as e:
        with open("F:/log.txt","a")as exc_file:
            exc_file.write(time.ctime())
            exc_file.write(addr)
            exc_file.write(traceback.format_exc())
        traceback.print_exc()
        pass
    else:
        writer = csv.writer(node_addr_file)
        for row in addr_txhash.items():
            writer.writerow(row)


'''                
                将 addr,txhash 以.csv文件存储下来
                把查询addr 排除在外，避免重复
                下一次查询，把此次txhash排除
'''

root_path = 'D:/json_data/'
file_path = "D:/raw_addr_csv/CoinGaming.io"
file_list = os.listdir(file_path)
if __name__ == "__main__":
    if not os.path.exists('D:/node_addr'):
        os.makedirs('D:/node_addr')
    # _dir = os.path.join(root_path, "k=1")
    # if not os.path.exists(_dir):
    #     os.makedirs(_dir)
    # 第一级
    # node_addr_file = open(os.path.join("D:/node_addr/", "1-1.csv"), "a", encoding="utf-8", newline='')
    # for files in file_list:
    #     print("打开文件{0},index = {1}".format(files,file_list.index(files)))
    #     with open(os.path.join(file_path, files)) as f1:
    #         df = pd.read_csv(f1, skiprows=1)
    #         count = 0
    #         for addr in df["address"]:
    #             filename = os.path.join(_dir, "%s.json" % addr)
    #             if not os.path.exists(filename):
    #                 print("index = {}".format(count))
    #                 near_neighbor(addr)
    #             else:
    #                 print("index = {}已经存在".format(count))
    #             count += 1
    # node_addr_file.close()

    # 第二级
    # with open("D:/node_addr/1-1.csv","r") as f2:
    #     addr_txhash_list = f2.readlines()
    #     _dir = os.path.join(root_path, "k=2")
    #     if not os.path.exists(_dir):
    #         os.makedirs(_dir)
    #     node_addr_file = open(os.path.join("D:/node_addr/", "2-1.csv"), "a", encoding="utf-8", newline='')
    #     count = 0
    #     for addr_txhash in addr_txhash_list: #2024.9.18:31290+7848
    #         addr,txhash = addr_txhash.strip().split(",")
    #         filename = os.path.join(_dir, "%s.json" % addr)
    #         if count%500 == 0:  # 每500次刷新缓冲区
    #             node_addr_file.flush()
    #         if not os.path.exists(filename):   # 避免重复查询影响效率
    #             print("index = {}".format(count))
    #             near_neighbor(addr,txhash)
    #         else:
    #             print("index = {}已经存在".format(count))
    #         count += 1
    #     node_addr_file.close()
    #
    # # 第三级
    with open("D:/node_addr/2-1.csv","r") as f3:
        addr_txhash_list = f3.readlines()
        _dir = os.path.join(root_path, "k=3")
        if not os.path.exists(_dir):
            os.makedirs(_dir)
        node_addr_file = open(os.path.join("D:/node_addr/", "3-1.csv"), "a", encoding="utf-8", newline='')
        count = 0
        for addr_txhash in addr_txhash_list:
            addr,txhash = addr_txhash.strip().split(",")
            filename = os.path.join(_dir, "%s.json" % addr)
            if count % 500 == 0:  # 每1000次刷新缓冲区
                node_addr_file.flush()
            if not os.path.exists(filename):   # 避免重复查询影响效率
                print("index = {}".format(addr_txhash_list.index(addr_txhash)))
                near_neighbor(addr,txhash)
            else:
                print("{}已经存在".format(addr_txhash_list.index(addr_txhash)))
        node_addr_file.close()

    # # 第四级
    # with open("D:/node_addr/3-1.csv","r") as f4:
    #     addr_txhash_list = f4.readlines()
    #     _dir = os.path.join(root_path, "k=4")
    #     if not os.path.exists(_dir):
    #         os.makedirs(_dir)
    #     node_addr_file = open(os.path.join("D:/node_addr/", "4-1.csv"), "a", encoding="utf-8", newline='')
    #     for addr_txhash in addr_txhash_list:
    #         addr,txhash = addr_txhash.strip().split(",")
    #         filename = os.path.join(_dir, "%s.json" % addr)
    #         if count // 1000 == 0:  # 每1000次刷新缓冲区
    #              node_addr_file.flush()
    #         if not os.path.exists(filename):   # 避免重复查询影响效率
    #             print("index = {}".format(addr_txhash_list.index(addr_txhash)))
    #             near_neighbor(addr,txhash)
    #         else:
    #             print("{}已经存在".format(addr_txhash_list.index(addr_txhash)))
    #     node_addr_file.close()
