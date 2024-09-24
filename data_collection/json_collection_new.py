import csv
import requests
import asyncio
import aiohttp
import aiofiles
import time
import pandas as pd
import os
import json


head = {
    "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 Edg/127.0.0.0'
}
raw_addr_url = "https://services.tokenview.io/vipapi/address/btc/"
tail = "/1/50?apikey=BTCxTvlBqH16IKBOBv3N"


async def get_response(url):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=head,timeout=aiohttp.ClientTimeout(connect=10)) as response:
                response.encoding = "utf-8"
                status = response.status
                if status == 200:
                    return await response.json()
                elif status == 429:
                    time.sleep(5)
                    node_addr_file.flush()
                    print("Rate limit.")
                    await get_response(url)
                else:
                    await get_response(url)
    except Exception as e:
        print(e)
        await get_response(url)



async def get_json_by_addr(addr):
    try:
        print("正在处理地址：{}".format(addr))
        url = raw_addr_url + addr + tail
        json_data = await get_response(url)
        if json_data["msg"] == "成功":
            filename = os.path.join(_dir, "%s.json" % addr)
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(json_data, f)
            return json_data
        else:
            raise ConnectionError
    except Exception as e:
        with open("D:/log.txt","a")as exc_file:
            exc_file.write(time.ctime())
            exc_file.write(addr)
            exc_file.write(type(e).__name__)
        print(e)
        await get_json_by_addr(addr)


async def near_neighbor(addr, _txhash=""):
    addr_json_data = await get_json_by_addr(addr)
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
        with open("D:/log.txt","a")as exc_file:
            exc_file.write(time.ctime())
            exc_file.write(addr)
            exc_file.write(type(e).__name__)
        print(e)
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
def is_file_exist(addr,_dir):
    filename = os.path.join(_dir, "%s.json" % addr)
    return os.path.exists(filename)
async def main3(addr_tx_tuple_list):
    _dir = os.path.join(root_path, "k=3")
    tasks = [near_neighbor(addr_tx_tuple[0],addr_tx_tuple[1]) for addr_tx_tuple in [x.strip().split(",") for x in addr_tx_tuple_list] if not is_file_exist(addr_tx_tuple[0],_dir)]
    await asyncio.gather(*tasks)

async def main4(addr_tx_tuple_list):
    _dir = os.path.join(root_path, "k=4")
    tasks = [near_neighbor(addr_tx_tuple[0],addr_tx_tuple[1]) for addr_tx_tuple in [x.strip().split(",") for x in addr_tx_tuple_list] if not is_file_exist(addr_tx_tuple[0],_dir)]
    await asyncio.gather(*tasks)

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
    # with open("D:/node_addr/2-1.csv","r") as f3:
    #     addr_txhash_list = f3.readlines()
    #     _dir = os.path.join(root_path, "k=3")
    #     if not os.path.exists(_dir):
    #         os.makedirs(_dir)
    #     node_addr_file = open(os.path.join("D:/node_addr/", "3-1.csv"), "a", encoding="utf-8", newline='')
    #     for i in range(1248,(len(addr_txhash_list)//100)-1):    # 2024.9.22:124700-124800
    #         asyncio.run(main3(addr_txhash_list[i*100:(i+1)*100]))
    #         print(i*100,"-",(i+1)*100)
    #         node_addr_file.close()
    #         node_addr_file = open(os.path.join("D:/node_addr/", "3-1.csv"), "a", encoding="utf-8", newline='')
    #     node_addr_file.close()


    # # 第四级
    with open("D:/node_addr/3-1.csv","r") as f3:
        addr_txhash_list = f3.readlines()
        _dir = os.path.join(root_path, "k=4")
        if not os.path.exists(_dir):
            os.makedirs(_dir)
        node_addr_file = open(os.path.join("D:/node_addr/", "4-1.csv"), "a", encoding="utf-8", newline='')
        for i in range((len(addr_txhash_list)//100)-1):
            asyncio.run(main4(addr_txhash_list[i*100:(i+1)*100]))
            print(i*100,"-",(i+1)*100,"Finished")
            node_addr_file.close()
            node_addr_file = open(os.path.join("D:/node_addr/", "4-1.csv"), "a", encoding="utf-8", newline='')
        node_addr_file.close()