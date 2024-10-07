import json
import os
import shutil
import pandas as pd


def copy(k,root_addr):
    # copy file from D:/json_data/k=k/addr.json to D:/rebuild_dir/root_addr/k=k/addr.json
    if k==1:
    # copy file from D:/json_data/k=1/root_addr.json to D:/rebuild_dir/root_addr/k=1/root_addr.json
        if not os.path.exists(dst+"{}/k=1/".format(root_addr)):
            os.makedirs(dst+"{}/k=1/".format(root_addr))
        if os.path.exists("F:/json_data/k=1/{}.json".format(root_addr)) and not os.path.exists(dst+"{0}/k=1/{0}.json".format(root_addr)):
            shutil.copyfile("F:/json_data/k=1/{}.json".format(root_addr),dst+"{0}/k=1/{0}.json".format(root_addr))
    else:
        if not os.path.exists(dst+"{}/k={}/".format(root_addr,k)):
            os.makedirs(dst+"{}/k={}/".format(root_addr,k))
        former_path = dst+"{}/k={}/".format(root_addr,k-1)
        file_list = os.listdir(former_path)
        for json_file_name in file_list:
            addr_list = []
            with open(os.path.join(former_path,json_file_name),"r") as file:
                json_file = json.load(file)
                for tx in json_file["data"][0]["txs"]:
                    for inputs in tx["inputs"]:
                        if "address" in inputs:
                            addr_list.append(inputs["address"])
                    for outputs in tx["outputs"]:
                        if 'address' in outputs:
                            addr_list.append(outputs['address'])
            for addr in addr_list:
                if os.path.exists("F:/json_data/k={}/{}.json".format(k,addr)) and not os.path.exists(dst+"{}/k={}/{}.json".format(root_addr,k,addr)):
                    shutil.copyfile("F:/json_data/k={}/{}.json".format(k,addr),dst+"{}/k={}/{}.json".format(root_addr,k,addr))





if __name__=="__main__":
    company_list = ['CloudBet.com','CoinGaming.io']
    for company in company_list:
        root_addr_file_path = "F:/raw_addr_csv/" + company + '/'
        addr_file_list = os.listdir(root_addr_file_path)
        dst = "F:/rebuild_dir/" + company + '/'
        for file_name in addr_file_list:
            with open(os.path.join(root_addr_file_path,file_name),"r") as f:
                df = pd.read_csv(f,skiprows=1)
                for addr in df["address"]:
                    if not os.path.exists(os.path.join(dst,addr)):
                        print("Dealing address:{}".format(addr))
                        copy(1,addr)
                        copy(2,addr)
                        copy(3,addr)
                        copy(4,addr)

