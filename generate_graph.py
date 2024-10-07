import networkx as nx
import json
import os



def Add_tx_node(graph, hash, input_count,output_count, block_height, timestamp, fee):
    graph.add_node(hash, input_count=input_count, output_count=output_count,
                   height=block_height, time=timestamp, fee=fee)


def Add_addr_node(graph, addr, tx_count, balence):
    graph.add_node(addr, tx_count=tx_count, balence=balence)


def Add_input_edge(graph, addr_node, tx_node, value, index):
    graph.add_edge(addr_node, tx_node, in_value=value,index=index)

def Add_output_edge(graph, tx_node,addr_node, value, index):
    graph.add_edge(tx_node, addr_node, out_value=value, index=index)


def deal_file(graph, file_name):
    # print("Dealing file:{}".format(file_name))
    json_file = open(file_name,"r")
    json_data = json.loads(json_file.read())["data"][0]
    Add_addr_node(graph, json_data["hash"], json_data["txCount"],
                  float(json_data["receive"])+float(json_data["spend"]))
    # What if coinbase?
    try:
        for tx in json_data["txs"]:
            tx_hash = tx["txid"]
            height = tx["height"]
            timestamp = tx["time"]
            fee = float(tx["fee"])
            input_count = tx["inputCnt"]
            output_count = tx["outputCnt"]
            Add_tx_node(graph,tx_hash,input_count,output_count,
                        height,timestamp,fee)
            for inputs in tx["inputs"]:
                input_no = inputs["input_no"]
                input_addr = inputs["address"]
                in_value = float(inputs["value"])
                Add_input_edge(graph,input_addr,tx_hash,in_value,input_no)

            for outputs in tx["outputs"]:
                output_no = outputs["output_no"]
                output_addr = outputs["address"]
                out_value = float(outputs["value"])
                Add_output_edge(graph,tx_hash,output_addr,out_value,output_no)
    except Exception as e:
        print(e)
    json_file.close()

def traverse(graph, rootpath):
    folder_list = os.listdir(rootpath)
    for folder in folder_list:
        folder_name = os.path.join(rootpath,folder)
        file_list = os.listdir(folder_name)
        for file in file_list:
            file_name = os.path.join(folder_name,file)
            deal_file(graph, file_name=file_name)


def combine_two_graphs(Graph_a, Graph_b):
    new_graph = nx.compose(Graph_a, Graph_b)
    return new_graph


def save_graph(graph,save_path):
    nx.write_gml(graph,save_path)



if __name__=="__main__":
    # traverse(Graph,"D:/json_data/")
    if not os.path.exists("D:/Graph/"):
        os.makedirs("D:/Graph/")
    folder_path = "D:/json_data/k=3/"
    file_list = os.listdir(folder_path)
    length = len(file_list)
    for i in range(0,length//1000+1,2):
        print("dealing index from {} to {}".format(i*1000,(i+2)*1000))
        Graph_A = nx.MultiDiGraph()
        Graph_B = nx.MultiDiGraph()
        Graph = nx.MultiDiGraph()
        if (i+2)*1000<length:
            for j in range(1000):
                deal_file(Graph_A, os.path.join(folder_path,file_list[i*1000+j]))
                deal_file(Graph_B, os.path.join(folder_path,file_list[(i+1)*1000+j]))
            Graph = combine_two_graphs(Graph_A, Graph_B)
            save_graph(Graph, os.path.join("D:/Graph/","k=3_{}-{}.gml".format(i*1000,(i+2)*1000)))
            Graph_A.clear()
            Graph_B.clear()
            Graph.clear()
        else:
            for rest_file in file_list[i*1000:]:
                deal_file(Graph_A, os.path.join(folder_path,rest_file))
            save_graph(Graph_A, os.path.join("D:/Graph/","k=3_{}-{}.gml".format(i*1000,length)))
            Graph_A.clear()








