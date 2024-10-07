import networkx as nx
import os
import re



def load_graph(file_path):
    return nx.read_gml(file_path)


def combine_two_graphs(graph_a, graph_b):
    new_graph = nx.compose(graph_a, graph_b)
    return new_graph

def save_graph(graph,save_path):
    nx.write_gml(graph,save_path)


k = "3"
folder_path = "D:/Graph/k=" + k + "/"
file_lisit = os.listdir(folder_path)
if __name__=="__main__":
    length = len(file_lisit)
    Graph = nx.MultiDiGraph()
    while length>1:
        for i in range(0,length,2):
            if (i+2)<=length:
                A_path = os.path.join(folder_path,file_lisit[i])
                Graph_A = load_graph(A_path)
                B_path = os.path.join(folder_path,file_lisit[i+1])
                Graph_B = load_graph(B_path)
                start, end = re.search(r"_(\d+)-\d+",file_lisit[i]).group(0),re.search(r"_\d+-(\d+)",file_lisit[i+1]).group(0)
                Graph = combine_two_graphs(Graph_A,Graph_B)
                save_graph(Graph,os.path.join(folder_path,"k={}_{}-{}".format(k,start,end)))
                os.remove(A_path)
                os.remove(B_path)
                Graph.clear()
            else:
                file_list = os.listdir(folder_path)
                length = len(file_list)
                continue


