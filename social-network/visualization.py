import networkx as nx
import numpy as np
import matplotlib.pyplot as plt
import sys
import csv


G = nx.DiGraph()

node_sizes = []
labels = []
node_color = []

with open('nodescsvfile.csv', 'rU') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        G.add_node(row['id'])
        node_sizes.append(10000*float(row['if'])+10)
        labels.append(row['id'])

        if int(row['id']) < 110:
            node_color.append('blue')
        if int(row['id']) > 110 and int(row['id']) < 268:
            node_color.append('red')
        if int(row['id']) > 268:
            node_color.append('green')



with open('edgecsvfile.csv', 'rU') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        G.add_edge(row['to'], row['from'])



pos = nx.spring_layout(G)
nx.draw_networkx_nodes(G, pos, cmap=plt.get_cmap('jet'), node_color = node_color, node_size = node_sizes, labels=labels, with_labels=True)


nx.draw_networkx_edges(G, pos, node_size=node_sizes, labels=labels, with_labels=True, arrows=False)

plt.show()