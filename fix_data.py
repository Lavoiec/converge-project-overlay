
# coding: utf-8

# Generate node IDs
# ------------------
# Uniquely ID each node in the tree in order to allow nodes to be loaded dynamically accounting for duplicates

import json
import pprint

tree = json.loads(open('converge_vis.json').read())
tree


def addIDs(root):
    curr_id = [0] # Increment this value to assign unique IDs to nodes
    print(curr_id)
    def addID(node):
        node['token'] = curr_id[0]
        curr_id[0] += 1
        if node['project'] == True:
            return
        for child in node['children']:
            if child['name'] == 'allps':
               del child
               continue
            addID(child)
    addID(root)

addIDs(tree)


with open('tree.json', 'w') as f:
     json.dump(tree, f, indent=4, separators=(',', ':'))

f.close()