import json
import ProjectOverlayDataProcess as data
import pandas as pd
import utility_funcs as uf
import code
import numpy as np
"""
This is the big boy.
"""

def import_data():
    return (data.import_dataframe("relevantgroups"),
            data.import_dataframe("mandates"),
            data.import_dataframe("top_cos_groups"),
            data.import_dataframe("top_sim_groups")
            )

def prune_dataframe(df, columns, drop_allps=True):
    """
    Keeps only specified columns.
    Drops duplicates and missing data.
    """
    new_df = df.copy()
    if drop_allps:
        new_df = new_df.loc[new_df.content_audience != 'allps']
    return new_df[columns].dropna().drop_duplicates()

def process_sim_matrix(sim, fix_values=False):
    """
    Process the Similarity Matrix that was imported
    from import_data()

    Similarity matrices list the guid's of similar groups.
    Columns are group guids, values are the similar groups of the column group

    Process drops 'Unnamed' if exists and returns an array of objects
    where values are integers

    Key of object is the guid, and holds an array of similar groups.
    """
    try:
        sim.drop('Unnamed: 0',1, inplace=True)
    except:
        pass
    sim.columns = [uf.make_int_if_can(col) for col in sim.columns]

    if fix_values:
        sim = sim.applymap(uf.make_int_if_can)
    sim_json = sim.to_dict('list')
    return sim_json


def add_groups_item_to_dict(init_json, df, nestcol, newcolname):
    """
    init_json: Initial Json Array of Objects
    df: Dataframe to add group items
    nestcol: Name of columns that will be nested (thrown into a list)
    newcolname: Name of new column

    Matches on guid

    Adds a specific column (nestcol) with name (newcolname) from
    Pandas dataframe (df) to the array (init_json)
    """

    new_dict = uf.nest_for_json(
        prune_dataframe(df, ['guid', nestcol]),
        'guid',
        nestcol,
        newcolname
    ).to_dict('r')

    for init_item in init_json:
        for new_item in new_dict:
            # Matches on guid between the two items
            if init_item['guid'] == new_item['guid']:
                init_item[newcolname] = new_item[newcolname]


def construct_network_graph_dict(df, groupbycols, nestcol, nestedkeyname, drop_allps):
    """
    df: Dataframe that holds the data
    groupbycols: The single-value (non-array) values of a JSON Object
    nestcol: The data that will be put into an array
    nestedkeyname: New columns name

    Returns an array of JSON objects.
    Pythonically, it's a list of dictionaries (that contain lists)
    """

    new_df = uf.nest_for_json(
        prune_dataframe(df, groupbycols + [nestcol], drop_allps=drop_allps),
        groupbycols,
        nestcol,
        nestedkeyname
    )
    return new_df.to_dict('r')


def add_sim_groups_to_dict(init_json, sim_json):
    """
    init_json: List of dictionaries (future array of JSON objects)
    sim_json: similarities array of json objects

    Adds similar groups to a JSON object
    """
    for d in init_json:
        guid_match = int(d['guid'])
        if guid_match in sim_json.keys():
            # If the guid is in the similarities
            # object, then add it to the initial
            # json object
            d['similar_groups'] = sim_json[guid_match]


def nest_nodes(nodes_json, holder_json, nest_key='guid', keys='children_nodes'):
    """
    nodes_json: JSON object - Array of nodes
    holder_json: JSON object - Holder of the information you want to nest
    nest_key: the key to identify where to nest
    keys: Key to use
    """

    for item in holder_json:
        item['children'] = []
        list_of_children = item[keys]

        for node in nodes_json:
            node_key = node[nest_key]

            if node_key in list_of_children:
                item['children'].append(node)


def add_node_attributes(list_dict, project=False):
    """
    list_dict: List of Dictionaries / Array of JSON Objects
    Adds attributes to each object in the list

    """
    for item in list_dict:
        item['free'] = True
        if not project:
            item['project'] = False
        else:
            item['project'] = True


def floats_to_ints(list_dict):
    """
    HORRIFIC ABOMINATION OF A FUNCTION THAT DIGS
    TO A CERTAIN LEVEL TO CONVERT FLOATS TO INTEGERS
    """
    for thing in list_dict:
        for k, v in thing.items():
            if isinstance(v, float):
                thing[k] = int(v)
            elif isinstance(v, list):
                for counter, item in enumerate(v):
                    if isinstance(item, float):
                        v[counter] = int(item)
    return list_dict


def main():
    groups, mandates, top_cos_groups, top_sim_groups = import_data()

    communities_json = construct_network_graph_dict(groups,
            ['guid', 'name', 'description', 'size', 'owner_guid', 'group_time'],
             'content_tag',
             'parent_nodes'
             )

    add_node_attributes(communities_json, project=True)

# From here on, we add to this dictionary
    add_groups_item_to_dict(communities_json,
    groups,
    'content_audience',
    'communities')

    add_groups_item_to_dict(communities_json,
    groups,
    'content_guid',
    'content')

    communities_json = floats_to_ints(communities_json)

    sim_json = process_sim_matrix(top_sim_groups)

    add_sim_groups_to_dict(communities_json, sim_json)

    cos_dict = process_sim_matrix(top_cos_groups, fix_values=True)

    mandates_dict_list = []
    for item in mandates.Priority.unique():
        mandates_dict = {}
        mandates_dict['name'] = item
        mandates_dict['children_nodes'] = cos_dict[item]
        mandates_dict_list.append(mandates_dict)

    add_node_attributes(mandates_dict_list)
    content_tag_json = construct_network_graph_dict(groups, groupbycols=['content_tag'], nestcol='guid', nestedkeyname='children_nodes')
    add_node_attributes(content_tag_json)
    content_tag_json = floats_to_ints(content_tag_json)
    audience_json = construct_network_graph_dict(groups, groupbycols=['content_audience'], nestcol='content_tag', nestedkeyname='content_tags')
    add_node_attributes(audience_json)
    audience_json = floats_to_ints(audience_json)
    for item in audience_json:
        item['name'] = item.pop('content_audience')

    for item in content_tag_json:
        item['name'] = item.pop('content_tag')

    nest_nodes(nodes_json=communities_json, holder_json=content_tag_json, nest_key='guid',keys='children_nodes')
    nest_nodes(nodes_json=content_tag_json, holder_json=audience_json,nest_key='name', keys='content_tags')

    nest_nodes(nodes_json=communities_json, holder_json=mandates_dict_list, nest_key='guid', keys='children_nodes')


    final_communities = {
        "name":"Communities",
        "free":True,
        "project":False,
        "children":audience_json
    }

    final_issues = {
        "name":"Issues",
        "free":True,
        "project":False,
        "children":mandates_dict_list
    }
    mother_node = {
        "name":"TheMotherNode",
        "free":True,
        "project":False,
        "children":[final_communities, final_issues]
    }
    with open('test_mothernode.json', 'w') as outfile:
        json.dump(mother_node, outfile, indent=4, separators=(',',':'))


if __name__ == "__main__":
    
    main()

    code.interact(local=locals())



