"""
Data importing process.
Gathers necessary data for all of the crunching
"""
import gcconnex as gc
import pandas as pd
import utility_funcs as uf
import code

def import_dataframe(filename):

    choices = {"groups":"group_content_tags.csv",
               "similarities":"similarity_matrix.csv",
               "mandates": "government_projects.csv",
               "cosine_similarities": "cosine_similarities.csv",
               "top_cos_groups":"top_cos_groups.csv",
               "top_sim_groups":"top_sim_groups.csv",
               "relevantgroups": "relevantgroups.csv"
               }
    try:
        path = choices[filename]
    except KeyError:
        path = filename

    return pd.read_csv(path)


def process_groups_for_vsm(groups, description_min):
    """
    vsm: Vector Space Model
     imports the main dataframe (group_content_tags)
     and performs operations to get it ready for the VSM
    """

    groups = groups[['guid', 'name', 'description']].drop_duplicates().dropna()
    groups = groups.applymap(lambda x: uf.try_loading_json(x, 'en'))

    groups['name'] = groups.name.apply(uf.remove_html)
    groups['description'] = groups.description.apply(uf.remove_html)

    groups = groups.loc[(groups.name != '') & (groups.description != '')]
    groups = groups.loc[~((groups.name == None) & (groups.description == None))]

    groups.description = groups.description.apply(uf.try_splitting_strings).apply(uf.remove_punctuation).apply(uf.remove_stop_words)

    groups = uf.keep_only_lists(groups, 'description')
    groups = uf.drop_groups_by_description_len(groups, 'description', description_min)

    return groups

def get_group_properties(conn):

    temp_groups = import_dataframe("groups")
    group_sizes = gc.groups.get_group_sizes().reset_index().rename(columns={"guid_one":"size"})

    groups = pd.merge(temp_groups, group_sizes)

    return groups

def get_top_contributors(conn, n):

    content = gc.entities.filter_('container_guid IN (SELECT guid FROM elgggroups_entity) AND subtype != 3')
    top_contributors = content.groupby(['container_guid', 'owner_guid']).count()['guid'].reset_index()
    top_contributors.columns = ['guid' , 'user', 'contributions']

    top_contributors = top_contributors.sort_values(['guid', 'contributions'], ascending=[1,0]).groupby('guid').head(n)

    return top_contributors

def get_group_membership(conn=None):
    if not conn:
        engine, conn = gc.connect_to_database()
        session, Base = gc.create_session()

    members = gc.groups.get_membership()
    return members

