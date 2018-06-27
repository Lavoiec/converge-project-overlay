import pandas as pd
import gccollab
import json
from IdentifyingSimilarities import calculate_group_similarities
import code
from utility_funcs import remove_html, make_dataframe_from_similar_groups
import ToJson


"""
Purpose of this script is to present a modified prototype of the project
overlay.

The main purpose of the project overlay is to take in the data of thousands
of groups and try to output the most relevant
groups. This modification will instead only use the queried groups specified
from the beginning, and graph them according to
pre-specified categories.

This modification eliminates most of the processing necessary to perform the
script, and is designed to conform exactly to the front-end's
specifications.

The final output is a JSON object that follows a format similar to:

{
    PRINCIPAL NODE
    NODE_CHILDREN: [
        {
            SUSTAINABLE DEV. GOAL 1
            GOAL_CHILDREN: [
                GROUPS
            ]
        },

        {
            SUSTAINABLE DEV. GOAL 2
            GOAL_CHILDREN: [
                GROUPS
            ]
        },
        ....,
    ]
}

The goal of this script is to establish a connection to GCcollab,
Import the proper data, process it, and create this JSON object
"""


def connect_to_collab():
    """
    Helper function to streamline connection to the GCcollab database
    """
    engine, conn = gccollab.connect_to_database()
    session, Base = gccollab.create_session()

    return engine, conn, session, Base

engine, conn, session, Base = connect_to_collab()


def access_groups_info(group_list, session=session, Base=Base, conn=conn):
    """
    Inputs a list of groups, and return the exact groups table from the
    database containing only the groups specified in group_list

    group_list: List of group guids (integers)
    session: SQLAlchemy Session
    Base: SQLAlcehmy Base
    conn: SQAlchemy connection

    ex.
    df = access_groups_info([123,456,789])
    Returns 3 row dataframe with each row corresponding to the
    guids in the list.
    """
    groups_table = Base.classes.elgggroups_entity
    statement = (session.query(groups_table)
                        .filter(groups_table.guid.in_(group_list))
                        .statement)
    table = pd.read_sql(statement, conn)

    return table


def access_sustainable_dev_goals(group_list, session=session, Base=Base, conn=conn):
    """
    Accesses all the tags in the groups_list that correspond with the
    sustainable development goals found here:
    https://sustainabledevelopment.un.org/

    group_list: List of group guids (integers)
    session: SQLAlchemy Session
    Base: SQLAlcehmy Base
    conn: SQAlchemy connection

    Returns a DataFrame with 2 columns named
    group_guid and tags that correspond. There may be
    multiple rows per guid, if that group has more than
    one sustainable development goal.
    """
    sd_goals = [
        "No Poverty",
        "Zero Hunger",
        "Good Health and Well-being",
        "Quality Education",
        "Gender Equality",
        "Clean Water and Sanitation",
        "Affordable and Clean Energy",
        "Decent Work and Economic Growth",
        "Industry Innovation and Infrastructure",
        "Reduced Inequalities",
        "Sustainable Cities and Communities",
        "Responsible Consumption and Production",
        "Climate Action",
        "Life Below Water",
        "Life on Land",
        "Peace Justice and Strong Institutions",
        "Partnerships for the Goals"
    ]
    # Easier string matching if everything is standardized to lower
    # case
    sd_goals = [goal.lower() for goal in sd_goals]
    # Specifying query
    metadata_table = Base.classes.elggmetadata
    metastrings_table = Base.classes.elggmetastrings

    statement = (
        session.query(
            metadata_table.entity_guid,
            metastrings_table.string
        ).filter(metadata_table.entity_guid.in_(group_list))
         .filter(metadata_table.value_id == metastrings_table.id)
        # The id for "interests" in the GCcollab db
         .filter(metadata_table.name_id == 82)
         .statement
    )

    get_all = pd.read_sql(statement, conn)
    get_all.columns = ['group_guid', 'tags']

    # Filtering out the tags in a multiple step process
    # Standardizing the queried tags to be lower-case
    get_all['tags'] = get_all['tags'].apply(lambda x: x.lower())
    # Only taking groups with sustainable development goals
    get_all = get_all.loc[get_all['tags'].isin(sd_goals), :]
    # Puts them in all caps.
    get_all['tags'] = get_all['tags'].apply(lambda x: x.upper())

    return get_all


def get_group_members(group_list, session=session, Base=Base, conn=conn):
    """
    Gets a list of group members for groups in group_list

    group_list: List of group guids (integers)
    session: SQLAlchemy Session
    Base: SQLAlcehmy Base
    conn: SQAlchemy connection

    ex.
    df = get_group_members([123,456,789])
    Returns a dataframe with number of rows equal to the
    sum of the members of each group and 2 columns
    """
    groups_table = Base.classes.elgggroups_entity
    relationships_table = Base.classes.elggentity_relationships
    statement = (session.query(
                # The 3 columns in the table
                groups_table.guid,
                relationships_table.guid_one
                # Filter like one would see in SQL
            ).filter(groups_table.guid.in_(group_list))
             .filter(groups_table.guid == relationships_table.guid_two)
             .filter(relationships_table.relationship == 'member'))

    statement = statement.statement

    get_all = pd.read_sql(statement, conn)

    get_all.columns = [
        'group_guid',
        'user_guid'
    ]

    get_all = get_all.apply(gccollab.convert_if_time)
    return get_all


def decode_json(text, lang):
    """
    Dataframes are often (read: exclusively) published in JSON format,
    which makes things convenient for toggle in French on the website.
    For easy code, use the helper functions decode_json_en()
    or decode_json_fr

    This gives a function meant to be used with the apply() method
    text: String to be decoded
    lang: Lang as either "en" or "fr". Decides what to decode

    ex.
    df = pd.DataFrame(data)
    df[col].apply(decode_json, args=("en",))
    """

    try:
        new_text = json.loads(text)
        return new_text[lang]
    except KeyError:
        return text


def decode_json_en(text):
    """
    Helper function to invoke english decode_json()
    text: String to be decoded.
    """
    return decode_json(text, lang="en")


def decode_json_fr(text):
    """
    Helper function to invoke french decode_json()
    text: String to be decoded.
    """
    return decode_json(text, lang="fr")


def merge_group_data(merge_key, *args):
    """
    Joins all of the dataframes given to it by one common merge_key
    In this case, it is used to get 3 dataframes, however this can be easily
    modified.

    merge_key: String. Name of column all DataFrames must merge on
    args: DataFrames. The Dataframes will be joined together

    Returns a DataFrame.
    """

    # Check if dataframes all have KW
    for arg in args:
        if type(arg) is not pd.DataFrame:
            raise TypeError("All arguments must be dataframes")
        if merge_key not in arg.columns:
            raise KeyError("Merge key not in all dataframes")
    # Number of df's
    n_df = len(args)
    new_df = pd.merge(args[0], args[1], on=merge_key)

    for i in range(2, n_df):
        new_df = pd.merge(new_df, args[i], on=merge_key)

    return new_df


def extract_similar_groups(df, id_col, number_of_groups):
    """
    Calls make_dataframe_from_similar_groups() using the specified
    parameters.

    This function just does some of the cleaning up to ensure it
    fits neatly within the rest of the program

    df: Dataframe to call the function on
    id_col: String. The name of the column to use as group id's
    number_of_groups: Integer specifying how many groups to match.

    Returns a (transposed) dataframe with len(df) rows and
    (number_of_groups) columns
    """
    sim_df = make_dataframe_from_similar_groups(df, id_col, number_of_groups)
    sim_df = pd.melt(sim_df)
    sim_df.rename(
        columns={'variable': 'group_guid', 'value': 'similar_groups'},
        inplace=True
    )
    return sim_df

# List of the groups we are looking for.
group_list = [79508, 891115, 891112, 891109, 891105, 891100, 891096, 891093, 891078,
              891065, 891062, 891059, 891056, 891053, 891050, 891047, 891044,
              891041, 891038, 891034, 891031, 891028, 891024, 891021, 891018,
              891015, 891012, 891009, 891006, 891003, 891000, 890997, 890993,
              890990, 890987, 890984, 890981, 890978, 890975, 890972, 890968,
              890965, 890956, 890952, 890948, 890935]

f = access_groups_info(group_list)
f['name'] = f['name'].apply(decode_json_en)
f['description'] = f['description'].apply(decode_json_en).apply(remove_html)
f.rename(columns={'guid': 'group_guid'}, inplace=True)
group_memberships = get_group_members(group_list)
tags = access_sustainable_dev_goals(group_list)
# Calculates the similarities of members in the groups
similar_groups = calculate_group_similarities(
    df=group_memberships,
    groupbycol='group_guid',
    nestcol='user_guid',
    newcolname='members'
)

sim_groups = extract_similar_groups(similar_groups, 'group_guid', 5)
# Getting all of the data into one dataframe
groups_data = merge_group_data(
    'group_guid',
    f,
    group_memberships,
    sim_groups,
    tags
    )


json_dict = ToJson.construct_network_graph_dict(
    df=groups_data,
    groupbycols=['group_guid', 'name', 'description'],
    nestcol='similar_groups',
    nestedkeyname='similar_groups',
    drop_allps=False
)

# Provides a global variable of sustainable development goals
# should be equivalent to sd_goals from access_sustainable_dev_goals()
sustainable_development_goals = groups_data.tags.unique()
# The construct_network_graph_dict() function returns a list of dictionaries
# The next bunch of code filters groups corresponding to each sustainable
# development goal and creates a JSON object for each group in each goal
# Then each sustainable development goal is placed inside a MOTHERNODE(TM)
# completing a JSON object as pictured in the original docstring
graph_categories = []
for goal in sustainable_development_goals:
    # Only groups that pertain to goal
    temp_df = groups_data.loc[groups_data.tags == goal, :]
    # contstructs JSON object, and nests similar groups into an array
    graph_branches = ToJson.construct_network_graph_dict(
        df=temp_df,
        groupbycols=['group_guid', 'name', 'description'],
        nestcol='similar_groups',
        nestedkeyname='similar_groups',
        drop_allps=False
        )
    for branch in graph_branches:
        # Converts all of the numbers in each branch (group) into integers
        # This is purely so it can be published to JSON
        branch['group_guid'] = int(branch['group_guid'])
        branch['similar_groups'] = [int(guid) for guid in branch['similar_groups']]
    # Adds the "free" and "project" attributes
    ToJson.add_node_attributes(graph_branches, project=True)
    # Creates a JSON object for each sustainable development goal
    graph_category = {
        "name": goal.upper(),
        "free": True,
        "project": False,
        "children": graph_branches
    }
    # Makes a list of each sustainable development goal object
    graph_categories.append(graph_category)
# T H E M O T H E R N O D E
mother_node = {
    "name": "TheMotherNode",
    "free": True,
    "project": False,
    "children": graph_categories
}

with open('converge_vis.json', 'w') as outfile:
    json.dump(mother_node, outfile, indent=4, separators=(',', ':'))
