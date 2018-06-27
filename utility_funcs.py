import json
import pandas as pd
from bs4 import BeautifulSoup
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize


def try_loading_json(x, lang):
    """
    Meant to be created as a new column using apply
    attempts to load a json string unless it isn't one
    then returns none
    
    Json strings are stored with languages as 'en' or 'fr'
    allows for french and english text to be separated
    """
    try:
        x = x.replace("'", "")
        x = json.loads(x)
        x = x[lang]
    except Exception as e:
        pass
        print(e)

    return x


def remove_html(x):
    try:
        return BeautifulSoup(x, 'lxml').text
    except:
        return
 
stop_words = stopwords.words('english')


def remove_stop_words(x):
    try:
        if not isinstance(x, list):
            raise ValueError('This needs to be a list')
        else:
            new_list = [word.lower() for word in x if word.lower() not in stop_words]
            return new_list
    except:
        return


def try_splitting_strings(x):
    try:
        return word_tokenize(x)
    except:
        return


def remove_punctuation(s):
    try:
        return [word for word in s if word.isalpha()]
    except:
        pass


def drop_groups_by_description_len(df, col, n, is_list=False):
    tempdf = df.copy()

    def calculate_length(x):
        """
        If words are already as a list, calculate length of list
        If in string format. Calculate the length of it was in a list
        """
        if isinstance(x, list):
            return len(x)
        elif isinstance(x, str):
            return len((' ').split(x))

    tempdf['list_length'] = tempdf[col].apply(calculate_length)
    return tempdf.loc[tempdf['list_length'] > n, tempdf.columns != 'list_length']


def keep_only_lists(df, col):
    tempdf = df.copy()
    tempdf['islist'] = tempdf[col].apply(type) == list

    return tempdf.loc[tempdf['islist'] == True]


def list_similarites(list1, list2):
    return 1 - len(list(set(list1) - set(list2)))/len(list1)


def convert_list_to_string(x):
    if isinstance(x, list):
        return (' ').join(x)
    else:
        return x


def text_cleaning_pipe(df, cols=None):

    new_df = df.copy()
    if not cols:
        new_df = (new_df.applymap(convert_list_to_string)
                        .applymap(lambda x: try_loading_json(x, 'en')))
    else:
        new_df[cols] = (new_df[cols].applymap(convert_list_to_string)
                                    .applymap(lambda x: try_loading_json(x, 'en')))

    return new_df


def make_dataframe_from_similar_groups(df, id_col, n):
    dict_of_groups = {}

    for col in df.columns:
        if col != id_col:
            dict_of_groups[col] = list(
                df[pd.to_numeric(df[id_col], errors='coerce').notnull()]
                .set_index(id_col)
                [col]
                .sort_values(ascending=False)
                .head(n)
                .index)

    return pd.DataFrame(dict_of_groups)


def nest_for_json(df, groupbycol, nestcol, newcolname):
    """
    df: Dataframe
    groupbycol: The column(s) that will stay as non-lists
    nestcol: The name of the column that will be nested into a list
    inside a column
    newcolname: The name of the new column

    Returns a pandas DataFrame
    """

    new_df = (df.groupby(groupbycol)
              .apply(lambda x: x[nestcol].values)
              .reset_index()
              #.drop_duplicates()
              .rename(columns={0: newcolname}))

    new_df[newcolname] = new_df[newcolname].apply(lambda x: list(set(x)))

    return new_df


def returning_top_tags(df, n=10):
    return(df
            .groupby(['content_audience', 'content_tag'], sort=True)
            .count()
            .reset_index()
            .groupby(['content_audience'])
            .apply(lambda x: x.nlargest(n, 'guid'))
            [['content_tag', 'guid']]
            .reset_index()
            .drop('level_1', 1)
            .rename(columns={'guid': 'count'}))


def make_int_if_can(x):
    try:
        return int(float(x))
    except ValueError:
        return(x)