import utility_funcs as uf
import ProjectOverlayDataProcess as data
import pandas as pd
import numpy as np
import code

number_of_groups=5

def import_data(only_relevant_groups=True):
    if only_relevant_groups:
        members = data.get_group_membership()
        relevantgroups = data.import_dataframe("relevantgroups")
        cosine_similarities = data.import_dataframe("cosine_similarities")

        members = members.loc[members.group_guid.isin(relevantgroups.guid),:]

        return members, cosine_similarities
    
    else:
        return data.get_group_membership(), data.import_dataframe("cosine_similarities")

def calculate_group_similarities(df, groupbycol, nestcol, newcolname):

    newdf = uf.nest_for_json(df, groupbycol=groupbycol,
                             nestcol=nestcol,
                             newcolname=newcolname)

    length = len(newdf)

    similarity_matrix = np.zeros((length, length))
    for i in range(length):
        for j in range(length):
            similarity_matrix[i, j] = uf.list_similarites(newdf[newcolname][i],newdf[newcolname][j])
    
    similarity_df = pd.DataFrame(similarity_matrix)
    similarity_df.columns = newdf[groupbycol]
    similarity_df[groupbycol] = newdf[groupbycol]

    return similarity_df

def main():
    group_memberships, cosine_groups = import_data(only_relevant_groups=True)


    similar_groups = calculate_group_similarities(df=group_memberships,
                                              groupbycol='group_guid',
                                              nestcol='user_guid',
                                              newcolname='members'
                                              )

    top_sim_groups = uf.make_dataframe_from_similar_groups(similar_groups, id_col='group_guid',n=number_of_groups)
    top_cos_groups = uf.make_dataframe_from_similar_groups(cosine_groups, id_col='Unnamed: 0',n=number_of_groups)
    top_cos_groups.to_csv("top_cos_groups.csv")
    top_sim_groups.to_csv("top_sim_groups.csv")


if __name__ == '__main__':
    main()
    code.interact(local=locals())