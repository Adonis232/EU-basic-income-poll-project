




import pandas as pd

# wrangling functions

def gender_modifier(x):
    if x == 'FeMale':
        return 'Female'
    elif x == 'Fem':
        return 'Female'
    elif x == 'female':
        return 'Female'
    elif x == 'male':
        return 'Male'
    elif x == 'Male':
        return 'Male'

def children_modifier(x):
    if x == 'NO':
        return 'no'
    elif x == 'nO':
        return 'no'
    elif x == 'No':
        return 'no'
    elif x == 'YES':
        return 'yes'
    elif x == 'yES':
        return 'yes'


def wrangle():

    print('Loading DataFrames...')
    df_personal = pd.read_csv('./raw_data/df_personal.csv')
    df_country = pd.read_csv('./raw_data/df_country.csv')
    df_career = pd.read_csv('./raw_data/df_career.csv')
    df_poll = pd.read_csv('./raw_data/df_poll.csv')
    country_codes_scrapped = pd.read_csv('./raw_data/country_codes_scrapped.csv')
    jobs_dataframe = pd.read_csv('./raw_data/jobs_dataframe.csv')
    print('Merging all data frames...')

    # appliying my definition to clean gender and dem_has_children columns
    df_personal['gender_modified'] = df_personal['gender'].apply(gender_modifier)
    df_personal['dem_has_children_mo'] = df_personal['dem_has_children'].apply(children_modifier)

    # from df raw, dropping the old and unclean 'gender' and dem_has_children columns
    df_personal_clean = df_personal.drop(columns=['gender', 'dem_has_children'])


    countries_merged = pd.merge(country_codes_scrapped, df_country, how='right', on='country_code')
    countries_merged.to_csv('./raw_data/country_info_merged.csv', index=False)

    full_personal = pd.merge(df_personal, countries_merged, how='left', on='uuid')

    merged_interview = pd.merge(full_personal, df_poll, how='left', on='uuid')
    full_poll = pd.merge(merged_interview, df_career, how='left', on='uuid')
    final_df = pd.merge(full_poll, jobs_dataframe, how='left', on='normalized_job_code')

    print('Changing new column names...')
    df_modified = final_df.drop(columns=['gender', 'dem_has_children'])
    full_set = df_modified.rename(columns={'gender_modified': 'gender', 'dem_has_children_mo': 'dem_has_children'})
    full_set.to_csv('./raw_data/full_set.csv', index=False)
