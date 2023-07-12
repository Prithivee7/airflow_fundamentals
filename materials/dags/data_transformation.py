from airflow import DAG
from airflow.operators.python import PythonOperator
from features import voters
import pandas as pd
import requests
import zipfile
import os
from datetime import datetime
import numpy as np
import hopsworks


def get_voters_data_from_ncsbe(**kwargs):
    """
    This function reads data from the voter stats link and stores the raw data in a csv file.

    The Voter Stats link is accepted as argument during runtime and
    the zip file is unzipped and the data is read as a dataframe. 

    The list of the elections can be found here -> https://www.ncsbe.gov/results-data/voter-registration-data

    """

    voter_stats_link = kwargs['dag_run'].conf.get('voter_stats_link')
    path_to_zip_file = "voter_dataset.zip"
    response = requests.get(voter_stats_link, stream=True)
    with open(path_to_zip_file, "wb") as f:
        for chunk in response.iter_content(chunk_size=512):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)

    # Extracting the data in the zip file

    with zipfile.ZipFile(path_to_zip_file, 'r') as zip_ref:
        zip_ref.extractall("my_data")

    # Reading the dataframe
    file_name = os.listdir("my_data")[0]
    file_path = f"my_data/{file_name}"
    voters_df = pd.read_csv(file_path, delimiter=r"\t+")
    return file_path


def get_preprocessed_dataframe(**kwargs):
    """
        This function reads the rawdata and performs feature engineering and
          stores the resultant dataframe in a csv file.
        For each feature there is a specific function in the voters.py file in the features folder.
        All these functions are called to create a feature dataframe.

    """
    file_path = kwargs['ti'].xcom_pull(task_ids='get_raw_data_task')
    df = pd.read_csv(file_path, delimiter=r"\t+")

    df['Political Party'] = df['party_cd'].apply(
        voters.perform_binning_political_parties)
    df['County ID'] = df['county_desc'].apply(voters.get_county_id)
    df['Race'] = df['race_code'].apply(voters.perform_binning_races)
    df['Age Bracket'] = df['age'].apply(voters.get_age_bracket)
    df['Sex'] = df['sex_code'].apply(voters.perform_binning_sex)
    df['Ethnicity'] = df['ethnic_code'].apply(voters.perform_binning_ethnicity)
    df = df[['Political Party', "County ID", "Race",
             "Age Bracket", "Sex", "Ethnicity", "total_voters"]]

    cat_csv_location = "my_data/categorical_feature_df.csv"
    df.to_csv(cat_csv_location, index=False)
    return cat_csv_location


def perform_aggregation(**kwargs):
    """
        This function reads the  feature engineered data and
          writes the aggregated dataframe to a csv file.
        Aggregation is done based on the size of total_voters 

    """
    file_path = kwargs['ti'].xcom_pull(task_ids='get_preprocessed_df_task')
    df = pd.read_csv(file_path)
    agg_df = df.groupby([
        'Political Party', 'Race',
        'Age Bracket', 'Sex', "Ethnicity", "County ID"]).agg(
        Voter_Count=('total_voters', np.size)
    ).reset_index()
    agg_df['p_key'] = [i for i in range(1, len(agg_df)+1)]

    agg_csv_location = "my_data/aggregated_df.csv"
    agg_df.to_csv(agg_csv_location, index=False)
    return agg_csv_location


def update_column_names(**kwargs):
    """
        This function reads the agregated data
        and updates the name of the features 
        and stores the resultant data to a csv file.
        This function is called to comply with Hopsworks's naming convention specification.

    """
    file_path = kwargs['ti'].xcom_pull(task_ids='get_aggregated_df_task')
    df = pd.read_csv(file_path)
    df.columns = ['political_party', 'race', 'age_bracket', 'sex', 'ethnicity',
                  'county_id', 'voter_count', 'p_key']

    final_csv_location = "my_data/final_df.csv"
    df.to_csv(final_csv_location, index=False)
    return final_csv_location


def push_data_to_feature_store(**kwargs):
    """
    This function reads the feature store ready data 
    and pushes it into the feature store
    """
    file_path = kwargs['ti'].xcom_pull(task_ids='update_columns_task')
    df = pd.read_csv(file_path)

    # Loging to hopsworks
    hopsworks_project = hopsworks.login()
    fs = hopsworks_project.get_feature_store()

    voters_fg = fs.get_or_create_feature_group(
        name="voterdata",
        version=1,
        description="Voter data with categorical variables and aggregation",
        primary_key=['p_key'],
        online_enabled=True
    )

    voters_fg.insert(df)

    feature_descriptions = [
        {"name": "political_party",
            "description": "It bins the political party into the Democratic, Republic and Others"},
        {"name": "race", "description": "Contains information about the Race of the voter"},
        {"name": "age_bracket",
            "description": "Contains information about the age bracket to which the voter belongs"},
        {"name": "sex", "description": "Contains information regarding the sex of the voter"},
        {"name": "ethnicity",
            "description": "Contains information about the ethnicity of the voter"},
        {"name": "county_id",
            "description": "Contains information about the county id of the voter"},
        {"name": "voter_count",
            "description": "Contains information regarding the number of voters"},
        {"name": "p_key", "description": "This feature is used as a primary key"},
    ]

    for desciption in feature_descriptions:
        voters_fg.update_feature_description(
            desciption["name"], desciption["description"])


with DAG('data_transformation', start_date=datetime(2023, 1, 1), schedule_interval='@daily', catchup=False) as dag:

    get_raw_data_task = PythonOperator(
        task_id='get_raw_data_task',
        python_callable=get_voters_data_from_ncsbe,
        provide_context=True
    )

    get_preprocessed_df_task = PythonOperator(
        task_id='get_preprocessed_df_task',
        python_callable=get_preprocessed_dataframe,
    )

    get_aggregated_df_task = PythonOperator(
        task_id='get_aggregated_df_task',
        python_callable=perform_aggregation,
    )

    update_columns_task = PythonOperator(
        task_id='update_columns_task',
        python_callable=update_column_names,
    )

    push_to_fs_task = PythonOperator(
        task_id='push_to_fs_task',
        python_callable=push_data_to_feature_store,
    )

    get_raw_data_task >> get_preprocessed_df_task >> get_aggregated_df_task >> update_columns_task >> push_to_fs_task
