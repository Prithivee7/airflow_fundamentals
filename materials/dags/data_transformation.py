from airflow import DAG
from airflow.operators.python import PythonOperator
from features import voters
import pandas as pd
import requests
import zipfile
import os
from datetime import datetime


def get_voters_data_from_ncsbe(**kwargs):
    """
    This function reads data from the voter stats link and returns the raw data.

    The Voter Stats link is accepted as argument during runtime and
    the zip file is unzipped and the data is read as a dataframe. 

    The list of the elections can be found here -> https://www.ncsbe.gov/results-data/voter-registration-data

    Returns:
        pd.Dataframe: Dataframe with Voter details(Raw).

    """

    voter_stats_link = kwargs['dag_run'].conf.get('voter_stats_link')
    print('printing link', voter_stats_link)
    print("type", type(voter_stats_link))
    path_to_zip_file = "voter_dataset.zip"
    response = requests.get(voter_stats_link, stream=True)
    print("Status code", response.status_code)
    with open(path_to_zip_file, "wb") as f:
        for chunk in response.iter_content(chunk_size=512):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)

    print("Printing contents of the directory")
    print(os.listdir())
    # Extracting the data in the zip file

    with zipfile.ZipFile(path_to_zip_file, 'r') as zip_ref:
        zip_ref.extractall("my_data")

    # Reading the dataframe
    file_name = os.listdir("my_data")[0]
    file_path = f"my_data/{file_name}"
    voters_df = pd.read_csv(file_path, delimiter=r"\t+")

    print("Printing raw df columns", voters_df.columns)
    print("Printing raw lenght of df", len(voters_df))
    print(voters_df.head())

    # Performing cleanup operation
    os.remove(path_to_zip_file)
    os.remove(file_path)
    os.rmdir("my_data")
    return voters_df


def get_final_dataframe(**kwargs):
    """
        This function takes in the voters dataframe and gives back the features.
        For each feature there is a specific function in the voters.py file in the features folder.
        All these functions are called to create a feature dataframe.

        Args:
            df (pd.Dataframe): Dataframe which contains the voter information(Raw)

        Returns:
            pd.Dataframe: Dataframe with Required Features.

    """
    df = kwargs['ti'].xcom_pull(task_ids='get_raw_data_task')

    df['Political Party'] = df['party_cd'].apply(
        voters.perform_binning_political_parties)
    df['County ID'] = df['county_desc'].apply(voters.get_county_id)
    df['Race'] = df['race_code'].apply(voters.perform_binning_races)
    df['Age Bracket'] = df['age'].apply(voters.get_age_bracket)
    df['Sex'] = df['sex_code'].apply(voters.perform_binning_sex)
    df['Ethnicity'] = df['ethnic_code'].apply(voters.perform_binning_ethnicity)
    df = df[['Political Party', "County ID", "Race",
             "Age Bracket", "Sex", "Ethnicity", "total_voters"]]
    print("Printing df columns", df.columns)
    print("Printing lenght of df", len(df))
    print(df.head())
    return df


with DAG('data_transformation', start_date=datetime(2023, 1, 1), schedule_interval='@daily', catchup=False) as dag:

    get_raw_data_task = PythonOperator(
        task_id='get_raw_data_task',
        python_callable=get_voters_data_from_ncsbe,
        provide_context=True
    )

    get_final_df_task = PythonOperator(
        task_id='get_final_df_task',
        python_callable=get_final_dataframe,
    )

    get_raw_data_task >> get_final_df_task
