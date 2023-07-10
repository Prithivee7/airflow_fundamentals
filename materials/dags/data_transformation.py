from airflow import DAG
from airflow.operators.python import PythonOperator
from features import voters
import pandas as pd
import requests
import zipfile
import os
from datetime import datetime
default_args = {
    'start_date': datetime(2023, 1, 1)
    # "voter_stats_link" : "https://s3.amazonaws.com/dl.ncsbe.gov/ENRS/2022_12_06/voter_stats_20221206.zip"
}


def print_the_link(**kwargs):
    print("Printing keyword arguments",kwargs)
    my_string = kwargs['dag_run'].conf.get('voter_stats_link')
    print(my_string)

def get_voters_data_from_ncsbe():
    
    """
    This function accepts the Registered Voter Stats file by Election Date link as parameter
    and outputs the data as a dataframe. 
    The list of the elections can be found here -> https://www.ncsbe.gov/results-data/voter-registration-data

    Returns:
        pd.Dataframe: Dataframe with Voter details(Raw).

    """
    
    voter_stats_link = "https://s3.amazonaws.com/dl.ncsbe.gov/ENRS/2022_12_06/voter_stats_20221206.zip"
    
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
    voters_df = pd.read_csv(file_path,delimiter=r"\t+")
    
    # Performing cleanup operation
    os.remove(path_to_zip_file)
    os.remove(file_path)
    os.rmdir("my_data")
    return voters_df

def get_final_dataframe(df):
    """
        This function takes in the voters dataframe and gives back the features.
        For each feature there is a specific function in the voters.py file in the features folder.
        All these functions are called to create a feature dataframe.
        
        Args:
            df (pd.Dataframe): Dataframe which contains the voter information(Raw)

        Returns:
            pd.Dataframe: Dataframe with Required Features.

    """
    
    df['Political Party'] = df['party_cd'].apply(voters.perform_binning_political_parties)
    df['County ID'] = df['county_desc'].apply(voters.get_county_id)
    df['Race'] = df['race_code'].apply(voters.perform_binning_races)
    df['Age Bracket'] = df['age'].apply(voters.get_age_bracket)
    df['Sex'] = df['sex_code'].apply(voters.perform_binning_sex)
    df['Ethnicity'] = df['ethnic_code'].apply(voters.perform_binning_ethnicity)
    df = df[['Political Party', "County ID", "Race",
            "Age Bracket", "Sex", "Ethnicity", "total_voters"]]
    return df


with DAG('data_transformation', default_args=default_args, start_date=datetime(2023, 1, 1), schedule_interval='@daily',catchup=False) as dag:
    
    print_link_task = PythonOperator(
        task_id='print_link_task',
        python_callable=print_the_link,
        provide_context=True
    )

    print_link_task

    # get_raw_data_task = PythonOperator(
    #     task_id='read_csv_from_github',
    #     python_callable=get_voters_data_from_ncsbe,
    # )

    # get_raw_data_task >> write_csv_task

