# Importing Necessary Libraries

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import numpy as np
import os
import zipfile
import requests

# Defining required fields: File locations and Year

BASE_URL = "https://www.ncei.noaa.gov/data/local-climatological-data/access/"
LINK_PARSE_LOCATION = "/opt/airflow/logs/artifacts/page_content.html"
FILE_STORE_LOCATION = "/opt/airflow/logs/artifacts/climate_data/"
ZIP_FILE_LOCATION = "/opt/airflow/logs/artifacts/climate_data_archive.zip"

REQUIRED_FILES = 10
YEAR = 2020


def fetch_file_links():
    """
    Function to parse the HTML file extracted using Bash operator from the archive URL and 
    it retrieves the links for the .csv files in the HTML file and returns
    the list of links to be downloaded.
    """
    with open(LINK_PARSE_LOCATION, "r") as f:
        html_content = f.read()

    soup = BeautifulSoup(html_content, 'html.parser')
    file_links = [link.get("href") for link in soup.find_all("a")]
    file_links = [link for link in file_links if link.endswith('.csv')]

    selected_files = np.random.choice(file_links, size=REQUIRED_FILES)

    return selected_files


def fetch_data():
    """
    Function to download the content from all the .csv files and
    stores the files in a folder that will be zipped in the next task.
    """
    selected_files = fetch_file_links()
    os.makedirs(FILE_STORE_LOCATION, exist_ok=True)

    for file in selected_files:
        file_path = f'{FILE_STORE_LOCATION}{file}'
        file_link = f'{BASE_URL}{YEAR}/{file}'

        response = requests.get(file_link)

        with open(file_path, "wb") as f:
            f.write(response.content)


def zip_files():
    """
    Function to zip the downloaded .csv files.
    """
    files = [os.path.join(FILE_STORE_LOCATION, file) for file in os.listdir(FILE_STORE_LOCATION)]

    with zipfile.ZipFile(ZIP_FILE_LOCATION, "w") as f:
        for file in files:
            f.write(file, os.path.basename(file))


# Defining the default arguments for DAG

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

# Defining the DAG

with DAG(
    'climate_data_fetch',
    default_args=default_args,
    schedule=None
) as dag:

    fetch_page_task = BashOperator(task_id='fetch_page_task', bash_command=f"curl -o {LINK_PARSE_LOCATION} {BASE_URL}{YEAR}/")
    fetch_data_task = PythonOperator(task_id='fetch_data_task', python_callable=fetch_data)
    zip_files_to_archive = PythonOperator(task_id='zip_files_to_archive', python_callable=zip_files)


# Defining the order of the tasks
fetch_page_task >> fetch_data_task >> zip_files_to_archive
