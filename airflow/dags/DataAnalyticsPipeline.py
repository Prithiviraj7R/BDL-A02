# Importing necessary libraries

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
import apache_beam as beam
import subprocess
import os
import imageio

from datetime import datetime, timedelta

# Defining required fields: File locations 

ZIP_FILE_LOCATION = "/opt/airflow/logs/artifacts/climate_data_archive.zip"
UNZIP_FOLDER_LOCATION = "/opt/airflow/logs/artifacts/unzipped_climate_data/"
ANALYTICS_BEAM_PIPELINE_FILE = "/opt/airflow/dags/AnalyticsBeamPipeline.py"
PLOT_BEAM_PIPELINE_FILE = "/opt/airflow/dags/PlotBeamPipeline.py"

OUTPUT_JSON_FILE = "/opt/airflow/logs/artifacts/weather_data.json"
MONTHLY_AVERAGES_FILE = '/opt/airflow/logs/artifacts/monthly_averages.json'
ZIP_FILE_LOCATION = "/opt/airflow/logs/artifacts/climate_data_archive.zip"
LINK_PARSE_LOCATION = "/opt/airflow/logs/artifacts/page_content.html"
FILE_STORE_LOCATION = "/opt/airflow/logs/artifacts/climate_data/"

PLOT_SAVE_LOCATION = '/opt/airflow/logs/templates/'
GIF_LOCATION = '/opt/airflow/logs/templates/gifs/'
REQUIRED_FIELDS = ['HourlyWindSpeed', 'HourlyDryBulbTemperature', 'HourlyDewPointTemperature', 'HourlyPressureChange']

os.makedirs(GIF_LOCATION, exist_ok=True)

def compile_gif():
    """
    Function to create gif from compilation of 12 months geo maps
    """
    for feature in REQUIRED_FIELDS:
        image_location = []
        gif_store_location = os.path.join(GIF_LOCATION, f'{feature}.gif')
        
        for month in range(1, 13):
            image_location.append(f'{PLOT_SAVE_LOCATION}{feature}_month_{month}.png')

        images = [imageio.imread(img) for img in image_location]
        imageio.mimsave(gif_store_location, images, duration=500)

# Defining the default arguments for DAG

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'schedule_interval': timedelta(minutes=1)
}

# Defining the DAG tasks

with DAG(
    'climate_data_analytics',
    default_args=default_args,
    schedule=None
) as dag:

    # task for checking the availability of the zip file at the given location
    wait_for_archive_task = FileSensor(
        task_id='wait_for_archive',
        fs_conn_id='fs_default',
        filepath=ZIP_FILE_LOCATION,
        timeout=5,
        mode='poke'
    )

    # task to unzip the files 
    unzip_file_task = BashOperator(
        task_id='unzip_file',
        bash_command=f'unzip -t {ZIP_FILE_LOCATION} && mkdir -p {UNZIP_FOLDER_LOCATION} && unzip -o {ZIP_FILE_LOCATION} -d {UNZIP_FOLDER_LOCATION}'
    )

    # task to extract the contents of the CSV and compute monthly averages for each field
    compute_averages_task = BeamRunPythonPipelineOperator(
        task_id='data_processing',
        py_file=ANALYTICS_BEAM_PIPELINE_FILE,
        pipeline_options={'runner': 'DirectRunner'},
    )

    # task to plot geo maps for the fields
    plot_geo_map_task = BeamRunPythonPipelineOperator(
        task_id='plot_geo_maps',
        py_file=PLOT_BEAM_PIPELINE_FILE,
        pipeline_options={'runner': 'DirectRunner'},
    )

    # task to create a gif file by compiling images
    create_gif_task = PythonOperator(
        task_id='create_gif',
        python_callable=compile_gif
    )

    # task to delete the files that are unused and not required for further downstream tasks
    delete_files_task = BashOperator(
        task_id='delete_csv_files', 
        bash_command=f'rm -r {FILE_STORE_LOCATION} && rm {LINK_PARSE_LOCATION} {ZIP_FILE_LOCATION}'
    )

    
# Defining the order of the tasks

wait_for_archive_task >> unzip_file_task >> compute_averages_task  >> plot_geo_map_task >> create_gif_task >> delete_files_task


