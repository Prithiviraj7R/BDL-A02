# Importing necessary libraries

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
# from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
import apache_beam as beam
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner

from datetime import datetime, timedelta

# Defining required fields: File locations 

ZIP_FILE_LOCATION = "/opt/airflow/logs/artifacts/climate_data_archive.zip"
UNZIP_FOLDER_LOCATION = "/opt/airflow/logs/artifacts/unzipped_climate_data/"
BEAM_PIPELINE_FILE = "/opt/airflow/dags/BeamPipeline.py"

# Defining the default arguments for DAG

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'climate_data_analytics',
    default_args=default_args,
    schedule=None
) as dag:

    wait_for_archive_task = FileSensor(
        task_id='wait_for_archive',
        fs_conn_id='fs_default',
        filepath=ZIP_FILE_LOCATION,
        timeout=5,
        poke_inteval=1,
        mode='poke'
    )
    unzip_file_task = BashOperator(
        task_id='unzip_file',
        bash_command=f'unzip -t {ZIP_FILE_LOCATION} && mkdir -p {UNZIP_FOLDER_LOCATION} && unzip -o {ZIP_FILE_LOCATION} -d {UNZIP_FOLDER_LOCATION}'
    )
    extract_content_beam_task = BeamRunPythonPipelineOperator(
        task_id='extract_content_to_tuple',
        py_file=BEAM_PIPELINE_FILE,
        pipeline_options={'runner': 'DirectRunner'},
    )


# Defining the order of the tasks
wait_for_archive_task >> unzip_file_task >> extract_content_beam_task