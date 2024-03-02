# Importing necessary libraries

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
import apache_beam as beam

from datetime import datetime, timedelta

# Defining required fields: File locations 

ZIP_FILE_LOCATION = "/opt/airflow/logs/artifacts/climate_data_archive.zip"
UNZIP_FOLDER_LOCATION = "/opt/airflow/logs/artifacts/unzipped_climate_data/"
EXTRACT_BEAM_PIPELINE_FILE = "/opt/airflow/dags/BeamPipeline.py"
COMPUTE_BEAM_PIPELINE_FILE = "/opt/airflow/dags/BeamPipeline2.py"
PLOT_BEAM_PIPELINE_FILE = "/opt/airflow/dags/BeamPipeline3.py"

OUTPUT_JSON_FILE = "/opt/airflow/logs/artifacts/weather_data.json"
MONTHLY_AVERAGES_FILE = '/opt/airflow/logs/artifacts/monthly_averages.json'
ZIP_FILE_LOCATION = "/opt/airflow/logs/artifacts/climate_data_archive.zip"

PLOT_SAVE_LOCATION = '/opt/airflow/logs/templates/'
GIF_LOCATION = '/opt/airflow/logs/templates/compilation.gif'
REQUIRED_FIELDS = ['HourlyWindSpeed', 'HourlyDryBulbTemperature']

bash_create_gif = ''
for feature in REQUIRED_FIELDS:
    bash_create_gif += f'{PLOT_SAVE_LOCATION}{feature}.png '

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
        mode='poke'
    )
    unzip_file_task = BashOperator(
        task_id='unzip_file',
        bash_command=f'unzip -t {ZIP_FILE_LOCATION} && mkdir -p {UNZIP_FOLDER_LOCATION} && unzip -o {ZIP_FILE_LOCATION} -d {UNZIP_FOLDER_LOCATION}'
    )
    extract_content_beam_task = BeamRunPythonPipelineOperator(
        task_id='extract_content_to_tuple',
        py_file=EXTRACT_BEAM_PIPELINE_FILE,
        pipeline_options={'runner': 'DirectRunner'},
    )
    compute_averages_task = BeamRunPythonPipelineOperator(
        task_id='compute_monthly_averages',
        py_file=COMPUTE_BEAM_PIPELINE_FILE,
        pipeline_options={'runner': 'DirectRunner'},
    )
    plot_geo_map_task = BeamRunPythonPipelineOperator(
        task_id='plot_geo_maps',
        py_file=PLOT_BEAM_PIPELINE_FILE,
        pipeline_options={'runner': 'DirectRunner'},
    )
    delete_files_task = BashOperator(
        task_id='delete_csv_files', 
        bash_command=f'rm {OUTPUT_JSON_FILE} {ZIP_FILE_LOCATION}'
    )
    create_gif_task = BashOperator(
        task_id='create_gif',
        bash_command=f'convert -delay 100 -loop 0 {bash_create_gif}{GIF_LOCATION}'
    )


# Defining the order of the tasks

wait_for_archive_task >> unzip_file_task >> extract_content_beam_task >> compute_averages_task >> plot_geo_map_task >> delete_files_task >> create_gif_task