import apache_beam as beam

import os
import pandas as pd

# Defining required fields: File locations 

UNZIP_FOLDER_LOCATION = "/opt/airflow/logs/artifacts/unzipped_climate_data/"
OUTPUT_TEXT_FILE = "/opt/airflow/logs/artifacts/weather_data.txt"
REQUIRED_FIELDS = ['DATE', 'HourlyDewPointTemperature', 'HourlyWetBulbTemperature', 'HourlyRelativeHumidity', 'HourlyDryBulbTemperature']

def read_lines():
    with open(OUTPUT_TEXT_FILE, 'r') as output_file:
        locations = output_file.readlines()

    return locations


def extract_content(file):
    """
    Function to extract content from the given CSV file and
    converts the data into a tuple of the form:
    <Lat, Lon, [[ArrayOfHourlyDataOfTheReqFields]]>
    """
    try:
        df = pd.read_csv(file)
        weather_fields = df[REQUIRED_FIELDS]

        latitude = df['LATITUDE'].iloc[0]
        longitude = df['LONGITUDE'].iloc[0]

        weather_data = weather_fields.values.tolist()
        weather_tuple = (latitude, longitude, weather_data)

        return [str(weather_tuple)]

    except Exception as e:
        raise ValueError(f'Error in extracting weather data from CSV file: {str(e)}')


# Defining beam pipeline

def run_beam_pipeline():
    with beam.Pipeline() as pipeline:
        files = (
            pipeline
            | "Form a list of csv files" >> beam.Create(os.listdir(UNZIP_FOLDER_LOCATION))
            | "Read CSV files into a dataframe" >> beam.Map(lambda file: os.path.join(UNZIP_FOLDER_LOCATION, file))
            | "Extract content into a tuple" >> beam.Map(extract_content)
            | "Write the tuples into a text file" >> beam.io.WriteToText(OUTPUT_TEXT_FILE)
        )


if __name__ == "__main__":
    run_beam_pipeline()