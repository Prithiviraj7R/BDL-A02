
## Importing necessary libraries

import apache_beam as beam

import os
import pandas as pd
import numpy as np
from datetime import datetime
import json

## Defining required fields: File locations 

UNZIP_FOLDER_LOCATION = "/opt/airflow/logs/artifacts/unzipped_climate_data/"
MONTHLY_AVERAGES_FILE = '/opt/airflow/logs/artifacts/monthly_averages.json'
REQUIRED_FIELDS = ['DATE', 'HourlyWindSpeed', 'HourlyDryBulbTemperature', 'HourlyDewPointTemperature', 'HourlyPressureChange']

## Defining required functions

def extract_content(file):
    """
    Function to extract content from the given CSV file and
    converts the data into a tuple of the form: 
    <Lat, Lon, [[ArrayOfHourlyDataOfTheReqFields]]>
    """
    try:
        df = pd.read_csv(file)
        weather_fields = df[REQUIRED_FIELDS]

        latitude = df['LATITUDE'][0]
        longitude = df['LONGITUDE'][0]

        weather_data = weather_fields.values.tolist()

        ## creating a dictionary to be passed on to the next task
        weather_dict = {
            'latitude': latitude,
            'longitude': longitude,
            'weather_data': weather_data
        }

        return weather_dict
    
    except Exception as e:
        raise ValueError(f"Error in extracting content from the .csv file: {str(e)}")


def compute_monthly_averages(data):
    """
    Function to compute monthly averages for the weather fields
    and store it in the form of tuples of format:
    <Lat, Long, [[Avg_11, ..., Avg_1N] .. [Avg_M1, ...,
    Avg_MN]]> for N fields and M months for plotting purposes.
    """
    try:
        all_monthly_averages = {i: [0.0]*len(REQUIRED_FIELDS) for i in range(1,13)}

        latitude, longitude, weather_data = data['latitude'], data['longitude'], data['weather_data']

        # data preprocessing to handle the incorrect data format
        weather_data_numeric = [
            [float(val) if isinstance(val, (int, float)) else 0 for val in row]
            for row in weather_data
        ]
        weather_data_numeric = np.array(weather_data_numeric)
        weather_data = np.array(weather_data)

        dates = weather_data[:, 0]
        weather_fields = weather_data_numeric[:, 1:]

        # extracting the months of the provided hourly data
        dates = np.array([datetime.strptime(date, r"%Y-%m-%dT%H:%M:%S") for date in dates])
        months = [date.month for date in dates]
        unique_months = set(months)

        # computing month wise averages
        for month in unique_months:
            indices = [i for i, m in enumerate(months) if m == month]
            weather_fields_monthly = weather_fields[indices]

            monthly_averages = np.mean(weather_fields_monthly, axis=0)

            all_monthly_averages[month] = monthly_averages.tolist()

        # sorting the averages to format it month-wise from Jan to Dec
        sorted_months = sorted(all_monthly_averages.keys())
        sorted_averages = [all_monthly_averages[month] for month in sorted_months]

        # storing the computed monthly averages in JSON format
        result_json = {
            'latitude': latitude,
            'longitude': longitude,
            'monthly_averages': sorted_averages
        }

        result_json_data = json.dumps(result_json)

        return result_json_data

    except Exception as e:
        raise ValueError(f"Error in computing monthly averages: {str(e)}")


## Defining beam pipelines

def run_beam_pipeline():
    with beam.Pipeline() as pipeline:
        averages = (
            pipeline
            | "Form a list of csv files" >> beam.Create(os.listdir(UNZIP_FOLDER_LOCATION))
            | "Get the file paths of csv files" >> beam.Map(lambda file: os.path.join(UNZIP_FOLDER_LOCATION, file))
            | "Extarct tuples from csv files" >> beam.Map(extract_content)
            | "Compute monthly averages" >> beam.Map(compute_monthly_averages)
            | "Write the tuples into a JSON file" >> beam.io.WriteToText(MONTHLY_AVERAGES_FILE)
        )


if __name__ == "__main__":
    run_beam_pipeline()