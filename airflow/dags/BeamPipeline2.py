import apache_beam as beam
import numpy as np
from datetime import datetime
import ast

# Defining required fields: File locations 
OUTPUT_TEXT_FILE = "/opt/airflow/logs/artifacts/weather_data.txt-00000-of-00001"
MONTHLY_AVERAGES_FILE = '/opt/airflow/logs/artifacts/monthly_averages.txt'


def read_lines(element):
    with open(OUTPUT_TEXT_FILE, 'r') as output_file:
        locations = output_file.readlines()

    return locations


def compute_monthly_averages(element):
    all_monthly_averages = {}

    element = element[0]

    data = ast.literal_eval(element)
    latitude, longitude, weather_data = data

    weather_data = np.array(weather_data)

    dates = weather_data[:, 0]
    weather_fields = weather_data[:, 1:]

    dates = np.array([datetime.strptime(date, r"%Y-%m-%dT%H:%M:%S") for date in dates])
    months = [date.month for date in dates]
    unique_months = set(months)

    for month in unique_months:
        indices = [i for i, m in enumerate(months) if m == month]
        weather_fields_monthly = weather_fields[indices]

        monthly_averages = np.mean(weather_fields_monthly, axis=0)

        if month not in all_monthly_averages:
            all_monthly_averages[month] = []

        all_monthly_averages[month] = monthly_averages

    for month in all_monthly_averages:
        all_monthly_averages[month] = np.array(all_monthly_averages[month])

    sorted_months = sorted(all_monthly_averages.keys())
    sorted_averages = []

    for month in sorted_months:
        sorted_averages.append(all_monthly_averages[month])

    sorted_averages = np.array(sorted_averages)

    tuple_data = (latitude, longitude, sorted_averages)

    return tuple_data


# Defining beam pipeline
def run_beam_pipeline():
    with beam.Pipeline() as pipeline:
        monthly_averages = (
            pipeline
            | 'Read Text file' >> beam.Create([None])  # Dummy element for the DoFn
            | 'Read and Process Lines' >> beam.FlatMap(read_lines)
            | 'Calculate Monthly Averages' >> beam.Map(compute_monthly_averages)
            | 'Store in text file' >> beam.io.WriteToText(MONTHLY_AVERAGES_FILE)
        )


if __name__ == "__main__":
    run_beam_pipeline()
