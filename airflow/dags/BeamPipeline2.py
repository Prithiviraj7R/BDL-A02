import apache_beam as beam
import numpy as np
from datetime import datetime
import json

# Defining required fields: File locations 
OUTPUT_JSON_FILE = "/opt/airflow/logs/artifacts/weather_data.json"
MONTHLY_AVERAGES_FILE = '/opt/airflow/logs/artifacts/monthly_averages.json'


def read_lines(dummy):
    with open(OUTPUT_JSON_FILE, 'r') as output_file:
        locations = [json.loads(line.strip()) for line in output_file]

    return locations


def compute_monthly_averages(data):

    all_monthly_averages = {}

    latitude, longitude, weather_data = data['latitude'], data['longitude'], data['weather_data']

    weather_data_numeric = [
        [float(val) if isinstance(val, (int, float)) else 0 for val in row]
        for row in weather_data
    ]
    weather_data_numeric = np.array(weather_data_numeric)
    weather_data = np.array(weather_data)

    dates = weather_data[:, 0]
    weather_fields = weather_data_numeric[:, 1:]

    dates = np.array([datetime.strptime(date, r"%Y-%m-%dT%H:%M:%S") for date in dates])
    months = [date.month for date in dates]
    unique_months = set(months)

    for month in unique_months:
        indices = [i for i, m in enumerate(months) if m == month]
        weather_fields_monthly = weather_fields[indices]

        monthly_averages = np.mean(weather_fields_monthly, axis=0)

        if month not in all_monthly_averages:
            all_monthly_averages[month] = []

        all_monthly_averages[month] = monthly_averages.tolist()

    sorted_months = sorted(all_monthly_averages.keys())
    sorted_averages = [all_monthly_averages[month] for month in sorted_months]

    result_json = {
        'latitude': latitude,
        'longitude': longitude,
        'monthly_averages': sorted_averages
    }

    result_json_data = json.dumps(result_json)

    return result_json_data


# Defining beam pipeline
def run_beam_pipeline():
    with beam.Pipeline() as pipeline:
        monthly_averages = (
            pipeline
            | 'Read Text file' >> beam.Create([None])  
            | 'Read and Process Lines' >> beam.FlatMap(read_lines)
            | 'Calculate Monthly Averages' >> beam.Map(compute_monthly_averages)
            | 'Store in text file' >> beam.io.WriteToText(MONTHLY_AVERAGES_FILE, shard_name_template='', num_shards=1)
        )


if __name__ == "__main__":
    run_beam_pipeline()
