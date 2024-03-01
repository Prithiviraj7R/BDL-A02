import apache_beam as beam

import os
import pandas as pd
from datetime import datetime

# Defining required fields: File locations 

OUTPUT_TEXT_FILE = "/opt/airflow/logs/artifacts/weather_data.txt"
MONTHLY_AVERAGES_FILE = '/opt/airflow/logs/artifacts/monthly_averages.txt'

def compute_monthly_averages(text)
    try:
        all_monthly_averages = {}

        weather_info = text.strip().split(',')
        latitude = weather_info[0]
        longitude = weather_info[1]
        weather_data = np.array(eval(','.join(weather_info[2:])))

        dates = weather_data[:, 0]
        weather_fields = weather_data[:, 1:]

        dates = np.array([datetime.strptime(date, r"%Y-%m-%d") for date in dates])
        months = [date.month for date in dates]
        unique_months = set(months)

        for month in unique_months:
            indices = [i for i, m in enumerate(months) if m==month]
            weather_fields_monthly = weather_fields[indices]

            monthly_averages = np.mean(weather_fields_monthly, axis=0)

            if month not in monthly_averages:
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

        return str(tuple_data)

    except Exception as e:
        raise ValueError(f'Error while computing monthly averages: {str(e)}')


# Defining beam pipeline

def run_beam_pipeline():
    with beam.pipeline() as pipeline:
        monthly_averages = (
            pipeline
            | 'Read Text file' >> beam.io.ReadFromText(OUTPUT_TEXT_FILE)
            | 'Calculate Monthly Averages' >> beam.Map(compute_monthly_averages)
            | 'Store in text file' >> beam.io.WriteToText(MONTHLY_AVERAGES_FILE)
        )

