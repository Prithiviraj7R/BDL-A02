# Importing necessary libraries

import apache_beam as beam
import numpy as np
import pandas as pd

from shapely.geometry import Point
import geopandas as gpd
import matplotlib.pyplot as plt

from datetime import datetime
import json

# Defining required fields: File locations 
MONTHLY_AVERAGES_FILE = '/opt/airflow/logs/artifacts/monthly_averages.json'
WORLD_MAP_SHAPE_FILE = '/opt/airflow/logs/templates/copy_4.shp'
PLOT_SAVE_LOCATION = '/opt/airflow/logs/templates/'
REQUIRED_FIELDS = ['HourlyWindSpeed', 'HourlyDryBulbTemperature', 'HourlyDewPointTemperature', 'HourlyPressureChange']
MONTH = 1

def plot_geo_map(dummy):
    """
    Function to read and generate month wise geo plots.
    """
    with open(MONTHLY_AVERAGES_FILE, 'r') as f:
        monthly_averages = [json.loads(line.strip()) for line in f]

    # reading the shape file of the world map to plot the points at appropriate locations
    world_map = gpd.read_file(WORLD_MAP_SHAPE_FILE)
    crs = {'init': 'EPSG:4326'}

    # plot for all the months
    for month in range(1, 13):

        # creating a dataframe with latitude, longitude and other weather data
        latitudes = []
        longitudes = []
        weather_field_data = {feature: [] for feature in REQUIRED_FIELDS}

        for data_point in monthly_averages:
            latitudes.append(data_point['latitude'])
            longitudes.append(data_point['longitude'])

            for j, feature in enumerate(REQUIRED_FIELDS):
                weather_field_data[feature].append(data_point['monthly_averages'][month-1][j])

        df_data = {
            'Latitude': latitudes,
            'Longitude': longitudes
        }

        df_data.update(weather_field_data)
        df = pd.DataFrame(df_data)

        # creating a Geo DataFrame
        geometry = [Point(lon, lat) for lon, lat in zip(longitudes, latitudes)]
        gdf = gpd.GeoDataFrame(
            df,
            geometry=geometry,
            crs=crs
        )    

        # plot for all the features in the required fields list
        for feature in REQUIRED_FIELDS:

            fig, ax = plt.subplots(figsize = (10,10))
            world_map.to_crs(epsg=4326).plot(ax=ax, color='lightgrey')
            gdf.plot(ax=ax, column=feature, cmap='coolwarm', legend=True, legend_kwds={'label': f'Monthly Averages of {feature}', 'shrink': 0.5})
            ax.set_title(f'{month}: {feature}')
            ax.set_xlabel('Longitude')
            ax.set_ylabel('Latitude')

            plt.savefig(f'{PLOT_SAVE_LOCATION}{feature}_month_{month}.png')
            plt.close()


# Defining beam pipeline
def run_beam_pipeline():
    with beam.Pipeline() as pipeline:
        geo_plot = (
            pipeline
            | 'Read Text file' >> beam.Create([None])
            | 'Plot' >> beam.Map(plot_geo_map)
        )


if __name__ == "__main__":
    run_beam_pipeline()