# BDL-ASSIGNMENT 02
### Data Engineering Pipeline using Apache Airflow and Apache Beam Pipeline Options
#### This data engineering pipeline is used to retrieve climate and weather data from a archive on the web and make transformations to visualize the weather information globally.

Below are the DAGs (Directed Acyclic Graphs) containing all the defined tasks in a pipeline, implemented using Apache Airflow.

![Screenshot 2024-03-05 185028](https://github.com/Prithiviraj7R/BDL-A02/assets/142074094/1b1fcddb-2ef3-4e55-81ef-c81eaba44aca)

![Screenshot 2024-03-05 185444](https://github.com/Prithiviraj7R/BDL-A02/assets/142074094/fefdb315-355f-4d9d-b87a-4011fc5dd2fe)

The Data Fetch Pipeline encompasses the subsequent procedures:

1. **Acquisition of URLs:** Given the URL pointing to the archive database of the National Centers for Environmental Information, the task retrieves available links to .csv files containing weather data from weather stations worldwide.

2. **Selection of Links:** The subsequent task involves randomly selecting a required number of links from the retrieved list and downloading the corresponding .csv files, which are stored in a designated folder.

3. **Compression of folder:** The final task of this pipeline involves compressing the folder that contains all the downloaded files into a single zip archive.

To address storage constraints, I have selectively downloaded the data from the 2020 archive year and opted for a subset comprising 100 files (100 stations worldwide) for analysis in downstream tasks.

The Analysis Pipeline encompasses the following tasks:

1. **File Sensor:** Given the location of the zipped archive from the fetch pipeline, the file sensor polls the location to check for the availability of the file. Upon availability, the archive is then unzipped for further computations.

2. **Data Extraction:** Data is extracted from the .csv files into a pandas dataframe. Location-wise data is retrieved and written as a tuple of the form: (Latitude, Longitude, [Array of Hourly values of the required fields]). This data is written onto a text file. Apache Beam pipelines are used for setting up this task as a pipeline.

3. **Computation of Monthly Averages:** The next task involves computing monthly averages from the extracted tuples and storing them in a file. The computed monthly averages are of the format: (Latitude, Longitude, [Array of monthly averages for M months and N fields]). Apache Beam pipelines are used for setting up this task as a pipeline.

4. **Plotting Geomaps:** The monthly averages data is utilized to plot geomaps using the geopandas library. A total of 12 plots (for 12 months) are plotted for each field. In order to plot the geographical map, it's necessary to read the entire file containing monthly averages from all locations. Therefore, the task of parallelization using Apache Beam was not implemented for this task.

5. **Compilation of GIF:** The subsequent task is to compile these 12 images for each field into a GIF.

6. **File Deletion:** The final task of the pipeline involves deleting the files that are downloaded and not in use.

From the data, I have selected 4 fields, namely: 'HourlyWindSpeed', 'HourlyDryBulbTemperature', 'HourlyDewPointTemperature', and 'HourlyPressureChange'. To improve convenience of retrieval, rather than using tuples, I've opted for dictionaries and saved the data in a .json file format.


