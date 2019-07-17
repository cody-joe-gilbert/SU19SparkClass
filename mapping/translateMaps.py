
"""
CSCI-GA.3033-001: Summer 2019 Team Project
Cody Gilbert, Fang Han, Jeremy Lao

This Python script converts US Census State and County shapefiles (.shp)
into .csv files that can be used in Spark RDD applications.

NOTE: The .csv files are tab (\t) delimited instead of comma due to commas
    being part of the geometry's well-known-text (wkt) string.

"""
import geopandas as gpd
from shapely import wkt
import pandas as pd

countyFile = r"C:\Users\Cody Gilbert\Downloads\tl_2017_us_county.shp"
stateFile = r"C:\Users\Cody Gilbert\Downloads\tl_2017_us_county.shp"

# Convert the county-level files
usa = gpd.read_file(countyFile)
usa['geometry'] = usa['geometry'].apply(lambda x: x.wkt)
with open('countyGeometry.csv', 'w') as f:
    usa.to_csv(f, sep="\t")

# Convert the state-level files
usa = gpd.read_file(stateFile)
usa['geometry'] = usa['geometry'].apply(lambda x: x.wkt)
with open('stateGeometry.csv.csv', 'w') as f:
    usa.to_csv(f, sep="\t")