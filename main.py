from src.gmaps import Gmaps
import pandas as pd
from tasks.data_pipeline.geo_cleaner import GeoCleaner
from tasks.data_pipeline.database.load_db import insert_data_from_csv

cleaner = GeoCleaner()


df = pd.read_csv('tasks/data_pipeline/queries/farm_queries_DEU.csv')

# Generate queries dynamically from DataFrame to list format for web scraping
queries = []
for _, row in df.iterrows():
    query = f"{row['query']}"
    queries.append(query)


Gmaps.places(queries)
#cleaner.clean("output/all/csv/places-of-all.csv")
#insert_data_from_csv("output/all/csv/places-of-all_cleaned.csv")