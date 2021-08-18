from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file("C:/Users/muhem/Desktop/"
                                                                    "sqlbigquerytraining-836d50cb80ec.json")
client = bigquery.Client(credentials=credentials)

# dataset_ref = client.dataset("openaq", project="bigquery-public-data")
# dataset = client.get_dataset(dataset_ref)
# table_ref = dataset_ref.table('global_air_quality')
# table = client.get_table(table_ref)

# result = client.list_rows(table, max_results=5).to_dataframe()
# print(result.to_string())

# Query to select all the items from the "city" column where the "country" column is 'US'
# Project: bigquery-public-data
# Dataset: openaq
# Table: global_air_quality

query = """
        SELECT city 
        FROM `bigquery-public-data.openaq.global_air_quality` 
        WHERE country = 'US'
        """

us_cities = client.query(query).to_dataframe()
print(us_cities.city.value_counts().head())
