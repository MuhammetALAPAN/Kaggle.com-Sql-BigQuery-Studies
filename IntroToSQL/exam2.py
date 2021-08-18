from google.oauth2 import service_account
from google.cloud import bigquery

ONE_MB = 1000*1000
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=ONE_MB)

credentials = service_account.Credentials.from_service_account_file("C:/Users/muhem/Desktop/"
                                                                    "sqlbigquerytraining-836d50cb80ec.json")
client = bigquery.Client(credentials=credentials)

# dataset_ref = client.dataset("openaq", project="bigquery-public-data")
# table_ref = dataset_ref.table('global_air_quality')
# table = client.get_table(table_ref)

# result = client.list_rows(table, max_results=100).to_dataframe()
# print(result.to_string())

# Query to select distinct countries which unit = 'ppm'
# Project: bigquery-public-data
# Dataset: openaq
# Table: global_air_quality

query1 = """
         SELECT DISTINCT country 
         FROM `bigquery-public-data.openaq.global_air_quality`
         WHERE unit = 'ppm'
         """

# Query to select all columns which value = 0
# Project: bigquery-public-data
# Dataset: openaq
# Table: global_air_quality

query2 = """
         SELECT *
         FROM `bigquery-public-data.openaq.global_air_quality`
         WHERE value=0
         """

# q1 = client.query(query1).to_dataframe()
# print(q1.to_string())

q2 = client.query(query2, job_config=safe_config).to_dataframe()
print(q2.head().to_string())
