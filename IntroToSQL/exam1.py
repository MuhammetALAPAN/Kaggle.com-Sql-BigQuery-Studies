
from google.oauth2 import service_account

from google.cloud import bigquery

# Task: create a Client object and Access a table then print table schema
# Project: bigquery-public-data
# Dataset: chicago_crime
# Table: crime

credentials = service_account.Credentials.from_service_account_file("C:/Users/muhem/Desktop/"
                                                                    "sqlbigquerytraining-836d50cb80ec.json")
client = bigquery.Client(credentials=credentials)

dataset_ref = client.dataset("chicago_crime", project="bigquery-public-data")
dataset = client.get_dataset(dataset_ref)
table_ref = dataset_ref.table("crime")
table = client.get_table(table_ref)
print(table.schema)
