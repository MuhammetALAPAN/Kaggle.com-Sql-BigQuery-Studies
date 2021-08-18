from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file("C:/Users/muhem/Desktop/"
                                                                    "sqlbigquerytraining-836d50cb80ec.json")
client = bigquery.Client(credentials=credentials)

# dataset_ref = client.dataset('nhtsa_traffic_fatalities', project='bigquery-public-data')
# table_ref = dataset_ref.table('accident_2015')
# table = client.get_table(table_ref)
# results = client.list_rows(table, max_results=10).to_dataframe()
# print(results.to_string())

safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)

# Query to find out the number of accidents for each day of the week
# Project: bigquery-public-data
# Dataset: nhtsa_traffic_fatalities
# Table: accident_2015

query = """
        SELECT EXTRACT(DAYOFWEEK FROM timestamp_of_crash) AS day_of_week, COUNT(consecutive_number) AS CrashCount
        FROM `bigquery-public-data.nhtsa_traffic_fatalities.accident_2015`
        GROUP BY day_of_week
        ORDER BY CrashCount DESC
        """

q = client.query(query, job_config=safe_config).to_dataframe()
print(q.to_string())
