from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file("C:/Users/muhem/Desktop/"
                                                                    "sqlbigquerytraining-836d50cb80ec.json")
client = bigquery.Client(credentials=credentials)

# dataset_ref = client.dataset('chicago_taxi_trips', project='bigquery-public-data')
# table_ref = dataset_ref.table('taxi_trips')
# table = client.get_table(table_ref)
# results = client.list_rows(table, max_results=5).to_dataframe()
# print(results.to_string())

safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)

# Query to list number of trips made per month (0-12) in 2017
# Project: bigquery-public-data
# Dataset: chicago_taxi_trips
# Table: taxi_trips

query1 = """
         WITH time AS
         (
            SELECT EXTRACT(MONTH FROM DATE(trip_start_timestamp)) AS month
            FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
            where EXTRACT(YEAR FROM DATE(trip_start_timestamp)) = 2017
         )
         SELECT month, count(1) AS num_trips
         FROM time
         GROUP BY month
         ORDER BY month
         """

# Query to list number of trips made per hour (0-24) and avarage speed in first 6 montsh of 2017.
# Project: bigquery-public-data
# Dataset: chicago_taxi_trips
# Table: taxi_trips

query2 = """
         WITH time AS
         (
            SELECT EXTRACT(HOUR from trip_start_timestamp) AS hour_of_day, 
                   trip_miles,
                   trip_seconds
            FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
            where trip_start_timestamp > '2017-01-01' AND 
                  trip_start_timestamp < '2017-07-01' AND
                  trip_seconds > 0 AND 
                  trip_miles > 0
         )
         SELECT hour_of_day, 
            count(1) AS num_trips,
            3600 * SUM(trip_miles) / SUM(trip_seconds) AS avg_mph
         FROM time
         GROUP BY hour_of_day
         ORDER BY hour_of_day
         """

# q1 = client.query(query1, job_config=safe_config).to_dataframe()
# print(q1.to_string())

q2 = client.query(query2, job_config=safe_config).to_dataframe()
print(q2.to_string())
