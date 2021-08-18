from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.\
    Credentials.from_service_account_file("C:/Users/muhem/Desktop/sqlbigquerytraining-836d50cb80ec.json")
client = bigquery.Client(credentials=credentials)

# dataset_ref = client.dataset('chicago_taxi_trips', project='bigquery-public-data')
# table_ref = dataset_ref.table('taxi_trips')
# table = client.get_table(table_ref)
# table_result = client.list_rows(table, max_results=10).to_dataframe()
# print(table_result.to_string())

safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10*100)

# Query to get average of the daily number of taxi trips. From 2016-01-01 to 2018-01-01,
# Calculate as 15 Preceding and 15 Following Rows
# project: bigquery-public-data
# dataset: chicago_taxi_trips
# table: taxi_trips

query1 = """
         WITH trips_by_day AS
         (
         SELECT DATE(trip_start_timestamp) AS trip_date,
             COUNT(*) as num_trips
         FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
         WHERE trip_start_timestamp >= '2016-01-01' AND trip_start_timestamp < '2018-01-01'
         GROUP BY trip_date
         ORDER BY trip_date
          )
         SELECT trip_date,
         AVG(num_trips)
             OVER (
                  ORDER BY trip_date
                  ROWS BETWEEN 15 PRECEDING AND 15 FOLLOWING
                  ) AS avg_num_trips
         FROM trips_by_day
         """

# Query to list rank of trips according to their respective community areas on 2017-05-01
# with pickup_community_area, trip_start_timestamp, trip_end_timestamp, trip_number columns.
# project: bigquery-public-data
# dataset: chicago_taxi_trips
# table: taxi_trips

query2 = """
         SELECT pickup_community_area,
             trip_start_timestamp,
             trip_end_timestamp,
             RANK()
                 OVER (
                       PARTITION BY pickup_community_area
                       ORDER BY trip_start_timestamp
                      ) AS trip_number
         FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
         WHERE DATE(trip_start_timestamp) = '2017-05-01' 
         """

# Query to calculate time difference between prev trip end time and current trip start time.
# on 2017-05-01.
# project: bigquery-public-data
# dataset: chicago_taxi_trips
# table: taxi_trips

query3 = """
         SELECT taxi_id,
            trip_start_timestamp,
            trip_end_timestamp,
            TIMESTAMP_DIFF(
                trip_start_timestamp, 
                LAG(trip_end_timestamp) 
                    OVER (
                         PARTITION BY taxi_id 
                         ORDER BY trip_start_timestamp), 
                MINUTE) as prev_break
         FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
         WHERE DATE(trip_start_timestamp) = '2017-05-01'
         """

# q1 = client.query(query1, job_config=safe_config).to_dataframe()
# print(q1.to_string())

# q2 = client.query(query2, job_config=safe_config).to_dataframe()
# print(q2.to_string())

q3 = client.query(query3, job_config=safe_config).to_dataframe()
print(q3.to_string())
