from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.\
    Credentials.from_service_account_file("C:/Users/muhem/Desktop/sqlbigquerytraining-836d50cb80ec.json")

client = bigquery.Client(credentials=credentials)

# dataset_ref = client.dataset('san_francisco', project='bigquery-public-data')
# table_ref = dataset_ref.table('bikeshare_trips')
# table = client.get_table(table_ref)
# table_result = client.list_rows(table, max_results=10).to_dataframe()
# print(table_result.to_string())

safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10*100)

# Query to count the (cumulative) number of trips per day
# project: bigquery-public-data
# dataset: san_francisco
# table: bikeshare_trips

query1 = """
         WITH temp AS
         (
         SELECT DATE(start_date) as date, COUNT(1) as trip_count_per_date
         FROM `bigquery-public-data.san_francisco.bikeshare_trips`
         WHERE EXTRACT(YEAR from start_date) = 2015
         GROUP BY date
         )
         SELECT *,
            SUM(trip_count_per_date)
                OVER (
                     ORDER BY date
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                     ) as cumulative_trip_count
         FROM temp
         """

# Query to track beginning and ending stations on October 25, 2015, for each bike
# project: bigquery-public-data
# dataset: san_francisco
# table: bikeshare_trips

query2 = """
         SELECT bike_number,
            TIME(start_date) as time,
            FIRST_VALUE(start_station_id)
                OVER (
                     PARTITION BY bike_number
                     ORDER BY TIME(start_date)
                     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                     ) as first_station_id,
            LAST_VALUE(end_station_id)
                OVER (
                     PARTITION BY bike_number
                     ORDER BY TIME(end_date)
                     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                     ) as last_station_id
         FROM `bigquery-public-data.san_francisco.bikeshare_trips`
         WHERE DATE(start_date) = '2015-10-25'
         """

# q1 = client.query(query1, job_config=safe_config).to_dataframe()
# print(q1.to_string())

q2 = client.query(query2, job_config=safe_config).to_dataframe().head()
print(q2.to_string())
