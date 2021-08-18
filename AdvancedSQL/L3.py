from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.\
    Credentials.from_service_account_file("C:/Users/muhem/Desktop/sqlbigquerytraining-836d50cb80ec.json")
client = bigquery.Client(credentials=credentials)

# dataset_ref = client.dataset('google_analytics_sample', project='bigquery-public-data')
# table_ref = dataset_ref.table('ga_sessions_20170801')
# table = client.get_table(table_ref)
# table_result = client.list_rows(table, max_results=10).to_dataframe()
# print(table_result.to_string())

# print("SCHEMA field for the 'totals' column:\n")
# print(table.schema[5])

# print("\nSCHEMA field for the 'device' column:\n")
# print(table.schema[7])

safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10*100)

# Query to list total_transaction per browser types
# project: bigquery-public-data
# dataset: google_analytics_sample
# table: ga_sessions_20170801

query1 = """
         SELECT device.browser AS browser_type,
            SUM(totals.transactions) AS total_transactions
         FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`
         GROUP BY browser_type
         ORDER BY total_transactions DESC
         """

# Query to find most clicked pages from 'hits' column which has Record and Repeated data types.
# project: bigquery-public-data
# dataset: google_analytics_sample
# table: ga_sessions_20170801

query2 = """
         SELECT hits.page.pagePath AS path,
            COUNT(hits.page.pagePath) AS counts
         FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`,
            UNNEST(hits) AS hits
         WHERE hits.type = 'PAGE' AND hits.hitNumber = 1
         GROUP BY path
         ORDER BY counts DESC
         """

# q1 = client.query(query1, job_config=safe_config).to_dataframe().head()
# print(q1.to_string())

q2 = client.query(query2, job_config=safe_config).to_dataframe().head()
print(q2.to_string())
