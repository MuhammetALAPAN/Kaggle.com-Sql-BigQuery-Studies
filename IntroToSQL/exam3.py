from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file("C:/Users/muhem/Desktop/sqlbigquerytraining-836d50cb80ec.json")
client = bigquery.Client(credentials=credentials)

# dataset_ref = client.dataset('hacker_news', project='bigquery-public-data')
# table_ref = dataset_ref.table('comments')
# table = client.get_table(table_ref)
# results = client.list_rows(table, max_results=5).to_dataframe()
# print(results.to_string())

safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)

# Query to list authors which have posts more than 10000
# Project: bigquery-public-data
# Dataset: hacker_news
# Table: comments

query1 = """
         SELECT author, COUNT(1) AS NumPosts
         FROM `bigquery-public-data.hacker_news.comments`
         GROUP BY author
         HAVING COUNT(1) > 10000
         """

# Query to Find totol deleted posts
# Project: bigquery-public-data
# Dataset: hacker_news
# Table: comments

query2 = """
         SELECT COUNT(1) AS TotalDeletedPosts
         FROM `bigquery-public-data.hacker_news.comments`
         WHERE deleted = True
         """

# q1 = client.query(query1, job_config=safe_config).to_dataframe()
# print(q1.to_string())

q2 = client.query(query2, job_config=safe_config).to_dataframe()
print(q2.to_string())
