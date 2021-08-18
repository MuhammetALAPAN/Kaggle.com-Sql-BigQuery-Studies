from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file("C:/Users/muhem/Desktop/"
                                                                    "sqlbigquerytraining-836d50cb80ec.json")
client = bigquery.Client(credentials=credentials)

# dataset_ref = client.dataset('hacker_news', project='bigquery-public-data')
# table_ref = dataset_ref.table('comments')
# table = client.get_table(table_ref)
# results = client.list_rows(table, max_results=1000).to_dataframe()
# print(results.to_string())

safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)

# Query to select comments that received more than 10 replies
# Project: bigquery-public-data
# Dataset: hacker_news
# Table: comments

query = """
        SELECT parent, COUNT(1) AS NumPosts
        FROM `bigquery-public-data.hacker_news.comments`
        GROUP BY parent
        HAVING COUNT(1) > 10
        """

q = client.query(query, job_config=safe_config).to_dataframe().head()
print(q.to_string())
