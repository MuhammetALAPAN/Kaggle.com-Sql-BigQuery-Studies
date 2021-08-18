from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.\
    Credentials.from_service_account_file("C:/Users/muhem/Desktop/sqlbigquerytraining-836d50cb80ec.json")
client = bigquery.Client(credentials=credentials)

# dataset_ref = client.dataset('hacker_news', project='bigquery-public-data')
# table1_ref = dataset_ref.table('comments')
# table2_ref = dataset_ref.table('stories')
# table1 = client.get_table(table1_ref)
# table2 = client.get_table(table2_ref)
# table_result1 = client.list_rows(table1, max_results=10).to_dataframe()
# table_result2 = client.list_rows(table2, max_results=10).to_dataframe()
# print(table_result1.to_string())
# print(table_result2.to_string())

safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10*100)

# Query to select all stories posted on January 1, 2012, with number of comments
# Project: bigquery-public-data
# Dataset: hacker_news
# Tables: comments, stories

query1 = """
         WITH c AS
         (
             SELECT parent, COUNT(id) as CommentCount
             FROM `bigquery-public-data.hacker_news.comments`
             GROUP BY parent
         )
         SELECT s.id as StoryID, s.by, s.title, c.CommentCount
         FROM `bigquery-public-data.hacker_news.stories` as s 
         LEFT JOIN c
             ON s.id = c.parent
         WHERE EXTRACT(date from time_ts) = '2012-01-01'
         ORDER BY c.CommentCount DESC 
         """

# Query to select all users who posted stories or comments on January 1, 2014
# Project: bigquery-public-data
# Dataset: hacker_news
# Tables: comments, stories

query2 = """
         SELECT s.by as username
         FROM `bigquery-public-data.hacker_news.stories` as s
         WHERE EXTRACT(date from time_ts) = '2014-01-01'
         UNION DISTINCT 
         SELECT c.by as username
         FROM `bigquery-public-data.hacker_news.comments` as c
         WHERE EXTRACT(date from time_ts) = '2014-01-01'
         """

# q1 = client.query(query1, job_config=safe_config).to_dataframe()
# print(q1.to_string())

q2 = client.query(query2, job_config=safe_config).to_dataframe()
print(len(q2))
# print(q2.to_string())

