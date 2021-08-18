from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.\
    Credentials.from_service_account_file("C:/Users/muhem/Desktop/sqlbigquerytraining-836d50cb80ec.json")
client = bigquery.Client(credentials=credentials)

# dataset_ref = client.dataset('stackoverflow', project='bigquery-public-data')
# table1_ref = dataset_ref.table('posts_questions')
# table2_ref = dataset_ref.table('posts_answers')
# table1 = client.get_table(table1_ref)
# table2 = client.get_table(table2_ref)
# table_result1 = client.list_rows(table1, max_results=10).to_dataframe()
# table_result2 = client.list_rows(table2, max_results=10).to_dataframe()
# print(table_result1.to_string())
# print(table_result2.to_string())

safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10*100)

# Query to determine how long does it take for questions to receive answers in January 2018?
# Project: bigquery-public-data
# Dataset: stackoverflow
# Tables: posts_answers, posts_questions

query1 = """
         SELECT pq.id as id, MIN(TIMESTAMP_DIFF(pa.creation_date, pq.creation_date, SECOND)) as time_to_answer
         FROM `bigquery-public-data.stackoverflow.posts_questions` as pq
         INNER JOIN `bigquery-public-data.stackoverflow.posts_answers` as pa
            ON pq.id = pa.parent_id
         WHERE pq.creation_date >= '2018-01-01' AND pa.creation_date < '2018-02-01'
         GROUP BY id
         ORDER BY time_to_answer DESC
         """

# Query to determine how long does it take for users to first interact with the website in January 2018?
# Project: bigquery-public-data
# Dataset: stackoverflow
# Tables: posts_answers, posts_questions

query2 = """
         SELECT q.id AS q_id,
              MIN(TIMESTAMP_DIFF(a.creation_date, q.creation_date, SECOND)) as time_to_answer
         FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
         LEFT JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
            ON q.id = a.parent_id
         WHERE q.creation_date >= '2018-01-01' and q.creation_date < '2018-02-01'
         GROUP BY q_id
         ORDER BY time_to_answer
         """

# Query to list activity of user who joined platform in January 2019
# Project: bigquery-public-data
# Dataset: stackoverflow
# Tables: posts_answers, posts_questions

query3 = """
         SELECT u.id AS id, MIN(q.creation_date) as q_creation_date, MIN(a.creation_date) as a_creation_date
         FROM `bigquery-public-data.stackoverflow.users` AS u
         LEFT JOIN `bigquery-public-data.stackoverflow.posts_questions` AS q
            ON u.id = q.owner_user_id
         LEFT JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
            ON u.id = a.owner_user_id
         WHERE u.creation_date >= '2019-01-01' and u.creation_date < '2019-02-01'
         GROUP BY id
         """

# Query to determine How many distinct users posted on January 1, 2019?
# Project: bigquery-public-data
# Dataset: stackoverflow
# Tables: posts_answers, posts_questions

query4 = """
         SELECT q.owner_user_id as owner_user_id 
         from `bigquery-public-data.stackoverflow.posts_questions` AS q
         WHERE EXTRACT(DATE from q.creation_date) = '2019-01-01'
         GROUP BY owner_user_id
         UNION DISTINCT 
         SELECT a.owner_user_id as owner_user_id 
         from `bigquery-public-data.stackoverflow.posts_answers` AS a
         WHERE EXTRACT(DATE from a.creation_date) = '2019-01-01'
         GROUP BY owner_user_id
         """

# q1 = client.query(query1, job_config=safe_config).to_dataframe()
# print(q1.to_string())

# q2 = client.query(query2, job_config=safe_config).to_dataframe()
# print(q2.to_string())
# print(len(q2))

# q3 = client.query(query3, job_config=safe_config).to_dataframe()
# print(q3.to_string())

q4 = client.query(query4, job_config=safe_config).to_dataframe()
print(q4.to_string())
