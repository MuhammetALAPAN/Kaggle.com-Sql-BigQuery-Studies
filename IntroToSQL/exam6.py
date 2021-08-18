from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file("C:/Users/muhem/Desktop/"
                                                                    "sqlbigquerytraining-836d50cb80ec.json")
client = bigquery.Client(credentials=credentials)

# dataset_ref = client.dataset('stackoverflow', project='bigquery-public-data')
# dataset = client.get_dataset(dataset_ref)
# table1_ref = dataset_ref.table('posts_questions')
# table2_ref = dataset_ref.table('posts_answers')
# table1 = client.get_table(table1_ref)
# table2 = client.get_table(table2_ref)
# table_result1 = client.list_rows(table1, max_results=10).to_dataframe()
# table_result2 = client.list_rows(table2, max_results=10).to_dataframe()
# print(table_result1.to_string())
# print(table_result2.to_string())

safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=27*10**10)

# Query to determine the number of answers per user which have at least 1 answer.
# Project: bigquery-public-data
# Dataset: stackoverflow
# Tables: posts_questions, posts_answers

query = """
        SELECT a.owner_user_id as user_id, COUNT(1) as number_of_answers
        FROM `bigquery-public-data.stackoverflow.posts_questions` AS q 
        INNER JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
            ON q.id = a.parent_id
        WHERE q.tags LIKE '%bigquery%'
        GROUP BY user_id
        HAVING number_of_answers >= 1
        ORDER BY number_of_answers DESC
        """

q = client.query(query, job_config=safe_config).to_dataframe().head()
print(q.to_string())
