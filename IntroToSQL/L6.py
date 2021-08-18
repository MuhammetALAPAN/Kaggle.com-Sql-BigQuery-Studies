from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file("C:/Users/muhem/Desktop/"
                                                                    "sqlbigquerytraining-836d50cb80ec.json")
client = bigquery.Client(credentials=credentials)

# dataset_ref = client.dataset('github_repos', project='bigquery-public-data')
# table1_ref = dataset_ref.table('licenses')
# table2_ref = dataset_ref.table('sample_files')
# table1 = client.get_table(table1_ref)
# table2 = client.get_table(table2_ref)
# table_result1 = client.list_rows(table1, max_results=10).to_dataframe()
# table_result2 = client.list_rows(table2, max_results=10).to_dataframe()
# print(table_result1.to_string())
# print(table_result2.to_string())

safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)

# Query to determine the number of files per license, sorted by number of files
# Project: bigquery-public-data
# Dataset: github_repos
# Tables: licenses, sample_files

query = """
        SELECT license, COUNT(1) as FileCount
        FROM `bigquery-public-data.github_repos.licenses` as l
        INNER JOIN `bigquery-public-data.github_repos.sample_files` as s
            ON l.repo_name = s.repo_name
        GROUP BY license
        ORDER BY FileCount DESC
        """

q = client.query(query, job_config=safe_config).to_dataframe()
print(q.to_string())
