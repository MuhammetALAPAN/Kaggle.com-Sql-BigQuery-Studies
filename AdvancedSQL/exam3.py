from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.\
    Credentials.from_service_account_file("C:/Users/muhem/Desktop/sqlbigquerytraining-836d50cb80ec.json")
client = bigquery.Client(credentials=credentials)

# dataset_ref = client.dataset('github_repos', project='bigquery-public-data')
# table_ref = dataset_ref.table('languages')
# table = client.get_table(table_ref)
# table_result = client.list_rows(table, max_results=10).to_dataframe()
# print(table_result.to_string())

# for i in table.schema:
#     print(i)

safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10*100)

# Query to list best committers in 2016
# project: bigquery-public-data
# dataset: github_repos
# table: sample_commits

query1 = """
         SELECT committer.name AS committer_name,
            COUNT(1) as num_commits
         FROM `bigquery-public-data.github_repos.sample_commits`
         WHERE EXTRACT(YEAR from committer.date) = 2016 
         GROUP BY committer_name
         ORDER BY num_commits DESC
         """

# Query to list most popular languages which appear in the most repositories.
# project: bigquery-public-data
# dataset: github_repos
# table: languages

query2 = """
         SELECT l.name as language_name,
            COUNT(repo_name) AS num_repos
         FROM `bigquery-public-data.github_repos.languages`,
            UNNEST(language) AS l
         GROUP BY language_name
         ORDER BY num_repos DESC
         """

# Query to list most used languages with their total byte count on 'polyrabbit/polyglot' repository.
# project: bigquery-public-data
# dataset: github_repos
# table: languages

query3 = """
         SELECT l.name as name,
            SUM(l.bytes) AS bytes
         FROM `bigquery-public-data.github_repos.languages`,
            UNNEST(language) AS l
         WHERE repo_name = 'polyrabbit/polyglot'
         GROUP BY name
         ORDER BY bytes DESC
         """

# q1 = client.query(query1, job_config=safe_config).to_dataframe().head()
# print(q1.to_string())

# q2 = client.query(query2, job_config=safe_config).to_dataframe().head()
# print(q2.to_string())

q3 = client.query(query3, job_config=safe_config).to_dataframe().head()
print(q3.to_string())
