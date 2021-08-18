from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file("C:/Users/muhem/Desktop/"
                                                                    "sqlbigquerytraining-836d50cb80ec.json")
client = bigquery.Client(credentials=credentials)

# dataset_ref = client.dataset('world_bank_intl_education', project='bigquery-public-data')
# table_ref = dataset_ref.table('international_education')
# table = client.get_table(table_ref)
# results = client.list_rows(table, max_results=50).to_dataframe()
# print(results.to_string())

safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)

# Query to List avarage educational spending per Country in 2010-2017
# Project: bigquery-public-data
# Dataset: world_bank_intl_education
# Table: international_education

query1 = """
         SELECT country_name, AVG(value) AS avg_ed_spending_pct
         FROM `bigquery-public-data.world_bank_intl_education.international_education`
         WHERE indicator_code = 'SE.XPD.TOTL.GD.ZS' and year >= 2010 and year <= 2017
         GROUP BY country_name
         ORDER BY avg_ed_spending_pct DESC
         """

# Query to list indicator codes which have been in table more than 175 times
# Project: bigquery-public-data
# Dataset: world_bank_intl_education
# Table: international_education

query2 = """
         SELECT indicator_name, indicator_code, COUNT(indicator_code) as num_rows
         FROM `bigquery-public-data.world_bank_intl_education.international_education`
         WHERE year = 2016
         GROUP BY indicator_name, indicator_code
         HAVING COUNT(indicator_code) >= 175
         ORDER BY num_rows DESC
         """

# q1 = client.query(query1, job_config=safe_config).to_dataframe()
# print(q1.to_string())

q2 = client.query(query2, job_config=safe_config).to_dataframe()
print(q2.to_string())
