from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file("C:/Users/muhem/Desktop/"
                                                                    "sqlbigquerytraining-836d50cb80ec.json")
client = bigquery.Client(credentials=credentials)

# dataset_ref = client.dataset('crypto_bitcoin', project='bigquery-public-data')
# table_ref = dataset_ref.table('transactions')
# table = client.get_table(table_ref)
# results = client.list_rows(table, max_results=10).to_dataframe()
# print(results.to_string())

safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)

# Query to select the number of transactions per date, sorted by date
# Project: bigquery-public-data
# Dataset: crypto_bitcoin
# Table: transactions

query = """
        WITH time AS
        (
            SELECT DATE(block_timestamp) AS trans_date
            FROM `bigquery-public-data.crypto_bitcoin.transactions`
        )
        SELECT COUNT(1) as transactions, 
            trans_date
        FROM time
        GROUP BY trans_date
        Order By trans_date
        """

q = client.query(query, job_config=safe_config).to_dataframe()
q.set_index('trans_date').plot()
#print(q.to_string())
