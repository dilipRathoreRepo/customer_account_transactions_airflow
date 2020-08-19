from airflow import DAG
from datetime import datetime, timedelta
import re
import gcsfs
import pandas as pd
from airflow.contrib.operators import gcs_to_bq
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.email_operator import EmailOperator


yesterday_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')

default_args = {
    'owner': 'DilipRathore',
    'start_date': datetime(2020, 8, 17),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}


def data_cleaner():
    fs = gcsfs.GCSFileSystem(project='rising-minutia-254502')
    with fs.open('gs://customer-account-transactions-bucket/transactions_mini.csv') as f:
        df = pd.read_csv(f)

    def clean_street_address(st_address):
        return re.sub(r'[^\w\s]', '', st_address).strip()

    def clean_account_no(acct_no):
        matches = re.findall(r'\d+', str(acct_no))
        if matches:
            return matches[0]
        return acct_no

    def remove_dollar(amount):
        return float(amount.replace('$', ''))

    df['street_address'] = df['street_address'].map(lambda x: clean_street_address(x))
    df['account_no'] = df['account_no'].map(lambda x: clean_account_no(x))
    df['amount'] = df['amount'].map(lambda x: remove_dollar(x))

    with fs.open('gs://customer-account-transactions-bucket/clean_transactions_mini.csv', 'w') as f:
        df.to_csv(f, index=False)

with DAG('customer_account_transactions_dag',default_args=default_args,schedule_interval='@daily', template_searchpath=['/usr/local/airflow/sql_files'], catchup=True) as dag:
    clean_csv = PythonOperator(task_id='clean_raw_csv', python_callable=data_cleaner)
    load_bigquery = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id='gcs_to_bq',
        bucket='customer-account-transactions-bucket',
        source_objects=['clean_transactions_mini.csv'],
        skip_leading_rows=1,
        destination_project_dataset_table='rising-minutia-254502:customer_account_transactions_ds.cust_acct_txn_table_new',
        schema_fields=[
                       {
                          "mode":"nullable",
                          "name":"customer_id",
                          "type":"STRING"
                       },
                       {
                          "mode":"nullable",
                          "name":"first_name",
                          "type":"STRING"
                       },
                       {
                          "mode":"nullable",
                          "name":"last_name",
                          "type":"STRING"
                       },
                       {
                          "mode":"nullable",
                          "name":"email",
                          "type":"STRING"
                       },
                       {
                          "mode":"nullable",
                          "name":"gender",
                          "type":"STRING"
                       },
                       {
                          "mode":"nullable",
                          "name":"account_no",
                          "type":"NUMERIC"
                       },
                       {
                          "mode":"nullable",
                          "name":"currency",
                          "type":"STRING"
                       },
                       {
                          "mode":"nullable",
                          "name":"amount",
                          "type":"FLOAT"
                       },
                       {
                          "mode":"nullable",
                          "name":"transaction_time",
                          "type":"DATE"
                       },
                       {
                          "mode":"nullable",
                          "name":"street_address",
                          "type":"STRING"
                       },
                       {
                          "mode":"nullable",
                          "name":"city",
                          "type":"STRING"
                       },
                       {
                          "mode":"nullable",
                          "name":"postal_code",
                          "type":"STRING"
                       },
                       {
                          "mode":"nullable",
                          "name":"country_name",
                          "type":"STRING"
                       }
                      ],
        source_format='CSV',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='bigquery_default',
        google_cloud_storage_conn_id='google_cloud_storage_default'
        )

    (clean_csv >> load_bigquery)

