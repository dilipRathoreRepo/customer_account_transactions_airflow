# Customer-Account-Transactions-Airflow

Customer-Account-Transactions-Airflow is a Python project for ingesting and cleaning customer's transactions records and generate meaning ful insights later.

## Deployment

The code needs to be copied to Cloud Composer(Airflow) DAG cloud storage bucket. #TODO Build a CI/CD framework to do that

## Usage

```python
Triggers via Airflow
```
**Note**: The code requires environment variables to be available at run time which are passed via Cloud Build Trigger -> Kubernetes Manifest File -> Python main.py. Pls run unit test cases on local system or patch the environment variables in main.py and utils.py to run locally.

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.