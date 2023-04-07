'''import google.auth
import google.auth.transport.requests
import google.auth.transport.urllib3
import google.auth.credentials'''
import json

from google.cloud import storage
from google.oauth2 import service_account

from google.cloud import bigquery
import boto3
import uuid
import json
import pandas as pd
import io
import time

KEY_FILE="../secure_files/aws_key.csv"

if __name__=="__main__":
  key_data=pd.read_csv(KEY_FILE).iloc[0]
  ACCESS_KEY = key_data['Access key ID']
  SECRET_KEY = key_data['Secret access key']

  # Set up AWS Athena client
  athena_client = boto3.client('athena', region_name='us-west-1', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

  # Set up S3 client
  s3_client = boto3.client('s3', region_name='us-west-1', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

  # Set the name of the Athena database and table you want to copy
  athena_database_name = 'default'
  athena_table_name = 'cloudtrail_logs_aws_cloudtrail_logs_266665233841_257eb010'


  # Set the Amazon S3 location for the query results
  s3_output_location = 's3://aws-athena-query-results-266665233841-us-west-1/'

  # Set the query string to select all columns from the Athena table
  query_string = f'SELECT * FROM "{athena_database_name}"."{athena_table_name}"'

  # Set the query execution ID and create the query execution
  #query_execution_id = str(uuid.uuid4())
  athena_response = athena_client.start_query_execution(
    QueryString=query_string,
    ResultConfiguration={'OutputLocation': s3_output_location}
  )
  query_execution_id=athena_response['QueryExecutionId']

  # Wait for the query to complete
  athena_status = 'RUNNING'
  while not athena_status == 'SUCCEEDED':
      athena_response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
      athena_status = athena_response['QueryExecution']['Status']['State']
      if athena_status == 'FAILED' or athena_status == 'CANCELLED':
        raise Exception('Athena query failed or was cancelled')

  # Get the S3 URL for the query results
  athena_output = athena_response['QueryExecution']['ResultConfiguration']['OutputLocation']
  s3_key = athena_output.replace(s3_output_location, '')

  # Download the query results from S3
  s3_response = s3_client.get_object(Bucket=s3_output_location.replace('s3://', '').replace('/', ''), Key=s3_key)
  s3_data = s3_response['Body'].read()

  # Load the query results into a Pandas DataFrame
  df = pd.read_csv(io.BytesIO(s3_data))
  ll=1

  # Create a BigQuery
  # Set up Google BigQuery client
  project_id = 'idc-sandbox-000'

  #credentials, project_id = google.auth.default()
  with open('/Users/george/idc/secure_files/idc-sandbox-000-5bcdf4f22c98.json') as source:
    info = json.load(source)

  storage_credentials = service_account.Credentials.from_service_account_info(info)
  bigquery_client = bigquery.Client(credentials=storage_credentials, project=project_id)

  #  Set the name of the BigQuery dataset and table you want to create
  bigquery_dataset_name = 'cloudwatch_transfer'
  bigquery_table_name = 'logs2'
  table_id=project_id+"."+bigquery_dataset_name+"."+bigquery_table_name

  bigquery_client.load_table_from_dataframe(df, table_id).result()
  #job_config= bigquery.QueryJobConfig(default_dataset="idc-sandbox-000.cloudwatch_transfer")
  #job= bigquery_client.query('select * from cloudwatch_transfer.logs', job_config=job_config)
  #for row in job.result():
  #  print(row)

  ll=1