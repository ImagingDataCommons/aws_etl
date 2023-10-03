import boto3
import uuid
import json
import pandas as pd
import io
import time
import move_safe


KEY_FILE="../secure_files/aws_key.csv"
key_data = pd.read_csv(KEY_FILE).iloc[0]
ACCESS_KEY = key_data['Access key ID']
SECRET_KEY = key_data['Secret access key']

# Set up AWS Athena client
athena_client = boto3.client('athena', region_name='us-east-1', aws_access_key_id=ACCESS_KEY,
                             aws_secret_access_key=SECRET_KEY)
s3_client=boto3.client('s3', region_name='us-east-1', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

s3_output_location="s3://athena-results-east1/"
athena_database_name="etl"
not_copied_view="uuids_not_copied_view_two"
img_bucket="idc-open-data-two"
#check_table=True
#athena_check_table="uuid_url_map_check_pub"
#athena_verify_table="uuid_url_map_verify_pub"


def fetch_query_to_dataframe(athena_client,s3_client,s3_output_location,query_string):

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
  return df

if __name__=="__main__":
  query_string = "SELECT * FROM "+athena_database_name+"."+not_copied_view
  #params=["false"]
  df_not_copied=fetch_query_to_dataframe(athena_client, s3_client, s3_output_location,query_string)

  for index, row in df_not_copied.iterrows():
    uuid = row['uuid']
    url = row['pub_aws_url']
    src_series=url.split('/')[3]
    src_loc=uuid+'.dcm'

    dest_series=row['pub_aws_url'].split('/')[3]
    dest_loc = dest_series + '/' + uuid+'.dcm'
    try:
      s3_client.copy(Bucket=img_bucket, Key=dest_loc,
                          CopySource={'Bucket': img_bucket, 'Key': src_loc})
    except Exception as e:
      print(str(e))
    tmp=1

