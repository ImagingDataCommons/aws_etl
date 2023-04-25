import boto3
import pandas as pd
import io
import sys
import concurrent.futures
from logging_config import errlogger
import logging

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
remaining_original_view="uuid_remaining_original_pub"
img_bucket="idc-open-data"
test=True
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

def keep_final_del_orig(args):
  orig_loc=args[0]
  final_loc=args[1]
  test=args[2]
  orig_ok=False
  final_ok=False
  try:
    orig_head = s3_client.head_object(Bucket=img_bucket, Key=orig_loc)
    orig_ok=True
  except:
    sys.stderr.write('original missing'+'\n')
    orig_ok=False
  try:
    final_head = s3_client.head_object(Bucket=img_bucket, Key=final_loc)
    final_ok=True
  except:
    sys.stderr.write('final missing'+'\n')
    final_ok = False

  if final_ok and orig_ok:
    if not test:
      s3_client.delete_object(Bucket=img_bucket, Key=orig_loc)
    else:
      sys.stderr.write('test. Was going to keep '+final_loc+' but delete '+orig_loc+'\n')
  else:
    sys.stderr.write('could not delete. orig_ok= '+str(orig_ok)+'. final_ok= '+str(final_ok)+'. Test= '+str(test)+'\n')
  return "done"


if __name__=="__main__":
  query_string = "SELECT * FROM "+athena_database_name+"."+remaining_original_view
  df_originals_present=fetch_query_to_dataframe(athena_client, s3_client, s3_output_location,query_string)

  inpA=[]
  for index, row in df_originals_present.iterrows():
    uuid = row['uuid']
    url = row['pub_aws_url']
    series=url.split('/')[3]
    final_loc=series+'/'+uuid+'.dcm'
    orig_loc=uuid+'.dcm'
    inpA.append((orig_loc,final_loc,test))


  with concurrent.futures.ProcessPoolExecutor() as executor:
    executor.map(keep_final_del_orig, inpA)
