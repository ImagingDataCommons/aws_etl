import boto3
import pandas as pd

KEY_FILE="../secure_files/aws_key.csv"
key_data = pd.read_csv(KEY_FILE).iloc[0]
ACCESS_KEY = key_data['Access key ID']
SECRET_KEY = key_data['Secret access key']
src_bucket="aws-cloudtrail-logs-051845558647-934d3c5b"
dest_bucket="aws-cloudtrail-logs-051845558647-22"


s3_client=boto3.client('s3', region_name='us-east-1', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

blob_set = s3_client.get_object(Bucket='idc-open-data-metadata', Key='gw_temp/testmv/recipe1.jpg')

obj_list=s3_client.list_objects_v2(Bucket=src_bucket, Prefix='')['Contents']
for obj in obj_list:
  if not obj['Key'].endswith('/'):
    key=obj['Key']
    rr=1
    blob_set = s3_client.get_object(Bucket=src_bucket, Key=key)
    s3_client.copy_object(Bucket=dest_bucket, Key=key, CopySource={'Bucket': src_bucket, 'Key': key})
    rr=1
kk=1

#s3_client.copy_object(Bucket=img_bucket, Key=dest_loc,
#                          CopySource={'Bucket': img_bucket, 'Key': src_loc})