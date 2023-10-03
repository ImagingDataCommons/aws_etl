import boto3
import pandas as pd
import hashlib

KEY_FILE="../secure_files/aws_key.csv"
key_data = pd.read_csv(KEY_FILE).iloc[0]
ACCESS_KEY = key_data['Access key ID']
SECRET_KEY = key_data['Secret access key']
img_bucket='idc-open-data'
#img_key='000009a8-6fb2-479f-bc17-d5bfe559703d/06c6d762-b82a-4183-b3a0-c18448f0a394.dcm'
img_key1='e4b81d12-9867-4d88-abbb-b4936c9b8b5d.dcm'
img_key2='66e110b0-c382-42cc-b135-f1a814512c2e/e4b81d12-9867-4d88-abbb-b4936c9b8b5d.dcm'
s3_client=boto3.client('s3', region_name='us-east-1', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

cur_head1=s3_client.head_object(Bucket=img_bucket, Key=img_key1)
cur_head2=s3_client.head_object(Bucket=img_bucket, Key=img_key2)
kk=1

#kk=1
#cur_obj=s3_client.get_object(Bucket=img_bucket, Key=img_key)
#bd=cur_obj['Body'].read()
#tmp=hashlib.md5(bd).hexidigest()

#rr=1
#cur

