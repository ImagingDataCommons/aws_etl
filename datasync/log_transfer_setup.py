import boto3
import pandas as pd
from datasync_setup import createLocalLocation, createGoogleLocation

'''transfer_tasks=[{'localBucket':'idc-open-data-logs','googleBucket':'aws_request_logs', 'localSubDir':'conv',
                 'googleSubDir':'public',
 'localLocationNm':'aws-public-logs', 'googleLocationNm':'google-public-logs',
 'googlePublicBucket':False,'logGrpPrefix':'pub', 'logGrpName':'pub-logs'},
{'localBucket':'idc-open-data-two-logs','googleBucket':'aws_request_logs', 'localSubDir':'conv',
                 'googleSubDir':'two',
 'localLocationNm':'aws-two-logs', 'googleLocationNm':'google-two-logs',
 'googlePublicBucket':False,'logGrpPrefix':'two', 'logGrpName':'two-logs'},
{'localBucket':'idc-open-data-cr-logs','googleBucket':'aws_request_logs', 'localSubDir':'conv',
                 'googleSubDir':'cr',
 'localLocationNm':'aws-cr-logs', 'googleLocationNm':'google-cr-logs',
 'googlePublicBucket':False,'logGrpPrefix':'cr', 'logGrpName':'cr-logs'}
                ]
                '''

transfer_tasks=[{'localBucket':'aws-cloudtrail-logs-266665233841-996bfd04','googleBucket':'aws_cloudtrail_2666', 'localSubDir':'',
                 'googleSubDir':'','localLocationNm':'aws-cloudtrail_2666', 'googleLocationNm':'google-cloudtrail-2666',
 'googlePublicBucket':False,'logGrpPrefix':'2666', 'logGrpName':'2666-cloudtrail'},
{'localBucket':'aws-cloudtrail-logs-051845558647-22','googleBucket':'aws_cloudtrail_05184', 'localSubDir':'',
                 'googleSubDir':'','localLocationNm':'aws-cloudtrail_05184', 'googleLocationNm':'google-cloudtrail-05184',
 'googlePublicBucket':False,'logGrpPrefix':'05184', 'logGrpName':'05184-cloudtrail'},


]
transfer_tasks=[{'localBucket':'aws-cloudtrail-logs-266665233841-996bfd04','googleBucket':'aws_test2', 'localSubDir':'',
                 'googleSubDir':'','localLocationNm':'aws_tst', 'googleLocationNm':'google_tst',
 'googlePublicBucket':False,'logGrpPrefix':'2666', 'logGrpName':'2666-cloudtrail'}]

# must provide name of datasync VM and agent ARN. Can get these from console.

ec2_key_name='DataSync_for_Logs'
agent_arn='arn:aws:datasync:us-east-1:266665233841:agent/agent-0b6e21d6e55f34611'
region='us-east-1'


aws_key_file="../secure_files/aws_key.csv"
google_key_file="../secure_files/aws_to_google_hmac2.csv"
role_arn = 'arn:aws:iam::266665233841:role/George_DataSync_Role_Block1'

if __name__=="__main__":
  key_data = pd.read_csv(aws_key_file).iloc[0]
  access_key = key_data['Access key ID']
  secret_key = key_data['Secret access key']
  google_key_data = pd.read_csv(google_key_file).iloc[0]
  google_access_key = google_key_data['Access key ID']
  google_secret_key = google_key_data['Secret access key']
  ds_client = boto3.client('datasync', region_name=region, aws_access_key_id=access_key,aws_secret_access_key=secret_key)

  for task in transfer_tasks:
    local_bucket=task['localBucket']
    local_sub_dir = task['localSubDir']
    local_location_name = task['localLocationNm']
    local_loc_arn = createLocalLocation(ds_client, role_arn, local_bucket, local_sub_dir, local_location_name)

    google_bucket = task['googleBucket']
    google_sub_dir = task['googleSubDir']
    google_location_name = task['googleLocationNm']
    google_loc_arn = createGoogleLocation(ds_client, google_bucket, google_sub_dir, google_access_key, google_secret_key,google_location_name, agent_arn)





