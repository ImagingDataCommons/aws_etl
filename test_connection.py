import boto3
import pandas as pd
import datetime
import time
from logging_config import errlogger, progresslogger, mainlogger
import logging
import s3fs
import io
from move_safe import move_safe


from multiprocessing import Process, Queue
from queue import Empty


aws_key_file="../secure_files/aws_key.csv"
key_data = pd.read_csv(aws_key_file).iloc[0]
access_key = key_data['Access key ID']
secret_key = key_data['Secret access key']
region='us-east-1'
s3_client = boto3.client('s3', region_name=region, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
map_bucket_contents = s3_client.list_objects_v2(Bucket='aws-cloudtrail-logs-051845558647-934d3c5b', Prefix='AWSLogs/051845558647/CloudTrail')
tmp=1
cur_obj='AWSLogs/051845558647/CloudTrail/us-east-1/2023/04/03/051845558647_CloudTrail_us-east-1_20230403T2135Z_bQrqKUA2E0PDHDGU.json.gz'
cur_head=s3_client.head_object(Bucket='aws-cloudtrail-logs-051845558647-934d3c5b', Key='tmp2.txt')

cur_head=s3_client.head_object(Bucket='aws-cloudtrail-logs-051845558647-934d3c5b', Key='AWSLogs/051845558647/CloudTrail/')
tmp=2