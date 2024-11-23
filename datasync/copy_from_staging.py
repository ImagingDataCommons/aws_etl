import boto3
from google.cloud import bigquery
from python_settings import settings
import csv

import settings as etl_settings
settings.configure(etl_settings)
assert settings.configured


default_project = settings.DEFAULT_PROJECT
default_dataset = settings.DEFAULT_DATASET
version = settings.CURRENT_VERSION

aws_access_key_id = settings.PROD_AWS_ACCESS_KEY_ID
aws_secret_access_key = settings.PROD_AWS_SECRET_ACCESS_KEY
aws_session_token = settings.PROD_AWS_ACCESS_TOKEN
region = settings.REGION
cur_date = settings.DATE

manifest_file= "manifest_"+cur_date+".csv"
manifest_file_small = "manifest_small_"+cur_date+".csv"
manifest_file_large = "manifest_large_"+cur_date+".csv"
report_file_large = "report_large_"+cur_date+".csv"

copy_name = "copy_"+cur_date
account_id= settings.PROD_ACCOUNT_ID
role_arn = settings.S3_ROLE_ARN
staging_bucket = settings.STAGING_BUCKET
destination_bucket = settings.DESTINATION_BUCKET

def setUpBatchCopy():
    s3_client = boto3.client('s3', region_name=region, aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token)
    s3_control = boto3.client('s3control', region_name=region, aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token)

    file_data = s3_client.get_object_attributes(Bucket='idc-manifests', Key=manifest_file, ObjectAttributes=['ETag'])
    etag = file_data['ETag']

    response = s3_control.create_job(
        AccountId=account_id,
        ConfirmationRequired=True,
        Tags=[{'Key': 'name', 'Value': copy_name}],
        Operation={
            'S3PutObjectCopy': {
                'TargetResource': 'arn:aws:s3:::' + destination_bucket,
                'BucketKeyEnabled': False,
                'ChecksumAlgorithm': 'SHA256'
            }
        },

        Report={
            'Bucket': 'arn:aws:s3:::idc-copy-reports',
            'Format': 'Report_CSV_20180820',
            'Enabled': True,
            'ReportScope': 'AllTasks'
        },
        Manifest={
            'Spec': {
                'Format': 'S3BatchOperations_CSV_20180820',
                'Fields': ['Bucket', 'Key']},
            'Location': {
                'ObjectArn': 'arn:aws:s3:::idc-manifests/' + manifest_file_small,
                'ETag': etag
            }
        },
        Priority=10,
        RoleArn=role_arn
    )
def dealWithLargeFiles():
    resp=[]
    s3_client = boto3.client('s3', region_name=region, aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token)
    with open(manifest_file_large) as csvfile:

      large_file_manifest = csv.reader(csvfile)
      for row in large_file_manifest:
        bucket = row[0]
        file = row[1]
        copy_source = {'Bucket': bucket, 'Key': file}
        s3_client.copy(CopySource=copy_source, Bucket=destination_bucket, Key=file)
        file_data = s3_client.get_object_attributes(Bucket=destination_bucket, Key=file, ObjectAttributes=['ETag', 'ObjectSize'])
        rw = destination_bucket+","+file+","+str(file_data['ObjectSize']+"\n")
        resp.append(rw)
      f = open(report_file_large, "w")
      f.writelines(resp)
      f.close()

if __name__=="__main__":
    #setUpBatchCopy()
    dealWithLargeFiles()



