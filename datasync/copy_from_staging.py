import boto3
from google.cloud import bigquery
from python_settings import settings
import pandas

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

account_id= settings.PROD_ACCOUNT_ID
role_arn = settings.S3_ROLE_ARN
staging_bucket = settings.STAGING_BUCKET

if __name__=="__main__":
    client = bigquery.Client(project=default_project)
    query = "select "+settings.STAGING_BUCKET+", CONCAT(se_uuid, '/', i_uuid, '.dcm') from " + default_project + "." + default_dataset + ".all_joined_public_and_current where i_rev_idc_version = "+version
    # Run the query and export the results to GCS
    query_job = client.query(query)

    #destination_blob = storage_client.bucket(bucket_name).blob(blob_name)
    #destination_blob.content_type = 'text/csv'
    #query_job.result().to_dataframe().to_csv('./test.csv', index=False, header )

    #df = query_job.result().to_dataframe()
    #df.to_csv(manifest_file, index=False, header = False)
    f =1
    s3_resource = boto3.resource('s3', region_name=region, aws_access_key_id=aws_access_key_id,
                                  aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token)

    s3_client = boto3.client('s3', region_name=region, aws_access_key_id=aws_access_key_id,
                                 aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token)

    s3_control = boto3.client('s3control', region_name=region, aws_access_key_id=aws_access_key_id,
                                 aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token)

    #s3_resource.meta.client.upload_file(manifest_file,'idc-manifests',manifest_file)
    #file_data = s3_client.get_object_attributes(Bucket='idc-manifests', Key='test.csv', ObjectAttributes=['ETag'])
    #etag = file_data['ETag']


    f=1

    response = s3_control.create_job(
        AccountId=account_id,
        ConfirmationRequired=False,
        Operation={

            'S3PutObjectCopy': {
                'TargetResource': 'arn:aws:s3:::gw-new-test',
                'BucketKeyEnabled': False,
                'ChecksumAlgorithm': 'SHA256'
            }

        },

        Report = {
            'Bucket':'arn:aws:s3:::idc-copy-reports',
            'Format':'Report_CSV_20180820',
            'Enabled':True,
            'ReportScope':'AllTasks'
        },
        Manifest={
            'Spec':{
                'Format': 'S3BatchOperations_CSV_20180820',
                'Fields': ['Bucket', 'Key'],

            },
            'Location':{
                'ObjectArn': 'arn:aws:s3:::idc-manifests/test.csv',
                'ETag': '22ccee92939ee5f245f79837d4ce6793'
            }
        },
        Priority=10,
        RoleArn=role_arn

    )
