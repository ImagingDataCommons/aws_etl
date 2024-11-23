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
manifest_file_small = "manifest_small_"+cur_date+".csv"
manifest_file_large = "manifest_large_"+cur_date+".csv"
cutoff=settings.S3_CUTOFF_SIZE

if __name__=="__main__":
    client = bigquery.Client(project=default_project)
    query = "select '" + settings.STAGING_BUCKET + "' as bucket, CONCAT(se_uuid, '/', i_uuid, '.dcm') as file, i_size from " + default_project + "." + default_dataset + ".all_joined_public_and_current where i_rev_idc_version = "+version
    # Run the query and export the results to GCS
    query_job = client.query(query)

    df_total = query_job.result().to_dataframe()
    df_small = df_total.query('i_size < ' + str(cutoff))
    df_small = df_small[["bucket", "file"]]
    df_small.to_csv(manifest_file_small, index=False, header = False)

    df_large = df_total.query('i_size >= ' + str(cutoff))
    df_large = df_large[["bucket", "file"]]
    df_large.to_csv(manifest_file_large, index=False, header = False)

    s3_resource = boto3.resource('s3', region_name=region, aws_access_key_id=aws_access_key_id,
                                  aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token)


    s3_resource.meta.client.upload_file(manifest_file_small,'idc-manifests',manifest_file_small)
    s3_resource.meta.client.upload_file(manifest_file_large, 'idc-manifests', manifest_file_large)
