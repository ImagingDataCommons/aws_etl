import boto3
import pandas as pd

import io
import random
import pyarrow as pa
import pyarrow.parquet as pq

from copy_safe import copy_safe


from multiprocessing import Process, Queue


aws_key_file="../secure_files/aws_key.csv"
key_data = pd.read_csv(aws_key_file).iloc[0]
access_key = key_data['Access key ID']
secret_key = key_data['Secret access key']
region='us-east-1'
map_bucket='gw-new-test'
map_folder='gw-new-test/map/'
remap_folder='gw-new-test/remap/'
orig_bucket='idc-open-data-two'
img_s3_bucket='gw_new-test'
img_s3_folder='test'
num_processes=1
s3_client = boto3.client('s3', region_name=region, aws_access_key_id=access_key, aws_secret_access_key=secret_key)

urlroot= 's3://'+img_s3_bucket
if (len(img_s3_folder)>0):
  urlroot = urlroot+'/'+img_s3_folder
urlroot=urlroot+'/'

if __name__ == '__main__':
  num_imgs=100
  num_ser=20
  ser=[]
  for i in range(num_ser):
    ser.append('/test/ser'+str(i))
  #fs=pa.filesystem.S3FSWrapper(region=region, access_key=access_key, secret_key=secret_key)
  #  .fs.S3FileSystem(region=region, access_key=access_key, secret_key=secret_key)
  blob_set_nm="idc_v14_dev/uuid_url_map_from_view_two/uuid_url_map_from_view_two_0.parquet"
  uri = f"s3://" + map_bucket + '/' + blob_set_nm
  #blob_set = pd.read_parquet("s3://idc-open-data-metadata/uuid_url_map_from_view_two_0.parquet", storage_options={"key": access_key, "secret": secret_key})
  blob_set=s3_client.get_object(Bucket='idc-open-data-metadata', Key='uuid_url_map_from_view_two_0.parquet')
  file = blob_set['Body'].read()
  df = pd.read_parquet(io.BytesIO(file))
  num_rows= len(df.index)
  ransel=random.sample(range(num_rows), num_imgs)
  df2=df.loc[ransel].copy()
  with open("test_manifest.csv","w") as f:

    for index, row in df2.iterrows():
      row['pub_aws_bucket']='gw-new-test'
      url=row['pub_aws_url']
      urlprts=url.split('/')
      nm=urlprts[-1]
      series=urlprts[-2]
      newurl=urlroot+series+'/'+nm
      row['pub_aws_url']=newurl
      f.write(orig_bucket+", "+nm+"\n")
  ss=1



