import boto3
import pandas as pd

import io
import random
import concurrent.futures


aws_key_file="../secure_files/aws_key.csv"
key_data = pd.read_csv(aws_key_file).iloc[0]
access_key = key_data['Access key ID']
secret_key = key_data['Secret access key']
region='us-east-1'
map_bucket='gw-new-test'
map_folder='gw-new-test/map/'
remap_folder='gw-new-test/remap/'
orig_bucket='idc-open-data-two'
img_s3_bucket='gw-new-test'
img_s3_folder='test'
num_processes=1

s3_client = boto3.client('s3', region_name=region, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
urlroot= 's3://'+img_s3_bucket
if (len(img_s3_folder)>0):
  urlroot = urlroot+'/'+img_s3_folder
urlroot=urlroot+'/'
blob_set_nm="idc_v14_dev/uuid_url_map_from_view_two/uuid_url_map_from_view_two_0.parquet"
uri = f"s3://" + map_bucket + '/' + blob_set_nm
blob_set=s3_client.get_object(Bucket='idc-open-data-metadata', Key='uuid_url_map_from_view_two_0.parquet')
file = blob_set['Body'].read()
df = pd.read_parquet(io.BytesIO(file))
num_imgs_per_part = 1000
num_part = 80
num_rows = len(df.index)
ransel = random.sample(range(num_rows), num_imgs_per_part * num_part)
ranselpart = [ransel[i * num_imgs_per_part:(i + 1) * num_imgs_per_part] for i in range(num_part)]


def copy_bloc(k):
  df2 = df.loc[ranselpart[k]].copy().reset_index(drop=True)
  for i in range(len(df2.index)):
    df2.at[i, 'pub_aws_bucket'] = 'gw-new-test'
    url = df2.at[i, 'pub_aws_url']
    urlprts = url.split('/')
    nm = urlprts[-1]
    series = urlprts[-2]
    newurl = urlroot + series + '/' + nm
    df2.at[i, 'pub_aws_url'] = newurl
    dest = img_s3_folder + '/' + nm
    nkey = series + '/' + nm
    s3_client.copy_object(Bucket=img_s3_bucket, Key=dest, CopySource={'Bucket': orig_bucket, 'Key': nkey})

  cur_buffer = io.BytesIO()
  df2.to_parquet(cur_buffer)
  cur_buffer.seek(0)
  s3_client.put_object(Body=cur_buffer.getvalue(), Bucket='gw-new-test', Key='map/test_data_' + str(k) + '.parquet')
  return("done "+str(k))

if __name__ == '__main__':
  #s3_client.copy_object(Bucket=img_s3_bucket, Key='t*st.parquet', CopySource={'Bucket': img_s3_bucket, 'Key': 't*st.parquet'})
  #kk=1
  with concurrent.futures.ProcessPoolExecutor() as executor:
    results = executor.map(copy_bloc,range(num_part))
    for result in results:
      print(result)




