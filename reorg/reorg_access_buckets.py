import boto3
import pandas as pd
#from concurrent.futures import ThreadPoolExecutor, as_completed



aws_key_file="../secure_files/aws_key.csv"
region='us-east-1'



if __name__=="__main__":
  key_data = pd.read_csv(aws_key_file).iloc[0]
  access_key = key_data['Access key ID']
  secret_key = key_data['Secret access key']
  session = boto3.Session(aws_access_key_id=access_key, aws_secret_access_key=secret_key)

  # Then use the session to get the resource
  s3 = session.resource('s3')
  bucket=s3.Bucket('idc-open-data-logs')
  print(len(list(bucket.objects.all())))
  kk=1
  #print(len(list(objects)))


