import boto3
import pandas as pd
import subprocess
import stor
import os

if __name__ == '__main__':
  #aws_key_file="../secure_files/aws_key.csv"
  #key_data = pd.read_csv(aws_key_file).iloc[0]
  #access_key = key_data['Access key ID']
  #secret_key = key_data['Secret access key']
  #region='us-east-1'

  bucket='idc-open-data'
  stats = pd.read_csv("./v17_stats.csv")
  #stats.to_csv("./v17_stats_2.csv")

  awsCnt = []
  awsBytes = []
  gCnt = []
  gBytes = []
  for index, row in stats.iterrows():
    series=row['series_uuid']
    counts=row['counts']
    v17_counts=row['v17_counts']
    cmd="/Users/george/s5cmd/s5cmd --no-sign-request --endpoint-url https://s3.amazonaws.com du 's3://idc-open-data/"+series+"/*'"
    gcmd="/Users/george/s5cmd/s5cmd --no-sign-request --endpoint-url https://storage.googleapis.com du 's3://public-datasets-idc/"+series+"/*'"
    #ret=os.system(cmd)

    '''nret=subprocess.check_output(cmd, shell=True).decode("utf-8")
    nretA = nret.split(' ')
    bytes=nretA[0]
    cnt=nretA[3]
    #stout=subprocess.run(['/Users/george/s5cmd/s5cmd', arg])
    awsCnt.append(cnt)
    awsBytes.append(bytes) '''

    nret = subprocess.check_output(gcmd, shell=True).decode("utf-8")
    nretA = nret.split(' ')
    bytes = nretA[0]
    cnt = nretA[3]
    gCnt.append(cnt)
    gBytes.append(bytes)

  stats['gcnt']=gCnt
  stats['gbytes']=gBytes
  stats.to_csv("./v17_stats_3.csv")

