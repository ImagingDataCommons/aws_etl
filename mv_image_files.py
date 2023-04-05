import boto3
import pandas as pd
import datetime
import time
from logging_config import errlogger, errformatter,progresslogger
import logging
import s3fs
import io
import pyarrow as pa
import pyarrow.parquet as pq

from copy_safe import copy_safe


from multiprocessing import Process, Queue


aws_key_file="../secure_files/aws_key.csv"
key_data = pd.read_csv(aws_key_file).iloc[0]
access_key = key_data['Access key ID']
secret_key = key_data['Secret access key']
region='us-east-1'
map_bucket='idc-open-data-metadata'
map_folder='idc_v14_dev/uuid_url_map_from_view_two/'
remap_folder='idc_v14_dev/uuid_url_newmap_two/'
img_s3_bucket='idc-open-data-two'
num_processes=1
s3_client = boto3.client('s3', region_name=region, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
s3fs=s3fs.S3FileSystem(anon=False, key=access_key, secret=secret_key)

def err_logger_worker(log_queue):
  while True:
    try:
      msg=log_queue.get(True, 3)
      errlogger.debug(msg)
    except Queue.empty:
      pass


def copy_some_blobs(nxt_task, log_queue):
  blob_set_nm=nxt_task[0]
  blob_num=nxt_task[1]
  uri=f"s3://"+map_bucket+'/'+blob_set_nm
  blob_set = s3_client.get_object(Bucket='idc-open-data-metadata', Key='uuid_url_map_from_view_two_0.parquet')
  file = blob_set['Body'].read()
  df = pd.read_parquet(io.BytesIO(file))
  #blob_set= pd.read_parquet(uri, storage_options={"key":access_key, "secret":secret_key})
  rr=1
  #boto3.client('s3', region_name=region, aws_access_key_id=access_key, aws_secret_access_key=secret_key)


def worker(task_queue, log_queue):
  for nxt_task in iter(task_queue.get, 'STOP'):
   #nxt_task=task_queue.get()
   #s3_client = boto3.client('s3', region_name=region, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
   copy_some_blobs(nxt_task,log_queue)

def copy_all_blobs():
  processes = []
  task_queue = Queue()
  log_queue = Queue()
  strt = time.time()


  # Start worker processes
  for process in range(num_processes):
    processes.append(
      Process(group=None, target=worker, args=(task_queue,log_queue)))
    processes[-1].start()
    #process.append(group=None, target=err_logger_worker, args=(log_queue))

  # Distribute the work across the task_queues
  #s3_client = boto3.client('s3', region_name=region, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
  #boto3.client('s3', region_name=region, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
  map_bucket_contents = s3_client.list_objects_v2(Bucket=map_bucket, Prefix=map_folder)['Contents']
  for n, obj in enumerate(map_bucket_contents):
    if obj['Key'].endswith('parquet'):
      task_queue.put((obj['Key'], n))
  #lgf.write('Primary work distribution complete; {} blobs'.format(len(map_bucket_contents)))
  progresslogger.info('Primary work distribution complete; {} blobs'.format(len(map_bucket_contents)))
  
  # Tell child processes to stop
  for i in range(num_processes):
    task_queue.put('STOP')


  # Wait for process to terminate
  for process in processes:
    print(f'Joining process: {process.name}, {process.is_alive()}')
    process.join()


if __name__ == '__main__':

  #fs=pa.filesystem.S3FSWrapper(region=region, access_key=access_key, secret_key=secret_key)
  #  .fs.S3FileSystem(region=region, access_key=access_key, secret_key=secret_key)
  blob_set_nm="idc_v14_dev/uuid_url_map_from_view_two/uuid_url_map_from_view_two_0.parquet"
  uri = f"s3://" + map_bucket + '/' + blob_set_nm
  #blob_set = pd.read_parquet("s3://idc-open-data-metadata/uuid_url_map_from_view_two_0.parquet", storage_options={"key": access_key, "secret": secret_key})
  blob_set=s3_client.get_object(Bucket='idc-open-data-metadata', Key='uuid_url_map_from_view_two_0.parquet')
  file=blob_set['Body'].read()
  df=pd.read_parquet(io.BytesIO(file))


  cur_buffer=io.BytesIO()

  df.to_parquet(cur_buffer)

  cur_buffer.seek(0)
  s3_client.put_object(Body=cur_buffer.getvalue(), Bucket='gw-new-test', Key='test.parquet')
  #with open(cur_buffer,"r") as f:
  #  s3_client.upload_file(f, 'idc-open-data-metadata', 'test.parquet')


  #pq.read_table("s3://idc-open-data-metadata/uuid_url_map_from_view_two_0.parquet", filesystem=s3fs)
  error_fh = logging.FileHandler('./logs/'+img_s3_bucket+'_err.log')
  error_fh.setFormatter(errformatter)
  #errlogger.addHandler(error_fh)
  progress_fh = logging.FileHandler('./logs/'+img_s3_bucket+'_progress.log')
  progresslogger.addHandler(progress_fh)
  progresslogger.info("Copying bucket "+img_s3_bucket+ " with map folder "+map_folder+", starting at "+str(datetime.datetime.now()))
  #s3_client = boto3.client('s3', region_name=region, aws_access_key_id=access_key,aws_secret_access_key=secret_key)
  #r=1
  copy_all_blobs()



