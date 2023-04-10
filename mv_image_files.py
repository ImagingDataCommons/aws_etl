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
map_bucket='idc-open-data-metadata'
map_folder='idc_v14_dev/uuid_url_map_from_view_pub/0/'
remap_folder='idc_v14_dev/uuid_url_newmap_pub/0/'
img_s3_bucket='idc-open-data'
img_folder=''

map_bucket='gw-new-test'
map_folder='map/'
remap_folder='map2/'
img_s3_bucket='gw-new-test'
img_folder='test'

img_path=img_s3_bucket
if (len(img_folder)>0):
  img_path=img_path+'/'+img_folder


num_processes=16
s3_client = boto3.client('s3', region_name=region, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
s3fs=s3fs.S3FileSystem(anon=False, key=access_key, secret=secret_key)

def logger_worker(log_queue):
  error_fh = logging.FileHandler('./logs/' + img_s3_bucket + '_err.log')
  errlogger.addHandler(error_fh)
  errlogger.info("test")
  progress_fh = logging.FileHandler('./logs/' + img_s3_bucket + '_progress.log')
  progresslogger.addHandler(progress_fh)
  progresslogger.info("test")
  progresslogger.info("test4")
  errlogger.info("test4")
  while True:
    try:
      msg_tsk=log_queue.get(True, 3)
      if msg_tsk=='STOP':
        break
      else:
        type=msg_tsk[0]
        msg=msg_tsk[1]
        if type=="progress":
          progresslogger.info(msg)
        elif type=="err":
          errlogger.info(msg)
    except Empty:
      pass


def move_some_blobs(nxt_task, log_queue):
  blob_set_nm=nxt_task[0]
  log_queue.put(('progress', 'about to move files in ' + blob_set_nm + ' at ' + str(datetime.datetime.now())))
  blob_set_filenm=blob_set_nm.rsplit('/',1)[-1]
  blob_set_report=remap_folder+'rep'+blob_set_filenm
  blob_num=nxt_task[1]
  uri=f"s3://"+map_bucket+'/'+blob_set_nm
  blob_set = s3_client.get_object(Bucket=map_bucket, Key=blob_set_nm)
  file = blob_set['Body'].read()
  df = pd.read_parquet(io.BytesIO(file))
  df2= df[['uuid','i_hash','pub_aws_bucket','pub_aws_url']].copy()
  df2['op'],df2['err'],df2['warn'],df2['destination_etag'],df2['source_etag'],df2['copy_ok']=['' for i in range(6)]

  for i in range(len(df2.index)):
    url = df2.at[i, 'pub_aws_url']
    exp_bucket= df2.at[i, 'pub_aws_bucket']
    exp_hash= df2.at[i, 'i_hash']
    uuid = df2.at[i, 'uuid']
    urlprts = url.split('/')
    bucket_in_url = urlprts[2]
    nm = urlprts[-1]
    uuid_in_url=nm.split('.')[0]
    series = urlprts[-2]
    source_bucket=img_s3_bucket
    dest_bucket=img_s3_bucket
    if (len(img_folder)>0):
      source_obj=img_folder+'/'+nm
      dest_obj=img_folder+'/'+series+'/'+nm
    else:
      source_obj=nm
      dest_obj=series+'/'+nm
    if (uuid==uuid_in_url) and (exp_bucket == bucket_in_url) and (exp_bucket == img_s3_bucket):
      for attempt in range(3):
        try:
          ret=move_safe(s3_client, source_bucket, source_obj, dest_bucket, dest_obj, False, exp_hash,True)
          df2.at[i,'op']=str(ret['op'])
          df2.at[i,'err'], df2.at[i,'warn'], df2.at[i,'destination_etag'], df2.at[i,'source_etag'], df2.at[i,'copy_ok'] = [ret['err'],ret['warn'],ret['destination_etag'],ret['source_etag'], ret['copy_ok']]
          if (len(ret['err'])>0):
            log_queue.put(('err',uuid+":"+ret['err']))
          if (len(ret['warn'])>0):
            log_queue.put(('err',uuid+":"+ret['warn']))
          break
        except Exception as e:
          log_queue.put(('err', uuid + ": attempt " +str(attempt) + ":" + str(e)))
    else:
      log_queue.put(('err', uuid + ":mismatch in bucket or uuid data with src bucket "+img_s3_bucket))


  cur_buffer = io.BytesIO()
  df2.to_parquet(cur_buffer)
  cur_buffer.seek(0)
  s3_client.put_object(Body=cur_buffer.getvalue(), Bucket=map_bucket, Key=blob_set_report)
  log_queue.put(('progress', 'just moved files in '+blob_set_nm+' at '+str(datetime.datetime.now())))


def worker(task_queue, log_queue):
  for nxt_task in iter(task_queue.get, 'STOP'):
   #nxt_task=task_queue.get()
   #s3_client = boto3.client('s3', region_name=region, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
   move_some_blobs(nxt_task,log_queue)

def move_all_blobs():
  processes = []
  task_queue = Queue()
  log_queue = Queue()
  strt = time.time()

  # Start worker processes
  for process in range(num_processes):
    processes.append(
      Process(group=None, target=worker, args=(task_queue,log_queue)))
    processes[-1].start()
  processes.append(
    Process(group=None, target=logger_worker, args=(log_queue,)))
  processes[-1].start()

  # Distribute the work across the task_queues

  map_bucket_contents = s3_client.list_objects_v2(Bucket=map_bucket, Prefix=map_folder)['Contents']
  for n, obj in enumerate(map_bucket_contents):
    if obj['Key'].endswith('parquet'):
      task_queue.put((obj['Key'], n))
  mainlogger.info('Primary work distribution complete; {} blobs'.format(len(map_bucket_contents)))
  
  # Tell child processes to stop
  for i in range(num_processes):
    task_queue.put('STOP')


  # Wait for process to terminate
  for process in processes[:-1]:
    print(f'Joining process: {process.name}, {process.is_alive()}')
    process.join()

  log_queue.put('STOP')
  print(f'Joining process: {processes[-1].name}, {processes[-1].is_alive()}')
  processes[-1].join()


if __name__ == '__main__':
  main_fh = logging.FileHandler('./logs/'+img_s3_bucket+'_main.log')
  mainlogger.addHandler(main_fh)
  mainlogger.info("Started moving bucket "+img_s3_bucket+ " with map folder "+map_folder+" at "+str(datetime.datetime.now()))

  move_all_blobs()
  mainlogger.info("Finished moving bucket " + img_s3_bucket + " with map folder " + map_folder + " at " + str(datetime.datetime.now()))



