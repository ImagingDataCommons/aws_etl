import boto3
import pandas as pd
import datetime
import time
from logging_config import errlogger, progresslogger, mainlogger
import logging
import s3fs
import io


from multiprocessing import Process, Queue
from queue import Empty


aws_key_file="../secure_files/aws_key.csv"
key_data = pd.read_csv(aws_key_file).iloc[0]
access_key = key_data['Access key ID']
secret_key = key_data['Secret access key']
region='us-east-1'

map_bucket='idc-open-data-metadata'
map_folder='idc_v14_dev/uuid_url_map_from_view_pub/5/'
#remap_folder='idc_v14_dev/uuid_url_newmap_two/'
verify_folder='idc_v14_dev/uuid_url_map_verify_pub/5/'
final_folder='idc_v14_dev/uuid_url_map_final_pub/5/'
img_s3_bucket='idc-open-data'
img_folder=''
#mode='check'
#mode='verify'

mode='final_verify'

#map_bucket='gw-new-test'
#map_folder='map/'
#verify_folder='verify/'
#img_s3_bucket='gw-new-test'
#img_folder='test'

img_path=img_s3_bucket
if (len(img_folder)>0):
  img_path=img_path+'/'+img_folder


num_processes=50
s3_client = boto3.client('s3', region_name=region, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
s3fs=s3fs.S3FileSystem(anon=False, key=access_key, secret=secret_key)

def logger_worker(log_queue):
  if mode=="check":  
    error_fh = logging.FileHandler('./logs/check_' + img_s3_bucket + '_err.log')
    progress_fh = logging.FileHandler('./logs/check_' + img_s3_bucket + '_progress.log')
  elif mode=="verify":
    error_fh = logging.FileHandler('./logs/verify_' + img_s3_bucket + '_err.log')
    progress_fh = logging.FileHandler('./logs/verify_' + img_s3_bucket + '_progress.log')
  elif mode=="final_verify":
    error_fh = logging.FileHandler('./logs/final_verify_' + img_s3_bucket + '_err.log')
    progress_fh = logging.FileHandler('./logs/final_verify_' + img_s3_bucket + '_progress.log')
  
  errlogger.addHandler(error_fh)
  progresslogger.addHandler(progress_fh)
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


def verify_some_blobs(nxt_task, log_queue):
  blob_set_nm=nxt_task[0]
  log_queue.put(('progress', 'about to verify files in ' + blob_set_nm + ' at ' + str(datetime.datetime.now())))
  blob_set_filenm=blob_set_nm.rsplit('/',1)[-1]
  if mode=="check":
    blob_set_report=verify_folder+'check_'+blob_set_filenm
  elif mode=="verify":  
    blob_set_report=verify_folder+'verify_'+blob_set_filenm
  elif mode=="final_verify":
    blob_set_report=final_folder+'final_'+blob_set_filenm  
  blob_num=nxt_task[1]
  uri=f"s3://"+map_bucket+'/'+blob_set_nm
  blob_set = s3_client.get_object(Bucket=map_bucket, Key=blob_set_nm)
  file = blob_set['Body'].read()
  df = pd.read_parquet(io.BytesIO(file))
  df2= df[['uuid','i_hash','pub_aws_bucket','pub_aws_url']].copy()
  df2['etag'],df2['file_found']=['' for i in range(2)]
  if mode=="final_verify":
    df2['size']='0'  
    df2['oetag'],df2['ofile_found']=['' for i in range(2)]
    df2['osize']='0'
  numfiles=0
  numerr=0
  for i in range(len(df2.index)):
    numfiles=numfiles+1
    url = df2.at[i, 'pub_aws_url']
    exp_hash= df2.at[i, 'i_hash']
    urlprts = url.split('/')
    nm = urlprts[-1]
    series = urlprts[-2]
    dest_bucket=img_s3_bucket

    if (len(img_folder)>0):
      dest_obj=img_folder+'/'+series+'/'+nm
      source_obj= img_folder+'/'+nm
    else:
      dest_obj=series+'/'+nm
      source_obj=nm
    if mode=="check":
      cur_obj=source_obj
    else:
      cur_obj=dest_obj  
    try:
      cur_head=s3_client.head_object(Bucket=dest_bucket, Key=cur_obj)
      df2.at[i, 'file_found']='true'
      df2.at[i, 'etag']=cur_head['ETag'].strip('"')
      if mode=="final_verify":
          df2['size']=str(cur_head['ContentLength'])

      if not (df2.at[i, 'etag']==exp_hash):
        log_queue.put(('err', "hash mismatch for file "+cur_obj+", "+series+", "+blob_set_nm+", "+df2.at[i, 'etag']+", "+exp_hash))
        numerr=numerr+1
    except:
      df2.at[i, 'file_found'] = 'false'
      log_queue.put(('err',"file "+cur_obj+" not found, "+series+", "+blob_set_nm))
      numerr=numerr+1
      
    if mode=="final_verify":
      try:
        o_head=s3_client.head_object(Bucket=dest_bucket, Key=source_obj)
        df2.at[i, 'ofile_found']='true'
        df2.at[i, 'oetag']=o_head['ETag'].strip('"')
        df2.at[i, 'osize']=str(o_head['ContentLength'])

      except:
        df2.at[i, 'ofile_found'] = 'false'


  cur_buffer = io.BytesIO()
  df2.to_parquet(cur_buffer)
  cur_buffer.seek(0)
  s3_client.put_object(Body=cur_buffer.getvalue(), Bucket=map_bucket, Key=blob_set_report)
  log_queue.put(('progress', 'just verified '+ str(numfiles)+' files in '+blob_set_nm+', '+str(numerr)+' errors at '+str(datetime.datetime.now())))


def worker(task_queue, log_queue):
  for nxt_task in iter(task_queue.get, 'STOP'):
   verify_some_blobs(nxt_task,log_queue)

def verify_all_blobs():
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
  if mode=="check":
    main_fh = logging.FileHandler('./logs/check_'+img_s3_bucket+'_main.log')
    mainlogger.info("Started checking  bucket "+img_s3_bucket+ " with map folder "+map_folder+" at "+str(datetime.datetime.now()))
  elif mode=="verify":  
    main_fh = logging.FileHandler('./logs/verify_'+img_s3_bucket+'_main.log')
    mainlogger.info("Started verify  bucket "+img_s3_bucket+ " with map folder "+map_folder+" at "+str(datetime.datetime.now()))
  elif mode=="final_verify":
    main_fh = logging.FileHandler('./logs/final_verify_'+img_s3_bucket+'_main.log')
    mainlogger.info("Started verify  bucket "+img_s3_bucket+ " with map folder "+map_folder+" at "+str(datetime.datetime.now()))
  mainlogger.addHandler(main_fh)

  verify_all_blobs()
  if mode=="check":
    mainlogger.info("Finished checking bucket " + img_s3_bucket + " with map folder " + map_folder + " at " + str(datetime.datetime.now()))
  else:
    mainlogger.info("Finished verifying bucket " + img_s3_bucket + " with map folder " + map_folder + " at " + str(datetime.datetime.now()))  



