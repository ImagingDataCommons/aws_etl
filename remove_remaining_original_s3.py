import boto3
import pandas as pd
import io
import sys
import concurrent.futures
from logging_config import errlogger,progresslogger, mainlogger
import logging
from multiprocessing import Process, Queue
from queue import Empty
import datetime
import time


KEY_FILE="../secure_files/aws_key.csv"
key_data = pd.read_csv(KEY_FILE).iloc[0]
ACCESS_KEY = key_data['Access key ID']
SECRET_KEY = key_data['Secret access key']

# Set up AWS Athena client

athena_client = boto3.client('athena', region_name='us-east-1', aws_access_key_id=ACCESS_KEY,
                             aws_secret_access_key=SECRET_KEY)
s3_client=boto3.client('s3', region_name='us-east-1', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

s3_output_location="s3://athena-results-east1/"
athena_database_name="etl"
remaining_original_view="uuid_remaining_original_pub"
img_bucket="idc-open-data"
test=True
num_processes=1
setlen=200
#setleninv=float(1.0/float(setlen))
#check_table=True
#athena_check_table="uuid_url_map_check_pub"
#athena_verify_table="uuid_url_map_verify_pub"


def fetch_query_to_dataframe(athena_client,s3_client,s3_output_location,query_string):

  # Set the query execution ID and create the query execution
  #query_execution_id = str(uuid.uuid4())
  athena_response = athena_client.start_query_execution(
    QueryString=query_string,
    ResultConfiguration={'OutputLocation': s3_output_location}
  )
  query_execution_id=athena_response['QueryExecutionId']

  # Wait for the query to complete
  athena_status = 'RUNNING'
  while not athena_status == 'SUCCEEDED':
      athena_response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
      athena_status = athena_response['QueryExecution']['Status']['State']
      if athena_status == 'FAILED' or athena_status == 'CANCELLED':
        raise Exception('Athena query failed or was cancelled')

  # Get the S3 URL for the query results
  athena_output = athena_response['QueryExecution']['ResultConfiguration']['OutputLocation']
  s3_key = athena_output.replace(s3_output_location, '')

  # Download the query results from S3
  s3_response = s3_client.get_object(Bucket=s3_output_location.replace('s3://', '').replace('/', ''), Key=s3_key)
  s3_data = s3_response['Body'].read()

  # Load the query results into a Pandas DataFrame
  df = pd.read_csv(io.BytesIO(s3_data))
  return df


def logger_worker(log_queue):
  error_fh = logging.FileHandler('./logs/remove_err.log')
  progress_fh = logging.FileHandler('./logs/remove_progress.log')
  errlogger.addHandler(error_fh)
  progresslogger.addHandler(progress_fh)
  while True:
    try:
      msg_tsk = log_queue.get(True, 3)
      if msg_tsk == 'STOP':
        break
      else:
        type = msg_tsk[0]
        msg = msg_tsk[1]
        if type == "progress":
          progresslogger.info(msg)
        elif type == "err":
          errlogger.info(msg)
    except Empty:
      pass

def remove_some_blobs(nxt_task, log_queue):
  done=0
  already_done=0
  error=0
  check=0
  blob_set=nxt_task
  attempt = len(blob_set)

  if test:
    log_queue.put(
      ('progress', 'about to check files ' + str(len(blob_set)) + ' files at ' + str(datetime.datetime.now())))
  else:
    log_queue.put(('progress', 'about to remove files ' + str(len(blob_set)) + ' files at ' + str(datetime.datetime.now())))
  for cinp in blob_set:
    orig_loc=cinp[0]
    final_loc=cinp[1]
    try:
      ret=keep_final_del_orig(orig_loc, final_loc, test)
      if len(ret['error'])>0:
        log_queue.put(('err', ret['error']))
      if ret['done']:
        done=done+1
      elif ret['final_ok'] and not ret['orig_ok']:
        already_done=already_done+1
      elif test and ret['check']:
        check=check+1
      else:
        error=error+1
        log_queue.put(('err', 'could not successfully delete duplicate of' + final_loc))
    except:
      log_queue.put(('err','could not run del for '+final_loc))
  if test:
    log_queue.put(('progress', 'checking possiblitiy to remove files ' + str(len(blob_set)) + ' files at ' + str(
      datetime.datetime.now()) + ', check=' + str(check) + ' ,already = ' + str(already_done) + ' errors= ' + str(error)))
  else:
    log_queue.put(('progress', 'attempted to remove files ' + str(len(blob_set)) + ' files at ' + str(datetime.datetime.now()) +', done='+str(done)+' ,already = '+str(already_done)+ ' errors= '+str(error)))

def keep_final_del_orig(orig_loc,final_loc,test):
  done=False
  ret={}
  err=""
  progress=""
  orig_ok=False
  final_ok=False
  check=False
  try:
    orig_head = s3_client.head_object(Bucket=img_bucket, Key=orig_loc)
    orig_ok=True
  except:
    #err=err+'original missing'
    orig_ok=False
  try:
    final_head = s3_client.head_object(Bucket=img_bucket, Key=final_loc)
    final_ok=True
  except:
    err=err+'missing '+final_loc
    final_ok = False

  if final_ok and orig_ok:
    if not test:
      s3_client.delete_object(Bucket=img_bucket, Key=orig_loc)
      done=True
    else:
      check=True
      progress=progress+'test. Was going to keep '+final_loc+' but delete '+orig_loc
  else:
    err=err+'could not delete. orig_ok= '+str(orig_ok)+'. final_ok= '+str(final_ok)+'. Test= '+str(test)

  ret={'done':done, 'error':err, 'progress':progress, 'final_ok':final_ok, 'orig_ok':orig_ok, 'check':check}
  return ret

def worker(task_queue, log_queue):
  for nxt_task in iter(task_queue.get, 'STOP'):
   remove_some_blobs(nxt_task,log_queue)

def remove_all_blobs(inpA):
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
  nsets = int(len(inpA)/float(setlen)) + 1
  lst = len(inpA)
  for j in range(nsets):
    ept = min(lst, (j + 1) * setlen)
    inpC = inpA[j * setlen:ept]
    task_queue.put(inpC)
  mainlogger.info('Primary work distribution complete removing; {} blobs'.format(len(inpA)))

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

  if test:
    mainlogger.info("Finished checking possibility to remove extra files from  bucket " + img_bucket + " using uuids in " + remaining_original_view + " at " + str(
    datetime.datetime.now()))
  else:
    mainlogger.info(
      "Finished removing extra files from  bucket " + img_bucket + " using uuids in " + remaining_original_view + " at " + str(
        datetime.datetime.now()))


if __name__=="__main__":

  main_fh = logging.FileHandler('./logs/remove_main.log')
  if test:
    mainlogger.info("Started checking possibility to remove extra files from  bucket " + img_bucket + " using uuids in " + remaining_original_view + " at " + str(
    datetime.datetime.now()))
  else:
    mainlogger.info(
      "Started removing extra files from  bucket " + img_bucket + " using uuids in " + remaining_original_view + " at " + str(
        datetime.datetime.now()))

  query_string = "SELECT * FROM "+athena_database_name+"."+remaining_original_view+' limit 600'
  df_originals_present=fetch_query_to_dataframe(athena_client, s3_client, s3_output_location,query_string)

  inpA=[]
  for index, row in df_originals_present.iterrows():
    uuid = row['uuid']
    url = row['pub_aws_url']
    series=url.split('/')[3]
    final_loc=series+'/'+uuid+'.dcm'
    orig_loc=uuid+'.dcm'
    inpA.append((orig_loc,final_loc))

  remove_all_blobs(inpA)

  '''with concurrent.futures.ProcessPoolExecutor() as executor:
    executor.map(keep_final_del_orig, inpA)'''
