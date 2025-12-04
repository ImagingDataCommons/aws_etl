import boto3
import time
import logging
import pprint

logger = logging.getLogger(__name__)

region='us-east-1'
ds_client = boto3.client('datasync', region_name=region)
ec2_client = boto3.client('ec2', region_name=region)
r=1
task_list = {'aws_pub_logs_to_google','aws_cr_logs_to_google','aws_two_logs_to_google'}
VM_FILTER = [{'Name':'tag:Name', 'Values':['DataSync_for_Logs']}]
ec='DataSync_for_Logs'
WAIT_MAX_20 = (20 * 60)
WAIT_MAX_10 = (10 * 60)
WAIT_MAX_60 = (60 * 60)
WAIT_VM_INT = 30
WAIT_TASK_INT = 60
WAIT_BTW_TASK_INT = 30

if __name__=="__main__":
  get_inst=ec2_client.describe_instances(Filters=VM_FILTER)
  instanceId=get_inst['Reservations'][0]['Instances'][0]['InstanceId']
  state=get_inst['Reservations'][0]['Instances'][0]['State']['Name']
  is_stopped = bool(state=='stopped')
  wait = 0
  while is_stopped and wait < WAIT_MAX_20:
    ec2_client.start_instances(InstanceIds=[instanceId])
    time.sleep(WAIT_VM_INT)
    get_inst=ec2_client.describe_instances(Filters=VM_FILTER)
    state=get_inst['Reservations'][0]['Instances'][0]['State']['Name']
    is_stopped = bool(state=='stopped')
    wait = wait+WAIT_VM_INT

  if is_stopped:
    # Unable to start the instance in 20 minutes--something might be wrong
    logger.error("[ERROR] Unable to start instance in 20 minutes! Exiting.")
    exit(1)

  logger.info("[STATUS] EC2 instance started.")
    
  filters=[{'Name':'Name', 'Values':['aws_pub_logs_to_google'], 'Operator':'Equals'}]
  tasks=ds_client.list_tasks()['Tasks']
  tasks=[tsk for tsk in tasks if tsk.get('Name',None) in task_list]
  task_result = {tsk.get('Name',None): 'incomplete' for tsk in tasks}
  for task in tasks:
    wait = 0
    task_desc=ds_client.describe_task(TaskArn=task['TaskArn'])
    is_unavail = bool(task_desc['Status'] == 'UNAVAILABLE')
    while wait < WAIT_MAX_10 and is_unavail:
      time.sleep(WAIT_TASK_INT)
      wait = wait+WAIT_TASK_INT
      task_desc=ds_client.describe_task(TaskArn=task['TaskArn'])
      is_unavail = bool(task_desc['Status'] == 'UNAVAILABLE')

    if (task_desc['Status'] == 'AVAILABLE'):
      ds_client.start_task_execution(TaskArn=task['TaskArn'])
      wait = 0
      is_avail = False
      while wait < WAIT_MAX_60 and not is_avail:
        time.sleep(WAIT_TASK_INT)
        cur_desc=ds_client.describe_task(TaskArn=task['TaskArn'])
        is_avail =  bool(cur_desc['Status']=='AVAILABLE')
        wait = wait+WAIT_TASK_INT
      if not is_avail:
        # Task unavailable after 60 minutes - it might be stuck
        logger.error("[STATUS] Task {} didn't become available in the time alotted ({} minutes) - possibly incomplete.".format(task.get('Name',None),str(WAIT_MAX_60/60)))
        task_result[task.get('Name',None)] = 'over time' 
      else:
        task_result[task.get('Name',None)] = 'complete'                     
    else:
      # Task never became available!
      logger.error("[STATUS] Task {} never became available--skipping.".format(task.get('Name',None))
      pass
    time.sleep(WAIT_BTW_TASK_INT)

  logger.info("[STATUS] Final task dispositions: ")
  logger.info(pprint.pp(task_result,width=10))
  
  ec2_client.stop_instances(InstanceIds=[instanceId])
