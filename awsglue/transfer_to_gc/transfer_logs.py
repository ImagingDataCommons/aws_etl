import boto3
import time

region='us-east-1'
ds_client = boto3.client('datasync', region_name=region)
ec2_client = boto3.client('ec2', region_name=region)
r=1
task_list={'aws_pub_logs_to_google','aws_cr_logs_to_google','aws_two_logs_to_google'}
ec='DataSync_for_Logs'

if __name__=="__main__":
  get_inst=ec2_client.describe_instances(Filters=[{'Name':'tag:Name', 'Values':['DataSync_for_Logs']}])
  instanceId=get_inst['Reservations'][0]['Instances'][0]['InstanceId']
  state=get_inst['Reservations'][0]['Instances'][0]['State']['Name']
  if (state=='stopped'):
    ec2_client.start_instances(InstanceIds=[instanceId])
    time.sleep(30)

  filters=[{'Name':'Name', 'Values':['aws_pub_logs_to_google'], 'Operator':'Equals'}]
  tasks=ds_client.list_tasks()['Tasks']
  tasks=[tsk for tsk in tasks if (('Name' in tsk) and (tsk['Name'] in task_list))]
  for task in tasks:
    while(True):
      task_desc=ds_client.describe_task(TaskArn=task['TaskArn'])
      if not (task_desc['Status'] == 'UNAVAILABLE'):
        break
      time.sleep(60)

    if (task_desc['Status'] == 'AVAILABLE'):
      ds_client.start_task_execution(TaskArn=task['TaskArn'])
      while(True):
        time.sleep(60)
        cur_desc=ds_client.describe_task(TaskArn=task['TaskArn'])
        if (cur_desc['Status']=='AVAILABLE'):
          break
    time.sleep(30)
  ec2_client.stop_instances(InstanceIds=[instanceId])