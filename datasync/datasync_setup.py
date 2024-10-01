import boto3
import requests
import time
from functools import reduce
from operator import concat
from python_settings import settings
import settings as etl_settings
settings.configure(etl_settings)
assert settings.configured

region = settings.REGION
my_IPV4 = settings.MY_IPV4
ami_id = settings.AMI_ID
instance_type = settings.INSTANCE_TYPE
role_arn = settings.DS_ROLE_ARN
ec2_key_name = settings.EC2_KEY_NAME
ds_name = settings.DS_NAME
cidr_block = settings.CIDR_BLOCK

aws_access_key_id = settings.DEV_AWS_ACCESS_KEY_ID
aws_secret_access_key = settings.DEV_AWS_SECRET_ACCESS_KEY
aws_session_token = settings.DEV_AWS_ACCESS_TOKEN

hmac_access_key_id = settings.HMAC_ACCESS_KEY_ID
hmac_secret_access_key = settings.HMAC_SECRET_ACCESS_KEY
vpc_name = settings.VPC_NAME

transfer_tasks = settings.TRANSFER_TASKS

def createVPC(ec2_resource,ec2_client,cidr_block,my_IPv4,service_name, vpc_name):
  vpc = ec2_resource.create_vpc(CidrBlock=cidr_block)
  vpc.create_tags(Tags=[{"Key": "Name", "Value": vpc_name}])
  igateway = ec2_resource.create_internet_gateway()
  igateway.create_tags(Tags=[{"Key": "Name", "Value": "new_gtwy"}])
  vpc.attach_internet_gateway(InternetGatewayId=igateway.id)
  route_table = vpc.create_route_table()
  route_table.create_tags(Tags=[{"Key": "Name", "Value": "new_rt"}])
  route=route_table.create_route(DestinationCidrBlock='0.0.0.0/0',GatewayId=igateway.id)

  subnet = ec2_resource.create_subnet(CidrBlock=cidr_block, VpcId=vpc.id)
  subnet.create_tags(Tags=[{"Key": "Name", "Value": "new_sub"}])
  route_table.associate_with_subnet(SubnetId=subnet.id)

  cidr_IP=my_IPv4+'/32'
  sec_group = ec2_resource.create_security_group(GroupName='awsRepEx',Description='configure for AWSRep', VpcId=vpc.id)
  sec_group.authorize_ingress(CidrIp=cidr_IP, IpProtocol='-1')
  sec_group.authorize_ingress(CidrIp='172.16.0.0/16', IpProtocol='-1')

  # outbound rule all traffic created automatically
  #ipPermissions = [{'FromPort':-1, 'IpProtocol':'-1', 'IpRanges':[{'CidrIp':my_IPv4, 'Description':'wtf'}], 'ToPort':-1}]
  #secGroup.authorize_egress(IpPermissions=ipPermissions)
  vpc_pt = ec2_client.create_vpc_endpoint(VpcEndpointType='Interface', VpcId=vpc.id, ServiceName=service_name,SubnetIds=[subnet.id], SecurityGroupIds=[sec_group.id], PrivateDnsEnabled=False)

def createDataSyncEc2(ec2_client,ec2_resource,ami_id, instance_type):
  vpc_filt = [{'Name': 'tag:Name', 'Values': ['ds_vpc']}]
  vpc_desc = ec2_client.describe_vpcs(Filters=vpc_filt)
  vpc_id = vpc_desc['Vpcs'][0]['VpcId']
  sub_filt = [{'Name': 'vpc-id', 'Values': [vpc_id]}]
  sub_desc = ec2_client.describe_subnets(Filters=sub_filt)
  subnet_id = sub_desc['Subnets'][0]['SubnetId']
  sec_filt = [{'Name': 'vpc-id', 'Values': [vpc_id]}, {'Name': 'group-name', 'Values': ['awsRepEx']}]
  secDesc = ec2_client.describe_security_groups(Filters=sec_filt)
  sec_grp_id = secDesc['SecurityGroups'][0]['GroupId']

  net_int= [
    {
      'AssociatePublicIpAddress': True,
      'DeleteOnTermination': True,
      'DeviceIndex':0,
      'Groups':[sec_grp_id],
      'SubnetId':subnet_id,

    }
  ]

  tag_specifications = [
                        {
                          'ResourceType': 'instance',
                           'Tags': [{
                              'Key': 'Name',
                              'Value': ec2_key_name
                            }],
                        }
                      ]

  instances=ec2_resource.create_instances(MinCount=1, MaxCount=1, ImageId=ami_id, InstanceType=instance_type, NetworkInterfaces=net_int, TagSpecifications=tag_specifications)
  return instances

def createAgent(ec2_client, ds_client,region):
  vpc_filt = [{'Name': 'tag:Name', 'Values': ['ds_vpc']}]
  vpc_desc = ec2_client.describe_vpcs(Filters=vpc_filt)
  vpc_id = vpc_desc['Vpcs'][0]['VpcId']
  sub_filt = [{'Name': 'vpc-id', 'Values': [vpc_id]}]
  sub_desc = ec2_client.describe_subnets(Filters=sub_filt)
  subnet_id = sub_desc['Subnets'][0]['SubnetId']
  subnet_arn= sub_desc['Subnets'][0]['SubnetArn']
  sec_filt = [{'Name': 'vpc-id', 'Values': [vpc_id]}, {'Name': 'group-name', 'Values': ['awsRepEx']}]
  sec_desc = ec2_client.describe_security_groups(Filters=sec_filt)
  sec_grp_id = sec_desc['SecurityGroups'][0]['GroupId']

  end_pt_filt = [{'Name': 'vpc-id', 'Values': [vpc_id]} ]
  end_pt_desc = ec2_client.describe_vpc_endpoints(Filters=end_pt_filt)
  end_pt_id= end_pt_desc['VpcEndpoints'][0]['VpcEndpointId']
  net_ints=end_pt_desc['VpcEndpoints'][0]['NetworkInterfaceIds']
  net_desc=ec2_client.describe_network_interfaces(NetworkInterfaceIds=net_ints)
  net_prv_IP=net_desc['NetworkInterfaces'][0]['PrivateIpAddress']


  #ec2Filt = [{'Name': 'vpc-id', 'Values': [vpcId]},{'Name':'instance-state-name','Values':['running']}, {'Name':'tag:Name', 'Values':['datasync-google2']}]
  ec2_filt = [{'Name': 'vpc-id', 'Values': [vpc_id]}, {'Name':'instance-state-name','Values':['pending','running']},{'Name':'tag:Name', 'Values':[ec2_key_name]}]

  state='pending'
  while state !='running':
    ec2_desc = ec2_client.describe_instances(Filters=ec2_filt)
    instance_dic = ec2_desc['Reservations'][0]['Instances'][0]
    state=instance_dic['State']['Name']
    time.sleep(1)

  ec2_id = ec2_desc['Reservations'][0]['Instances'][0]['InstanceId']
  ec2_ext_IP = ec2_desc['Reservations'][0]['Instances'][0]['PublicIpAddress']

  actUrl='http://'+ec2_ext_IP+':80/?gatewayType=SYNC&activationRegion='+region+'&privateLinkEndpoint='+net_prv_IP+'&endpointType=PRIVATE_LINK&no_redirect'
  #actUrl = 'http://' + ec2ExtIp + ':80/?gatewayType=SYNC&activationRegion=us-west-1&no_redirect'
  status_code=0
  while status_code != 200:
    try:
      actReq=requests.get(actUrl)
      status_code=actReq.status_code
    except:
      time.sleep(10)
      pass

  actKey=actReq.text
  arnRoot=subnet_arn.rsplit(':',1)[0]
  sub_grp_arn=arnRoot+':security-group/'+sec_grp_id
  tags=[{'Key':'tag:Name', 'Value':'googleAgent' } ]
  agent_desc=ds_client.create_agent(AgentName=ds_name,ActivationKey=actKey,VpcEndpointId=end_pt_id, SubnetArns=[subnet_arn], SecurityGroupArns=[sub_grp_arn])
  return agent_desc['AgentArn']

def createLocalLocation(ds_client,role_arn, bucket, sub_dir, local_location_nm):
  tags=[{'Key':'Name', 'Value':local_location_nm}]
  bucket_arn='arn:aws:s3:::'+bucket
  local_loc=ds_client.create_location_s3(Subdirectory=sub_dir, S3Config= {'BucketAccessRoleArn': role_arn}, S3BucketArn=bucket_arn, Tags=tags)
  return local_loc['LocationArn']

def createGoogleLocation(ds_client, bucket, sub_dir, access_key, secret_key, google_location_name, agent_arn):
  tags = [{'Key': 'Name', 'Value': google_location_name}]
  if access_key is None:
    google_loc = ds_client.create_location_object_storage(ServerHostname='storage.googleapis.com', ServerPort=80,
                                                          ServerProtocol='HTTP',
                                                          Subdirectory=sub_dir, BucketName=bucket, AgentArns=[agent_arn])

  else:
    google_loc=ds_client.create_location_object_storage(ServerHostname='storage.googleapis.com', ServerPort=80, ServerProtocol='HTTP',
                                          Subdirectory=sub_dir, BucketName=bucket, AccessKey=access_key, SecretKey=secret_key, AgentArns=[agent_arn])
  return google_loc['LocationArn']

def createCloudWatchLogGroup(lg_client, log_name):
  lg_client.create_log_group(logGroupName=log_name)
  get_grps = lg_client.describe_log_groups(logGroupNamePattern=log_name)
  cgrp_arn=get_grps['logGroups'][0]['arn']
  return cgrp_arn

def createDataSyncTask(ds_client, local_loc_arn, google_loc_arn, cgrp_arn, trans_mode, verify_mode, nm):
  options = {'VerifyMode': verify_mode, 'OverwriteMode': 'ALWAYS', 'TransferMode':trans_mode, 'PreserveDeletedFiles':'PRESERVE', 'ObjectTags':'NONE'}
  task=ds_client.create_task(SourceLocationArn=google_loc_arn, DestinationLocationArn=local_loc_arn, CloudWatchLogGroupArn=cgrp_arn,
                                Options=options, Name=nm)
  return task['TaskArn']

def runTask(ds_client, task_arn):
  ds_client.start_task_execution(TaskArn=task_arn)

def deleteVpc(ec2_client,vpc_name):
  vpc_filt = [{'Name': 'tag:Name', 'Values': [vpc_name]}]
  vpc_desc = ec2_client.describe_vpcs(Filters=vpc_filt)
  for vpc in vpc_desc['Vpcs']:
    vpc_id = vpc['VpcId']
    resource_filt = [{'Name': 'vpc-id', 'Values': [vpc_id]}]
    end_pt_desc = ec2_client.describe_vpc_endpoints(Filters=resource_filt)
    end_pt_list =[end_pr['VpcEndpointId'] for end_pr in end_pt_desc['VpcEndpoints']]
    if (len(end_pt_list)>0):
      ec2_client.delete_vpc_endpoints(VpcEndpointIds=end_pt_list)
    while (len(end_pt_list)>0):
      end_pt_desc = ec2_client.describe_vpc_endpoints(Filters=resource_filt)
      end_pt_list = [end_pr['VpcEndpointId'] for end_pr in end_pt_desc['VpcEndpoints']]
      time.sleep(1)

    ec2_desc = ec2_client.describe_instances(Filters=resource_filt)
    try:
      ec2_list = [inst['InstanceId'] for inst in reduce(concat,[insts['Instances'] for insts in [res for res in ec2_desc['Reservations']]])]
    except:
      ec2_list=[]

    if len(ec2_list) > 0:
      ec2_client.terminate_instances(InstanceIds=ec2_list)

    while len(ec2_list)>0:
      ec2_desc = ec2_client.describe_instances(Filters=resource_filt)
      try:
        ec2_list = [inst['InstanceId'] for inst in
                   reduce(concat, [insts['Instances'] for insts in [res for res in ec2_desc['Reservations']]])]
      except:
        ec2_list = []
      time.sleep(1)

    sec_grp_desc=ec2_client.describe_security_groups(Filters=resource_filt)
    sec_grp_list=[sec_grp['GroupId'] for sec_grp in sec_grp_desc['SecurityGroups'] if not (sec_grp['GroupName'] =='default')]
    for sec_grp_id in sec_grp_list:
      ec2_client.delete_security_group(GroupId=sec_grp_id)

    subnet_desc = ec2_client.describe_subnets(Filters=resource_filt)
    subnet_list = [subnet['SubnetId'] for subnet in subnet_desc['Subnets']]
    if (len(subnet_list) > 0):
      for subnet_id in subnet_list:
        ec2_client.delete_subnet(SubnetId=subnet_id)
    route_desc = ec2_client.describe_route_tables(Filters=resource_filt)
    route_list=[rt['RouteTableId'] for rt in route_desc['RouteTables'] if ( (len(rt['Associations'])==0) or ('Main' not in rt['Associations'][0]) or not rt['Associations'][0]['Main'])]
    for route in route_list:
      ec2_client.delete_route_table(RouteTableId=route)

    igw_filt = [{'Name': 'attachment.vpc-id', 'Values': [vpc_id]}]
    igw_desc = ec2_client.describe_internet_gateways(Filters=igw_filt)
    igw_list = [igw['InternetGatewayId'] for igw in igw_desc['InternetGateways']]
    if (len(igw_list) > 0):
      for ig_id in igw_list:
        ec2_client.detach_internet_gateway(InternetGatewayId=ig_id, VpcId=vpc_id)
        ec2_client.delete_internet_gateway(InternetGatewayId=ig_id)

    ec2_client.delete_vpc(VpcId=vpc_id)

def findStorageLocationArn(ds_client,bucket,sub_dir,type):
  sel_arn=''
  tim=None
  uri='s3://'+bucket+'/'+sub_dir
  list_filters=[]
  if (type=='Local'):
    list_filters = [{'Name': 'LocationType', 'Values': ['S3'], 'Operator': 'Equals'},{'Name':'LocationUri', 'Values':[uri], 'Operator':'Equals'}]
  elif (type=='Object_storage'):
    list_filters = [{'Name': 'LocationType', 'Values': ['OBJECT_STORAGE'], 'Operator': 'Equals'}]

  loc_locs = ds_client.list_locations(Filters=list_filters)
  for loc in loc_locs['Locations']:
    loc_arn = loc['LocationArn']
    loc_uri = loc['LocationUri']
    locD=[]
    meets_filter=False
    if (type=='Local'):
      locD = ds_client.describe_location_s3(LocationArn=loc_arn)
      meetsFilter=True
    elif (type=='Object_storage'):
      locD = ds_client.describe_location_object_storage(LocationArn=loc_arn)
      agent_arn=locD['AgentArns'][0]
      agent_desc=ds_client.describe_agent(AgentArn=agent_arn)
      if agent_desc['Status']=='ONLINE':
        meets_filter = True
    tim_new=locD['CreationTime']
    if meetsFilter and ((tim_new is None) or (tim<tim_new)):
      tim=tim_new
      sel_arn=loc_arn
  return sel_arn

def clearDataSync(ds_client):
  task_list= ds_client.list_tasks()
  for task in task_list['Tasks']:
    arn=task['TaskArn']
    ds_client.delete_task(TaskArn=arn)

  location_list= dsClient.list_locations()
  for loc in location_list['Locations']:
    arn= loc['LocationArn']
    ds_client.delete_location(LocationArn=arn)

  agent_list = dsClient.list_agents()
  for agent in agent_list['Agents']:
    arn= agent['AgentArn']
    ds_client.delete_agent(AgentArn=arn)


if __name__=="__main__":

  service_name='com.amazonaws.'+region+'.datasync'
  ec2_resource = boto3.resource('ec2', region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token)
  ec2_client =  boto3.client('ec2', region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token)
  ds_client = boto3.client('datasync', region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token)
  lg_client = boto3.client('logs', region_name=region, aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key,aws_session_token=aws_session_token)

  #createDataSyncEc2(ec2_client, ec2_resource, ami_id, instance_type)
  #agent_arn = createAgent(ec2_client, ds_client, region)
  agent_arn = 'arn:aws:datasync:us-east-1:266665233841:agent/agent-0aa01c686d7526c8e'

  for task in transfer_tasks:

    nm=task['taskname']
    local_bucket=task['localBucket']
    local_sub_dir=task['localSubDir']
    local_location_name=task['localLocationNm']
    local_loc_arn=createLocalLocation(ds_client, role_arn,local_bucket, local_sub_dir, local_location_name)

    google_bucket = task['googleBucket']
    google_sub_dir = task['googleSubDir']
    google_location_name = task['googleLocationNm']
    public_bucket =task['googlePublicBucket']

    if public_bucket:
      google_loc_arn = createGoogleLocation(ds_client, google_bucket, google_sub_dir, None, None, google_location_name, agent_arn)
    else:
      google_loc_arn=createGoogleLocation(ds_client, google_bucket, google_sub_dir, hmac_access_key_id, hmac_secret_access_key, google_location_name, agent_arn)

    cgrp_arn=''
    log_prefix=task['logGrpPrefix']
    log_nm = task['logGrpName']
    get_grps = lg_client.describe_log_groups(logGroupNamePrefix=log_prefix)
    if (len(get_grps['logGroups']) == 0):
      cgrp_arn = createCloudWatchLogGroup(lg_client, log_name)
    else:
      cgrp_arn = get_grps['logGroups'][0]['arn']
    verify_mode='ONLY_FILES_TRANSFERRED'
    trans_mode='CHANGED'
    taskArn = createDataSyncTask(ds_client, local_loc_arn, google_loc_arn, cgrp_arn, trans_mode, verify_mode, nm)
