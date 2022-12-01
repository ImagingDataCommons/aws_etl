import boto3
import pandas as pd
import requests
from google.cloud import bigquery
KEY_FILE="../secure_files/aws_key.csv"
GOOGLE_KEY_FILE="../secure_files/google_key.csv"

def createVPC(ec2Resource,ec2Client,cidrBlock,myIpv4,serviceName):
  vpc = ec2Resource.create_vpc(CidrBlock=cidrBlock)

  vpc.create_tags(Tags=[{"Key": "Name", "Value": "new_vpc1"}])
  igateway = ec2Resource.create_internet_gateway()
  igateway.create_tags(Tags=[{"Key": "Name", "Value": "new_gtwy"}])
  vpc.attach_internet_gateway(InternetGatewayId=igateway.id)
  routetable = vpc.create_route_table()
  routetable.create_tags(Tags=[{"Key": "Name", "Value": "new_rt"}])
  route=routetable.create_route(DestinationCidrBlock='0.0.0.0/0',GatewayId=igateway.id)

  subnet = ec2Resource.create_subnet(CidrBlock=cidrBlock, VpcId=vpc.id)
  subnet.create_tags(Tags=[{"Key": "Name", "Value": "new_sub"}])
  routetable.associate_with_subnet(SubnetId=subnet.id)

  secGroup = ec2Resource.create_security_group(GroupName='awsRepEx',Description='configure for AWSRep', VpcId=vpc.id)
  secGroup.authorize_ingress(CidrIp=myIpv4, IpProtocol='-1')

  # outbound rule all traffic created automatically
  #ipPermissions = [{'FromPort':-1, 'IpProtocol':'-1', 'IpRanges':[{'CidrIp':myIpv4, 'Description':'wtf'}], 'ToPort':-1}]
  #secGroup.authorize_egress(IpPermissions=ipPermissions)
  vpcPt = ec2Client.create_vpc_endpoint(VpcEndpointType='Interface', VpcId=vpc.id, ServiceName=serviceName,SubnetIds=[subnet.id], SecurityGroupIds=[secGroup.id], PrivateDnsEnabled=False)

def createDataSyncEc2(ec2Client,ec2Resource, amiId, instanceType, keyName):
  vpcFilt = [{'Name': 'tag:Name', 'Values': ['new_vpc1']}]
  vpcDesc = ec2Client.describe_vpcs(Filters=vpcFilt)
  vpcId = vpcDesc['Vpcs'][0]['VpcId']
  subFilt = [{'Name': 'vpc-id', 'Values': [vpcId]}]
  subDesc = ec2Client.describe_subnets(Filters=subFilt)
  subnetId = subDesc['Subnets'][0]['SubnetId']
  secFilt = [{'Name': 'vpc-id', 'Values': [vpcId]}, {'Name': 'group-name', 'Values': ['awsRepEx']}]
  secDesc = ec2Client.describe_security_groups(Filters=secFilt)
  secGrpId = secDesc['SecurityGroups'][0]['GroupId']

  netInt= [
    {
      'AssociatePublicIpAddress': True,
      'DeleteOnTermination': True,
      'DeviceIndex':0,
      'Groups':[secGrpId],
      'SubnetId':subnetId,

    }
  ]

  tagSpecifications = [
                        {
                          'ResourceType': 'instance',
                           'Tags': [{
                              'Key': 'Name',
                              'Value': 'dataSyncEc42'
                            }],
                        }
                      ]

  ec2Resource.create_instances(KeyName=keyName, MinCount=1, MaxCount=1, ImageId=amiId, InstanceType=instanceType, NetworkInterfaces=netInt, TagSpecifications=tagSpecifications)

def createAgent(ec2Client, dsClient):
  vpcFilt = [{'Name': 'tag:Name', 'Values': ['new_vpc1']}]
  vpcDesc = ec2Client.describe_vpcs(Filters=vpcFilt)
  vpcId = vpcDesc['Vpcs'][0]['VpcId']
  subFilt = [{'Name': 'vpc-id', 'Values': [vpcId]}]
  subDesc = ec2Client.describe_subnets(Filters=subFilt)
  subnetId = subDesc['Subnets'][0]['SubnetId']
  subnetArn= subDesc['Subnets'][0]['SubnetArn']
  secFilt = [{'Name': 'vpc-id', 'Values': [vpcId]}, {'Name': 'group-name', 'Values': ['awsRepEx']}]
  secDesc = ec2Client.describe_security_groups(Filters=secFilt)
  secGrpId = secDesc['SecurityGroups'][0]['GroupId']

  endPtFilt = [{'Name': 'vpc-id', 'Values': [vpcId]} ]
  endPtDesc = ec2Client.describe_vpc_endpoints(Filters=endPtFilt)
  endPtId= endPtDesc['VpcEndpoints'][0]['VpcEndpointId']
  netInts=endPtDesc['VpcEndpoints'][0]['NetworkInterfaceIds']
  netDesc=ec2Client.describe_network_interfaces(NetworkInterfaceIds=netInts)
  netPrvIp=netDesc['NetworkInterfaces'][0]['PrivateIpAddress']

  #ec2Filt = [{'Name': 'vpc-id', 'Values': [vpcId]},{'Name':'instance-state-name','Values':['running']}, {'Name':'tag:Name', 'Values':['datasync-google2']}]
  ec2Filt = [{'Name': 'vpc-id', 'Values': [vpcId]}, {'Name':'instance-state-name','Values':['running']},{'Name':'tag:Name', 'Values':['dataSyncEc42']}]
  ec2Desc = ec2Client.describe_instances(Filters=ec2Filt)
  ec2Id = ec2Desc['Reservations'][0]['Instances'][0]['InstanceId']
  ec2ExtIp = ec2Desc['Reservations'][0]['Instances'][0]['PublicIpAddress']

  actUrl='http://'+ec2ExtIp+':80/?gatewayType=SYNC&activationRegion=us-west-1&privateLinkEndpoint='+netPrvIp+'&endpointType=PRIVATE_LINK&no_redirect'
  actReq=requests.get(actUrl)
  actKey=actReq.text

  arnRoot=subnetArn.rsplit(':',1)[0]
  subGrpArn=arnRoot+':security-group/'+secGrpId
  tags=[{'Key':'tag:Name', 'Value':'googleAgent' } ]
  agentArn=dsClient.create_agent(ActivationKey=actKey,VpcEndpointId=endPtId, SubnetArns=[subnetArn], SecurityGroupArns=[subGrpArn])
  return agentArn


def createLocalLocation(dsClient,s3arn, bucket, subDir, localLocationNm):
  tags=[{'Key':'Name', 'Value':localLocationNm}]
  dsClient.create_location_s3(Subdirectory=subDir, S3Config= {'BucketAccessRoleArn': s3arn}, S3BucketArn=bucket, Tags=tags)

def createGoogleLocation(dsClient, bucket, subDir, accessKey, secretKey, googleLocationName, agentArn):
  tags = [{'Key': 'Name', 'Value': googleLocationName}]
  dsClient.create_location_object_storage(ServerHostname='storage.googleapis.com', ServerPort=80, ServerProtocol='HTTP',
                                          Subdirectory=subDir, BucketName=bucket, AccessKey=accessKey, SecretKey=secretKey, AgentArns=[agentArn])


if __name__=="__main__":
  myIpv4 = 'YOUR LOCAL IPV4 HERE'
  key_data=pd.read_csv(KEY_FILE).iloc[0]
  ACCESS_KEY = key_data['Access key ID']
  SECRET_KEY = key_data['Secret access key']
  google_key_data=pd.read_csv(GOOGLE_KEY_FILE).iloc[0]
  GOOGLE_ACCESS_KEY = key_data['Access key ID']
  GOOGLE_SECRET_KEY = key_data['Secret access key']

  cidrBlock='172.16.0.0/16'
  serviceName='com.amazonaws.us-west-1.datasync'
  ec2Resource = boto3.resource('ec2', region_name='us-west-1', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
  ec2Client =  boto3.client('ec2', region_name='us-west-1', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
  dsClient = boto3.client('datasync', region_name='us-west-1', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

  amiId='ami-04c9d148c9314fc2d'
  s3arn='arn:aws:iam::266665233841:role/George_DataSync_Role_Block1'
  bucket ='arn:aws:s3:::idc-test-bucket2'
  subdir=''
  instanceType='t2.xlarge'
  keyName='datasync-test'
  localLocationNm='localStore'

  createVPC(ec2Resource,ec2Client,cidrBlock, myIpv4, serviceName)
  createDataSyncEc2(ec2Client, ec2Resource, amiId, instanceType, keyName)
  agentArn=createAgent(ec2Client,dsClient)

  localLocationNm = 'localStore'
  googleLocationName='googleLoc'
  createLocalLocation(dsClient, s3arn,bucket, subdir, localLocationNm)
  createGoogleLocation(dsClient, bucket, subdir, GOOGLE_ACCESS_KEY, GOOGLE_SECRET_KEY, googleLocationName, agentArn)


