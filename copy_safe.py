import boto3
import pandas as pd


def copy_safe(s3_client,source_bucket, source_obj, destination_bucket,destination_obj,owrite):
  return_dic['op'] = [source_bucket, source_obj,destination_bucket,destination_obj,str(owrite)]
  err=[]
  source_ok = False
  dest_permission = False
  dest_empty = False
  destination_ok = False


  try:
    source_head = s3_client.head_object(Bucket=source_bucket, Key=source_obj)
    source_ok = True
    return_dic['source_etag']=source_head['ETag']
  except Exception as e:
    source_ok=False
    err.append(str(e))
  try:
    s3_client.head_object(Bucket=destination_bucket, Key=destination_obj)
    dest_empty = False
    destination_ok = True
    err.append("destination object already exists")
  except Exception as e:
    if (e.response['Error']['Message'] == 'Forbidden') or (e.response['Error']['Code'] == '403'):
      err.append(str(e))
      err.append(str(e))
    elif (e.response['Error']['Message'] == 'Not Found') or (e.response['Error']['Code'] == '404'):
      dest_empty = True
      dest_permission = True
      destination_ok = True
    else:
      destination_ok = False

  if source_ok and destination_ok and (dest_empty or owrite) and dest_permission:
    if not dest_empty:
      return_dic['warn']="destination is not empty"
    try:
      s3_client.copy_object(Bucket=destination_bucket, Key=destination_obj, CopySource={'Bucket':source_bucket, 'Key':source_obj})
      destination_head = s3_client.head_object(Bucket=destination_bucket, Key=destination_obj)
      return_dic['destination_etag'] = destination_head['ETag']
      copy_ok=False
      if (return_dic['destination_etag'] == return_dic['source_etag']):
        copy_ok = True
      else:
        err.append("etags different after copy")
        copy_ok = False
      if copy_ok:
        s3_client.delete_object(Bucket=source_bucket, Key=source_obj)
      else:
        s3_client.delete_object(Bucket=destination_bucket, Key=destination_obj)
    except Exception as e:
      err.append(str(e))

  if (len(err)>0):
    errStr="||".join(err)
    return_dic['err']=errStr

  return return_dic

if __name__ == '__main__':
  aws_key_file = "../secure_files/aws_key.csv"
  key_data = pd.read_csv(aws_key_file).iloc[0]
  access_key = key_data['Access key ID']
  secret_key = key_data['Secret access key']
  region = 'us-east-1'

  s3_client = boto3.client('s3', region_name=region, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
  ret1=copy_safe(s3_client, 'gw-new-test','testmv1/recipe1.jpg','gw-new-test','testmv2/recipe1.jpg',False)
  print(ret1)
  ret2=copy_safe(s3_client, 'gw-new-test', 'testmv1/recipe1.jpg', 'gw-new-test','testmv2/recipe1.jpg',False)
  print(ret2)
  ret3 = copy_safe(s3_client, 'gw-new-test', 'testmv1/recipe1.jpg', 'gw-new-test', 'testmv2/recipe_1.jpg', False)
  print(ret2)
  ret3 = copy_safe(s3_client, 'gw-new-test', 'testmv1/recipe33.jpg', 'gw-new-test', 'testmv2/recipe_1.jpg', False)
  print(ret3)
  ret3 = copy_safe(s3_client, 'idc-open-data-metadata', 'gw_temp/testmv1/recipe33.jpg', 'gw-new-test', 'testmv2/recipe_33.jpg', False)
  print(ret3)
  ret3 = copy_safe(s3_client, 'idc-open-data-metadata', 'gw_temp/testmv1/recipe33.jpg', 'idc-open-data-metadata','gw_temp/testmv2/recipe_33.jpg', False)
  print(ret3)
  ff=1