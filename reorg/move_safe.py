import boto3
import pandas as pd
import datetime



def move_safe(s3_client,source_bucket, source_obj, destination_bucket,destination_obj,owrite,exp_hash, trust_hash):
  return_dic={
    'op': [source_bucket, source_obj,destination_bucket,destination_obj,str(owrite)],
    'err':'',
    'warn':'',
    'destination_etag':'',
    'source_etag':'',
    'copy_ok':'False',
    'datetime':''
  }
  err=[]
  warn=[]
  source_ok = False
  dest_permission = False
  dest_empty = False
  destination_ok = False
  copy_ok=''
  hash_mismatch=False
  preserve_hash_issue=False
  src_dest_match=False
  copy_needed=True
  cur_dest_hash=''

  try:
    source_head = s3_client.head_object(Bucket=source_bucket, Key=source_obj)
    source_ok = True
    return_dic['source_etag']=source_head['ETag'].strip('"')
    if (exp_hash is not None) and not (exp_hash == return_dic['source_etag']):
      hash_mismatch=True
      warn.append("hash mismatch")
  except Exception as e:
    source_ok=False
    err.append('Error getting source object:'+str(e))

  if ((source_bucket==destination_bucket) and (source_obj==destination_obj)):
    destination_ok = False
    cur_dest_hash=return_dic['source_etag']
    err.append("destination and source objects are the same! Not moving")
  else:
    try:
      dest_head=s3_client.head_object(Bucket=destination_bucket, Key=destination_obj)
      dest_empty = False
      destination_ok = True
      dest_permission = True
      cur_dest_hash=dest_head['ETag'].strip('"')
      if hash_mismatch and (cur_dest_hash == exp_hash):
        preserve_hash_issue = True
        err.append("destination object has expected hash, source does not. Not moving")
      elif trust_hash and (cur_dest_hash==return_dic['source_etag']):
        preserve_hash_issue=True
        err.append("destination object has same hash as source. Trusting hash and not moving")
      else:
        if owrite:
          warn.append("destination object already exists, but allowing overwrite")
        else:
          err.append("destination object already exists, not allowing overwrite")

    except Exception as e:
      if (e.response['Error']['Message'] == 'Forbidden') or (e.response['Error']['Code'] == '403'):
        err.append(str(e))
        dest_permission= False
      elif (e.response['Error']['Message'] == 'Not Found') or (e.response['Error']['Code'] == '404'):
        dest_empty = True
        dest_permission = True
        destination_ok = True
      else:
        destination_ok = False

  if source_ok and destination_ok and (dest_empty or owrite) and dest_permission and not preserve_hash_issue:
    try:
      s3_client.copy_object(Bucket=destination_bucket, Key=destination_obj, CopySource={'Bucket':source_bucket, 'Key':source_obj})
      destination_head = s3_client.head_object(Bucket=destination_bucket, Key=destination_obj)
      return_dic['destination_etag'] = destination_head['ETag'].strip('"')
      copy_ok=False
      if (return_dic['destination_etag'] == return_dic['source_etag']):
        copy_ok = True
      else:
        err.append("etag different after moving. Retaining original and new")
        copy_ok = False
      if copy_ok:
        s3_client.delete_object(Bucket=source_bucket, Key=source_obj)
      else:
        #s3_client.delete_object(Bucket=destination_bucket, Key=destination_obj)
        pass
    except Exception as e:
      err.append(str(e))

  elif not dest_empty:
    warn.append("did not move but destination object currently exists")
    return_dic['destination_etag'] = cur_dest_hash
  if (len(err)>0):
    errStr="||".join(err)
    return_dic['err']=errStr
  if (len(warn)>0):
    warnStr="||".join(warn)
    return_dic['warn']=warnStr
  return_dic['copy_ok']=str(copy_ok)
  return_dic['datetime']=str(datetime.datetime.now())
  return return_dic

if __name__ == '__main__':
  aws_key_file = "../secure_files/aws_key.csv"
  key_data = pd.read_csv(aws_key_file).iloc[0]
  access_key = key_data['Access key ID']
  secret_key = key_data['Secret access key']
  region = 'us-east-1'

  s3_client = boto3.client('s3', region_name=region, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
  ret1=move_safe(s3_client, 'gw-new-test','testmv1/recipe1.jpg','gw-new-test','testmv1/recipe2.jpg',False,None, False)
  print(ret1)
  ret2=move_safe(s3_client, 'gw-new-test', 'testmv1/recipe1.jpg', 'gw-new-test','testmv1/recipe2.jpg',True,None, False)
  print(ret2)
