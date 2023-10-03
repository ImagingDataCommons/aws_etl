from google.cloud import storage
from google.oauth2 import service_account
from google.cloud import storage
from google.cloud import bigquery
import json
import pyarrow as pa
import gcsfs
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import io

CRED_FILE='/Users/george/idc/secure_files/idc-dev-etl-8a571d5ad9a5.json'
IDC_VERSION='v13'
project_id = 'idc-dev-etl'

part =[-1,500,1000,1500,2000,2500,3010]

def reform_parquet(reform_cols, bucket_name, location):
  print('fixing table '+location)
  fs=gcsfs.GCSFileSystem(project=project_id, token=CRED_FILE)
  file_list=fs.ls(bucket_name+'/'+location)
  for nm in file_list:
    destination_uri = "gs://{}".format(nm)
    nschema=None
    with fs.open(destination_uri) as f:
      pq_array = pa.parquet.read_table(f, memory_map=True)
      #nschema = pq_array.schema
      for col in reform_cols:
        #print(col[0], col[1])
        nschema = pq_array.schema
        if len(col)==2:
          nschema=nschema.set(col[0], pa.field(col[1],pa.string()))
        else:
          nschema = nschema.set(col[0], pa.field(col[1], pa.list_(pa.string(),-1)))
        new_array = pq_array.cast(target_schema=nschema)
        #try:
        #  new_array = pq_array.cast(target_schema=nschema)
        #except:
        #  pass
    with fs.open(destination_uri, 'wb') as f:
      pa.parquet.write_table(new_array,f)



def export_table_wrapper(client,source_id,dataset_id, table_id,bucket_name, partitions):
  full_id=source_id+"."+dataset_id+"."+table_id
  table=client.get_table(full_id)
  reform_cols = []
  for idx, field in enumerate(table.schema):
    if field.field_type=='TIME':
      args=[idx, field.name]
      if field.mode=='REPEATED':
        args.append('list')
      reform_cols.append(args)


  destination_uri=""
  if (table.num_bytes<pow(10,9)) and (partitions<=1):
    try:
      destination_uri="gs://{}/{}".format(bucket_name, dataset_id + "/" + table_id + "/" + table_id + ".parquet")
      export_table(client,source_id,dataset_id, table_id,destination_uri)
    except:
      destination_uri="gs://{}/{}".format(bucket_name, dataset_id + "/" + table_id + "/" + table_id + "_*" + ".parquet")
      export_table(client, source_id, dataset_id, table_id, destination_uri)

  elif (partitions >1):
    dest_uris=[]
    for part in range(partitions):
      dest_uris.append("gs://{}/{}".format(bucket_name, dataset_id + "/" + table_id + "/" + table_id + "_"+str(part)+"_*"+".parquet"))
    export_table(client, source_id, dataset_id, table_id, dest_uris)
  else:
    destination_uri = "gs://{}/{}".format(bucket_name, dataset_id + "/" + table_id + "/" + table_id + "_*"+".parquet")
    export_table(client, source_id, dataset_id, table_id, destination_uri)
  if len(reform_cols)>0:
    jj=1
    #reform_parquet(reform_cols,bucket_name,dataset_id + "/" + table_id)


def export_table(client,source_id,dataset_id, table_id,destination_uri):
      dataset_ref = bigquery.DatasetReference(source_id, dataset_id)
      table_ref = dataset_ref.table(table_id)
      job_config = bigquery.job.ExtractJobConfig()
      job_config.destination_format = "PARQUET"

      extract_job = client.extract_table(
        table_ref,
        destination_uri,
        # Location must match that of the source table.
        location="US",
        job_config=job_config
      )  # API request
      extract_job.result()  # Waits for job to complete.

      print(
        "Exported {}:{}.{} to {}".format(source_id, dataset_id, table_id, destination_uri)
      )

def export_partition_job(client, job_config,dataset_ref,dataset_id,table_id, part_no, bucket_name):


  destination_uri = "gs://{}/{}".format(bucket_name, dataset_id + "/" + table_id  +"/" + table_id + "_" + str(part_no) +".parquet")
  if (len(part))>0:
    temp=part.copy()
    temp.append(part_no)
    temp.sort()
    pos = temp.index(part_no)-1
    destination_uri = "gs://{}/{}".format(bucket_name, dataset_id + "/" + table_id +"/"+str(pos) +"/" + table_id + "_" + str(part_no) + ".parquet")

  part_id=table_id+"$"+str(part_no)
  part_ref = dataset_ref.table(part_id)
  #print("starting")
  extract_job = client.extract_table(
    part_ref,
    destination_uri,
    # Location must match that of the source table.
    location="US",
    job_config=job_config
  )
  return (extract_job)
  # API request
  #extract_job.result()
  # Waits for job to complete.
  #print("Exported {}:{}.{} to {}".format(source_id, dataset_id, part_id, destination_uri))
  #return ("Exported {}:{}.{} to {}".format(source_id, dataset_id, part_id, destination_uri))
  #print(
  #  "Exported {}:{}.{} to {}".format(source_id, dataset_id, part_id, destination_uri)
  #)



def export_partitions(client,source_id,dataset_id, table_id,bucket_name):
  full_id = source_id + "." + dataset_id + "." + table_id
  table = client.get_table(full_id)
  strt = table.range_partitioning.range_.start
  end = table.range_partitioning.range_.end
  dataset_ref = bigquery.DatasetReference(source_id, dataset_id)
  job_config = bigquery.job.ExtractJobConfig()
  job_config.destination_format = "PARQUET"
  #job_config.compression = bigquery.Compression.GZIP
  jobs=[]
  threads=[]
  results=[]
  executor = ThreadPoolExecutor(6)
  for part_no in range(strt, end):
    jobs.append(export_partition_job(client,job_config,dataset_ref,dataset_id,table_id,part_no,bucket_name))
    print(str(part_no))
  for job in jobs:
    threads.append(executor.submit(job.result))

  for future in as_completed(threads):
    print("done")

  ss=1
  #with concurrent.futures.ProcessPoolExecutor() as executor:
  #  return executor.map(export_partition,args)




if __name__=='__main__':
  source_id='bigquery-public-data'
  alt_source_id='idc-dev-etl'
  dataset_id = 'idc_'+IDC_VERSION
  dataset_clinical_id = 'idc_'+IDC_VERSION+'_clinical'
  alt_id='idc_v14_dev'
  table_id = 'dicom_all'
  bucket_name='idc-open-metadata'
  #export_table_wrapper(client, source_id, dataset_id, table_id, bucket_name)

  #destination_uri="gs://{}/{}".format(bucket_name, dataset_id+"/"+table_id+"/"+table_id+"*")
  #credentials, project_id = google.auth.default()

  with open(CRED_FILE) as source:
    info = json.load(source)
    storage_credentials = service_account.Credentials.from_service_account_info(info)
    client = bigquery.Client(credentials=storage_credentials, project=project_id)
    gcs_client = storage.Client(credentials=storage_credentials, project=project_id)
    #export_table_wrapper(client, alt_source_id, alt_id, "uuid_url_map_from_view_cr", bucket_name,1)
    #export_partitions(client, alt_source_id, alt_id, "uuid_url_map_from_view_two", bucket_name)
    #export_partitions(client, alt_source_id, alt_id, "uuid_url_map_from_view_cr", bucket_name)
    export_partitions(client, alt_source_id, alt_id, "uuid_url_map_from_view_pub", bucket_name)
    #export_table_wrapper(client, alt_source_id, alt_id, "uuid_url_map_from_view_two", bucket_name,6)
    #export_table_wrapper(client, alt_source_id, alt_id, "uuid_url_map_from_view_pub", bucket_name,1)
    #export_partitions(client, alt_source_id, alt_id, "dicom_all_refactor_partition2", bucket_name)


    '''for dsource in [(alt_source_id, alt_id)]:
      source_id=dsource[0]
      ds_id=dsource[1]
      tables = client.list_tables(source_id+"."+ds_id)
      for table in tables:
        if table.table_type=='TABLE':
          query="select count(*) from "+source_id+"."+ds_id+"."+table.table_id
          job=client.query(query)
          for row in job.result():
            print(table.table_id+str(row))
          export_table_wrapper(client,source_id,ds_id, table.table_id,bucket_name)'''









