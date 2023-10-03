from google.oauth2 import service_account
from google.cloud import storage
from google.cloud import bigquery
import json
import pyarrow as pa
import gcsfs
import pandas as pd
import io




bucket='aws_request_logs'
strt='not_processed'
end='processed'
project_id = 'idc-dev-etl'
if __name__=='__main__':
  with open(cred_file) as source:
    info = json.load(source)
    credentials = service_account.Credentials.from_service_account_info(info)
    fs = gcsfs.GCSFileSystem(project=project_id, token=CRED_FILE)
    file_list = fs.ls(bucket + '/' + strt)
    for nm in file_list:
      source_uri = "gs://{}".format(nm)
      df = pd.read_csv(source_uri,token=cred_file)
      ll=1

