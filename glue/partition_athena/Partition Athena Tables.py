import sys
import boto3
import logging
from awsglue.utils import getResolvedOptions

MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
ilogger = logging.getLogger('itester')
ilogger.setLevel(logging.INFO)
ilogger.info("Test log message")

client=boto3.client('athena', region_name='us-east-1')
args = getResolvedOptions(sys.argv, ['athena_db','updated_tables','athena_results'])

ilogger.info(args)


config = {
        'OutputLocation': 's3://' + args['athena_results'] + '/',
        'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}

    }

if __name__ == '__main__':
    context = {'Database': args['athena_db']}
    tbls=args['updated_tables'].split(',')
    for tbl in tbls:
      sql = 'MSCK REPAIR TABLE '+args['athena_db']+'.'+tbl
      client.start_query_execution(QueryString=sql, QueryExecutionContext=context, ResultConfiguration=config)
     
    
  
  
