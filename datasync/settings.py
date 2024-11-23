import os
from os.path import join, dirname, exists
from dotenv import load_dotenv


SECURE_LOCAL_PATH = os.environ.get('SECURE_LOCAL_PATH', '')
if not exists(join(dirname(__file__), SECURE_LOCAL_PATH)):
    print("[ERROR] Couldn't open secure file expected at {}!".format(
        join(dirname(__file__), SECURE_LOCAL_PATH))
    )
    print("[ERROR] Exiting settings.py load - check your Pycharm settings and secure_path.env file.")
    exit(1)

load_dotenv(dotenv_path=join(dirname(__file__), SECURE_LOCAL_PATH))

S3_CUTOFF_SIZE=5000000000

GOOGLE_HMAC_KEY_FILE = os.environ.get('GOOGLE_HMAC_KEY_FILE','')
AWS_CREDENTIALS_FILE = os.environ.get('AWS_CREDENTIALS_FILE','')

load_dotenv(dotenv_path=GOOGLE_HMAC_KEY_FILE)
load_dotenv(dotenv_path=AWS_CREDENTIALS_FILE)

DEV_AWS_ACCESS_KEY_ID = os.environ.get('DEV_AWS_ACCESS_KEY_ID','')
DEV_AWS_SECRET_ACCESS_KEY = os.environ.get('DEV_AWS_SECRET_ACCESS_KEY','')
DEV_AWS_ACCESS_TOKEN = os.environ.get('DEV_AWS_SESSION_TOKEN','')

PROD_AWS_ACCESS_KEY_ID = os.environ.get('PROD_AWS_ACCESS_KEY_ID','')
PROD_AWS_SECRET_ACCESS_KEY = os.environ.get('PROD_AWS_SECRET_ACCESS_KEY','')
PROD_AWS_ACCESS_TOKEN = os.environ.get('PROD_AWS_SESSION_TOKEN','')


HMAC_ACCESS_KEY_ID = os.environ.get('HMAC_ACCESS_KEY_ID','')
HMAC_SECRET_ACCESS_KEY = os.environ.get('HMAC_SECRET_ACCESS_KEY','')

DEFAULT_PROJECT ='idc-dev-etl'

CURRENT_VERSION = '19'
DEFAULT_DATASET = 'idc_v' + CURRENT_VERSION + '_dev'

INSTANCE_TYPE = 'm5.4xlarge'
REGION = 'us-east-1'
DATE = '09_12_24'
EC2_KEY_NAME = 'dataSyncImgFiles_'+DATE
DS_NAME = 'dataSyncImgFiles_'+DATE

AMI_ID = 'ami-044565343082e2c1b'

CIDR_BLOCK = '172.16.0.0/16'
DS_ROLE_ARN = 'arn:aws:iam::266665233841:role/George_DataSync_Role_Block1'
VPC_NAME = 'ds_vpc'

PROD_ACCOUNT_ID = '051845558647'
STAGING_BUCKET = 'idc-open-data-staging'
S3_ROLE_ARN = 'arn:aws:iam::051845558647:role/s3_batch_role'
DESTINATION_BUCKET = 'idc-open-data'
#your local ip here only needed to create or update VPC
MY_IPV4 = ''

TRANSFER_TASKS = [{'taskname':'staging-dataset-'+DATE,'localBucket':'idc-open-data-staging','googleBucket':'public-datasets-idc-staging','localSubDir':'','googleSubDir':'',
                   'localLocationNm':'aws-open-staging', 'googleLocationNm':'google-open-staging',
                 'googlePublicBucket':False,'logGrpPrefix':'pub', 'logGrpName':'pub-datasync-idc-open'}]


TRANSFER_TASKS = [{'taskname':'metadata-dataset-'+DATE,'localBucket':'idc-open-metadata','googleBucket':'bq_export_idc','localSubDir':'','googleSubDir':'',
                   'localLocationNm':'aws-metadata', 'googleLocationNm':'google-metadata',
                 'googlePublicBucket':False,'logGrpPrefix':'meta', 'logGrpName':'metadata-logs'}]
