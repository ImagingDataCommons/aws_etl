scripts for copying data from gcs to aws s3 (or vice versa).


Preliminaries
1 Set enironmental variables to PYTHONUNBUFFERED=1;SECURE_LOCAL_PATH=../../secure_files/ds.env
2.Create the following files to be copied to ../../secure_files

ds.env

GOOGLE_HMAC_KEY_FILE="../../secure_files/transfer_to_aws_hmac.env"
AWS_CREDENTIALS_FILE="../../secure_files/aws_credentials.env"
PROD_ACCOUNT_ID= # put AWS production account id here #

transfer_to_aws_hmac.env

HMAC_ACCESS_KEY_ID= # HMAC access key for accessing GCS private buckets
HMAC_SECRET_ACCESS_KEY= # HMAC secret access key for accessing GCS private buckets

aws_credentials.env

DEV_AWS_ACCESS_KEY_ID= # dev access key #
DEV_AWS_SECRET_ACCESS_KEY= # dev secret token #
DEV_AWS_SESSION_TOKEN= # dev session token #

PROD_AWS_ACCESS_KEY_ID= # prod access key #
PROD_AWS_SECRET_ACCESS_KEY= # prod secret key #
PROD_AWS_SESSION_TOKEN= # prod session token #




Python Scripts

1.datasync_setup.py - create a new datsync vm, datasync agent, datasync locations, and datasync task
2. log_transfer_setup.py - set up new log transfer tasks using an existing agent and VM. Must turn on the agent first
3. upload_manifest.py - create s3 batch manifests by copying filenames from the GCS staging bucket. Creates a separate manifest for large (>5 GB) files that cannot be moved through s3 batch
4. copy_from_staging - creates the s3 batch operation to copy files in the AWS production account from staging to public. Directly copies over the large files in the AWS staging bucket to public.
 
