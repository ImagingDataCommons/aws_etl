aws glue create-job --name S3AccessLogConvertorCr \
--description "Convert and partition S3 Access logs in CR bucket" \
--role AWSGlueServiceRoleDefault \
--command Name=glueetl,ScriptLocation=s3://idc-script-bucket/glue_scripts/s3_access_job.py \
--default-arguments '{
  "--extra-py-files":"s3://idc-script-bucket/glue_scripts/athena_glue_service_logs.zip",
  "--job-bookmark-option":"job-bookmark-enable",
  "--raw_database_name":"s3_access_logs",
  "--raw_table_name":"s3_access_cr_raw",
  "--converted_database_name":"s3_access_logs",
  "--converted_table_name":"s3_access_cr_conv",
  "--TempDir":"s3://idc-etl-temp/tmp",
  "--s3_converted_target":"s3://idc-open-data-cr-logs/conv/",
  "--s3_source_location":"s3://idc-open-data-cr-logs/orig/"
}'
