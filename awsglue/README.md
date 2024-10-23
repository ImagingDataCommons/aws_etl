Copies of the aws glue jobs used to format and transfer logs to GCS. 

The lib/athena_glue_service_logs directory contains the library from the github https://github.com/awslabs/athena-glue-service-logs that is used to in the s3access glue jobs. Sorry I don't have the exact commit id I downloaded. This library is discussed in https://aws.amazon.com/blogs/big-data/easily-query-aws-service-logs-using-amazon-athena/.

 
The lib/athena_glue_converter_v6.0.0._new contains a modified version of this library that writes cloudtrail logs in a format that is friendly for BQ ingestion. In the original their were hangups over dates etc.


