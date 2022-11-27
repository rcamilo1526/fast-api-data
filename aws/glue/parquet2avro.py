import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re
import boto3

args = getResolvedOptions(sys.argv, ["JOB_NAME", "s3_key", "s3_bucket"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

sts = boto3.client("sts")
account_id = sts.get_caller_identity()["Account"]
s3 = boto3.resource('s3')

bucket = args['s3_bucket']
key = args['s3_key']

input_file = f's3a://{bucket}/{key}'

ttype = (re.search('.*(DEPARTMENTS|HIRED_EMPLOYEES|JOBS).*',
                   input_file).group(1))

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [
            input_file
        ]
    }
)
avro_bucket = f"company-backup-{account_id}"
avro_key = f"backup/{ttype}.avro"
s3.Object(avro_bucket, avro_key).delete()
# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=S3bucket_node1,
    connection_type="s3",
    format="avro",
    connection_options={
        "path": f"s3://{avro_bucket}/{avro_key}",
        "partitionKeys": [],
    },
    transformation_ctx="gluejob",
)

s3.Object(bucket, key).delete()

job.commit()
