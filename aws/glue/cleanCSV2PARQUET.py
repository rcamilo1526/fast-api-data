import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col
import re
import boto3
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


table_dict = {
    'hired_employees': StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("datetime", StringType(), True),
        StructField("department_id", IntegerType(), True),
        StructField("job_id", IntegerType(), True)]),
    'departments': StructType([
        StructField("id", IntegerType(), True),
        StructField("department", StringType(), True)]),
    'jobs': StructType([
        StructField("id", IntegerType(), True),
        StructField("job", StringType(), True)]),
}

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

ttype = (re.search('.*(departments|hired_employees|jobs).*',
                   input_file).group(1))


df = spark.read.csv(input_file, header=False, schema=table_dict[ttype])

for c in df.columns:
    df = df.filter(col(c).isNotNull())

path = f's3://{bucket}/data/{ttype}.parquet'

df.write.mode("overwrite").option("encoding", "UTF-8").parquet(path)

s3 = boto3.resource('s3')
s3.Object(bucket, key).delete()

job.commit()
