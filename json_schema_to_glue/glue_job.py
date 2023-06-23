import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from json_schema_to_glue.schema_parser import parse_schema
from json_schema_to_glue.glue_table_creator import create_glue_tables_from_schema

if __name__ == "__main__":
    args = getResolvedOptions(
        sys.argv, ["JOB_NAME", "schema_files_bucket", "schema_files_prefix"]
    )

    bucket_name = args["schema_files_bucket"]
    prefix = args["schema_files_prefix"]
    source_path = f"s3://{bucket_name}/{prefix}"

    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    place_holders = {"[aws_env]": "sandbox", "[logical_env]": "sai"}
    partition_keys_list = [
        "year:string",
        "month:string",
        "day:string",
        "hour:string",
        "minute:string",
    ]

    data = parse_schema(source_path, place_holders=place_holders, partition_keys_list=partition_keys_list)
    create_glue_tables_from_schema(spark, data['config'], data['schema'], data['partition_keys'])

    job.commit()
