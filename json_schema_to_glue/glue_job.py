import sys
from uuid import uuid4

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from json_schema_to_glue.glue.glue_table_creator import create_glue_tables_from_schema
from json_schema_to_glue.json_schema_parser import ZipSchema


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

    partition_keys_list = [
        "year:string",
        "month:string",
        "day:string",
        "hour:string",
        "minute:string",
    ]
    place_holders = {"[aws_env]": "sandbox", "[logical_env]": "app1"}

    extract_base_path = f"/tmp/{uuid4()}"

    zip_schema = ZipSchema(
        zip_schema=source_path, extract_base_path=extract_base_path, place_holders=place_holders
    )
    config = zip_schema.config
    schema = zip_schema.parse_schema()
    create_glue_tables_from_schema(spark, config, schema, partition_keys_list)

    job.commit()
