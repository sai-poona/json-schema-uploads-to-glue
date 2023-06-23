import boto3
import json
import shutil
from uuid import uuid4

from json_schema_to_glue.schema.ref_resolver import JSONRefResolver
from json_schema_to_glue.schema.utils import *


def download_extract_file_from_s3(source_path):
    """
    Download a file from Amazon S3 and extract its contents to a temporary directory.

    Args:
        source_path (str): The name of the local or S3 zip file path.
    Returns:
        str: The path of the temporary directory where the file has been extracted.
    """
    temp_dir = f"/tmp/{uuid4()}"
    # Create the temporary directory if it doesn't exist
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    # Generate a unique file name for the temporary file
    file_name = os.path.join(temp_dir, os.path.basename(source_path))

    if "s3://" in source_path or "s3a://" in source_path:
        _, _, bucket_name, prefix = source_path.split("/", 3)
        # Initialize the S3 client
        s3 = boto3.client("s3")
        # Download the file from S3 to the temporary path
        s3.download_file(bucket_name, prefix, file_name)
    else:
        file_name = source_path

    # Extract the contents of the file to the temporary directory
    shutil.unpack_archive(file_name, temp_dir)

    # Return the path of the temporary directory
    return temp_dir


def load_json_schema(schema_file_location):
    """
    Loads a JSON schema from a file.

    Args:
        schema_file_location (str): The path to the JSON schema file.

    Returns:
        dict: The loaded JSON schema.
    """
    # Open the JSON schema file
    with open(schema_file_location) as schema_file:
        # Load the JSON schema
        schema = json.load(schema_file)

    # Create an instance of JSONRefResolver
    resolver = JSONRefResolver(schema)

    # Resolve the $ref references
    resolved_schema = resolver.resolve_refs()

    return resolved_schema


def parse_schema(zip_file_path):
    assert zip_file_path.endswith(".zip")
    extracted_files_path = download_extract_file_from_s3(source_path=zip_file_path)

    config_file_path = find_config_file(extracted_files_path)

    if not config_file_path:
        raise Exception("Config file not found.")

    placeholders = {"[aws_env]": "sandbox", "[logical_env]": "sai"}
    config = load_yaml_config(config_file_path, placeholders)

    current_working_directory = os.getcwd()
    os.chdir(extracted_files_path)

    main_schema_file = config["main_schema_file"]
    schema = load_json_schema(main_schema_file)

    os.chdir(current_working_directory)

    partition_key_list = [
        "year:string",
        "month:string",
        "day:string",
        "hour:string",
        "minute:string",
    ]
    partition_keys = create_partition_keys(partition_key_list)
    return dict(config=config, schema=schema, partition_keys=partition_keys)
