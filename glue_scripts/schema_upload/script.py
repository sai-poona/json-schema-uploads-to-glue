import json
import os
import re
import shutil
import sys
from urllib.parse import urlparse

import boto3
import requests
import yaml
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from pyspark.context import SparkContext
from pyspark.sql.functions import lit
from pyspark.sql.types import _parse_datatype_string

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)


class JSONRefResolver:
    """
    A class for resolving $refs in a JSON schema.
    """

    def __init__(self, schema):
        """
        Initialize the JSONRefResolver.

        Args:
            schema (dict): The JSON schema to resolve.
        """
        self.schema = schema
        self.resolved_refs = {}

    def resolve_refs(self):
        """
        Resolve the $refs in the JSON schema.

        Returns:
            dict: The resolved JSON schema.
        """
        self._resolve_refs_recursive(self.schema)
        return self.schema

    def _resolve_refs_recursive(self, obj):
        """
        Recursively resolve $refs in the JSON schema.

        Args:
            obj (dict or list): The JSON object to process.
        """
        if isinstance(obj, dict):
            if "$ref" in obj:
                ref_value = obj["$ref"]
                if ref_value.startswith("#/definitions/"):
                    resolved_value = self._resolve_definition(ref_value)
                    if resolved_value is not None:
                        obj.clear()
                        obj.update(resolved_value)
                else:
                    self._resolve_ref(obj, ref_value)
            else:
                for value in obj.values():
                    self._resolve_refs_recursive(value)
        elif isinstance(obj, list):
            for item in obj:
                self._resolve_refs_recursive(item)

    def _resolve_definition(self, definition):
        """
        Resolve a definition referenced by a $ref.

        Args:
            definition (str): The $ref value representing the definition.

        Returns:
            dict: The resolved definition.
        """
        if definition in self.resolved_refs:
            return self.resolved_refs[definition]

        parts = definition.split("/")
        assert len(parts) == 3
        current = self.schema

        for part in parts[1:]:
            if part == "definitions":
                continue
            current = current["definitions"][part]
            if current is None:
                break

        if current is not None:
            self.resolved_refs[definition] = current
            self._resolve_refs_recursive(current)

            return current

    def _resolve_ref(self, obj, ref_value):
        """
        Resolve a $ref that references another schema.

        Args:
            obj (dict): The JSON object containing the $ref.
            ref_value (str): The $ref value.

        Raises:
            Exception: If the $ref references a local file that is not found or an external URL that fails to load.
        """
        parsed_url = urlparse(ref_value)
        if parsed_url.scheme and parsed_url.netloc:
            self._resolve_external_url_ref(obj, ref_value)
        else:
            self._resolve_local_file_ref(obj, ref_value)

    def _resolve_local_file_ref(self, obj, ref_value):
        """
        Resolve a $ref that references a local file.

        Args:
            obj (dict): The JSON object containing the $ref.
            ref_value (str): The $ref value.

        Raises:
            Exception: If the local file is not found.
        """
        if os.path.isfile(ref_value):
            with open(ref_value) as file:
                local_schema = json.load(file)

            if "definitions" in local_schema:
                if "definitions" in self.schema:
                    self.schema["definitions"].update(local_schema["definitions"])
                else:
                    self.schema["definitions"] = local_schema["definitions"]
                local_schema.pop("definitions")

            self._resolve_refs_recursive(local_schema)

            obj.clear()
            obj.update(local_schema)
        else:
            raise Exception(f"Error resolving local file reference: Schema file not found - {ref_value}")

    def _resolve_external_url_ref(self, obj, ref_value):
        """
        Resolve a $ref that references an external URL.

        Args:
            obj (dict): The JSON object containing the $ref.
            ref_value (str): The $ref value.

        Raises:
            Exception: If the external URL fails to load.
        """
        response = requests.get(ref_value)
        if response.status_code == 200:
            external_schema = response.json()

            if "definitions" in external_schema:
                if "definitions" in self.schema:
                    self.schema["definitions"].update(external_schema["definitions"])
                else:
                    self.schema["definitions"] = external_schema["definitions"]
                external_schema.pop("definitions")

            self._resolve_refs_recursive(external_schema)
            obj.clear()
            obj.update(external_schema)
        else:
            raise Exception(f"Error resolving external URL reference: {response.status_code} - {ref_value}")


def replace_placeholders(dictionary, placeholders):
    """
    Replaces placeholders in a dictionary with corresponding values using regex.

    Args:
        dictionary (dict): The dictionary to process.
        placeholders (dict): A dictionary of placeholders and their corresponding values.

    Returns:
        dict: The updated dictionary with replaced placeholders.
    """
    replaced_dict = {}
    for key, value in dictionary.items():
        if isinstance(value, dict):
            # Recursively process nested dictionaries
            replaced_dict[key] = replace_placeholders(value, placeholders)
        elif isinstance(value, str):
            # Replace placeholders in string values using regex
            pattern = re.compile(r'\[(.*?)\]')
            replaced_value = pattern.sub(lambda x: placeholders[x.group()], value)
            replaced_dict[key] = replaced_value
        else:
            # Keep non-string, non-dict values as is
            replaced_dict[key] = value
    return replaced_dict


def convert_to_lower_with_underscore(string):
    """
    Converts a string to lowercase, replaces special characters with an underscore,
    and strips leading/trailing whitespaces.

    Args:
        string (str): The input string.

    Returns:
        str: The converted string.
    """
    # Strip leading/trailing whitespaces
    stripped_string = string.strip()

    # Convert the string to lowercase
    lowercase_string = stripped_string.lower()

    # Replace special characters with an underscore
    converted_string = re.sub(r'[^a-z0-9_]+', '_', lowercase_string)

    return converted_string


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


def load_yaml_config(config_file_location, placeholders):
    """
    Loads a configuration file in YAML format.

    Args:
        config_file_location (str): The path to the configuration file.
        placeholders (dict): A dictionary of placeholders and their corresponding values.

    Returns:
        dict: The loaded configuration data.
    """
    # Open the configuration file
    with open(config_file_location) as config_file:
        # Load the YAML content
        _config = yaml.safe_load(config_file)
        config = replace_placeholders(_config, placeholders)

    return config


def download_extract_file_from_s3(bucket_name, object_key, temp_file="files.zip"):
    """
    Download a file from Amazon S3 and extract its contents to a temporary directory.

    Args:
        bucket_name (str): The name of the S3 bucket.
        object_key (str): The key of the file in the S3 bucket.
        temp_file (str): The name of the temporary file to download from S3.

    Returns:
        str: The path of the temporary directory where the file has been extracted.
    """
    temp_dir = "/tmp/temp_dir"

    # Create the temporary directory if it doesn't exist
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    # Initialize the S3 client
    s3 = boto3.client("s3")

    # Generate a unique file name for the temporary file
    file_name = os.path.join(temp_dir, temp_file)

    # Download the file from S3 to the temporary path
    s3.download_file(bucket_name, object_key, file_name)

    # Extract the contents of the file to the temporary directory
    shutil.unpack_archive(os.path.join(temp_dir, temp_file), temp_dir)

    # Return the path of the temporary directory
    return temp_dir


def find_config_file(temp_dir):
    """
    Find a config file with the extension '.config.yml' in the specified directory.

    Args:
        temp_dir (str): The path of the directory where the extracted files are stored.

    Returns:
        str: The path of the config file if found, or None if not found.
    """
    for root, dirs, files in os.walk(temp_dir):
        for file in files:
            if file.endswith(".config.yml"):
                return os.path.join(root, file)
    return None


def load_json_from_s3(bucket_name, object_key):
    """
    Retrieves a JSON file from an S3 bucket and loads its contents.

    Args:
        bucket_name (str): The name of the S3 bucket.
        object_key (str): The key of the JSON file within the bucket.

    Returns:
        dict: The loaded JSON data.
    """
    # Create an S3 client
    s3 = boto3.client('s3')

    # Retrieve the object from S3
    response = s3.get_object(Bucket=bucket_name, Key=object_key)

    # Read the JSON content from the response
    json_content = response['Body'].read().decode('utf-8')

    # Parse the JSON content
    data = json.loads(json_content)

    return data


def load_yaml_from_s3(bucket_name, object_key):
    """
    Retrieves a YAML file from an S3 bucket and loads its contents.

    Args:
        bucket_name (str): The name of the S3 bucket.
        object_key (str): The key of the YAML file within the bucket.

    Returns:
        dict: The loaded YAML data.
    """
    # Create an S3 client
    s3 = boto3.client('s3')

    # Retrieve the object from S3
    response = s3.get_object(Bucket=bucket_name, Key=object_key)

    # Read the YAML content from the response
    yaml_content = response['Body'].read().decode('utf-8')

    # Parse the YAML content
    yaml_data = yaml.safe_load(yaml_content)

    return yaml_data


def create_partition_keys(partition_key_list):
    """
    Creates a list of partition keys from a given partition key list.

    Args:
        partition_key_list (list): A list of partition keys in the format "key_name:key_type".

    Returns:
        list: A list of partition keys as dictionaries, with each dictionary containing "Name" and "Type" keys.

    Example:
        Input: ["year:int", "month:string"]
        Output: [{"Name": "year", "Type": "int"}, {"Name": "month", "Type": "string"}]
    """
    partition_keys = []
    for partition_key in partition_key_list:
        # Split the partition key into name and type
        key_name, key_type = partition_key.split(":")

        # Create a dictionary for the partition key
        partition_key_dict = {
            "Name": key_name.strip(),
            "Type": key_type.strip()
        }

        # Add the partition key to the list
        partition_keys.append(partition_key_dict)

    return partition_keys


def get_additional_columns(pipeline_type):
    """
    Get additional columns based on the pipeline type.

    Args:
        pipeline_type (str): The pipeline type.

    Returns:
        list or None: The additional columns as a list of dictionaries, or None if the pipeline type is unknown.
    """
    if pipeline_type.lower() == "scd1":
        # Additional columns for SCD1 pipeline type
        additional_columns = [{
            "Name": "last_updated",
            "Type": "STRING"
        }]
    elif pipeline_type.lower() == "scd2":
        # Additional columns for SCD2 pipeline type
        additional_columns = [{
            "Name": "last_updated",
            "Type": "STRING"
        },
        {
            "Name": "active",
            "Type": "STRING"
        }]
    else:
        # Unknown pipeline type, return None
        additional_columns = None
    
    return additional_columns


def flatten_json_schema(json_schema, prefix='', delimiter='_'):
    """
    Flattens a JSON schema by unwrapping nested structs and creating flattened property names.

    Args:
        json_schema (dict): The JSON schema.
        prefix (str): Prefix for the flattened property names (used for recursion).
        delimiter (str): Delimiter to separate flattened property names.

    Returns:
        dict: The flattened JSON schema.
    """
    flattened_schema = {}

    for key, value in json_schema["properties"].items():
        if value["type"] == "object":
            # Flatten the nested struct recursively
            flattened_properties = flatten_json_schema(value, prefix=f"{prefix}{key}{delimiter}", delimiter=delimiter)
            flattened_schema.update(flattened_properties)
        else:
            # Create the flattened property name
            flattened_key = f"{prefix}{key}"
            flattened_schema[flattened_key] = value

    return flattened_schema


def glue_column_type_from_json_schema(value):
    """
    Converts a JSON schema type to an AWS Glue column type.

    Args:
        value (dict): The JSON schema type.

    Returns:
        str: The corresponding AWS Glue column type.

    Raises:
        Exception: If the JSON schema type is unknown or an empty array is encountered.
    """
    if value["type"] == "string":
        return "STRING"
    elif value["type"] == "integer":
        return "BIGINT"
    elif value["type"] == "number":
        return "DOUBLE"
    elif value["type"] == "boolean":
        return "BOOLEAN"
    elif value["type"] == "object":
        _columns = []

        # Convert each property of the object to Glue column type
        for _key, _value in value["properties"].items():
            if "type" not in _value:
                raise Exception(f"Required key 'type' not found for {_key}.")

            _key = convert_to_lower_with_underscore(_key)
            _column_type = glue_column_type_from_json_schema(_value)
            _column = f"{_key}:{_column_type}"
            _columns.append(_column)
        
        return f"STRUCT<{','.join(_columns)}>"
    elif value["type"] == "array":
        items = value["items"]

        if len(items) == 0:
            raise Exception("Empty arrays are not allowed in Glue")

        if items["type"] == "array":
            return f"ARRAY<{glue_column_type_from_json_schema(items)}>[]"
        else:
            return f"ARRAY<{glue_column_type_from_json_schema(items)}>"
    else:
        raise Exception("Unknown type")


def convert_json_schema_to_glue_columns(json_schema, flatten=False, delimiter='_'):
    """
    Converts a JSON schema to a list of AWS Glue columns.

    Args:
        json_schema (dict): The JSON schema.
        flatten (bool): Whether to flatten the schema before generating Glue columns.
        delimiter (str): Delimiter to separate flattened property names.

    Returns:
        list: The list of Glue columns.
    """

    if "properties" not in json_schema:
        raise Exception("Required key 'properties' not found in the JSON schema.")

    if flatten:
        flattened_schema = flatten_json_schema(json_schema, delimiter=delimiter)
        schema_properties = flattened_schema
    else:
        schema_properties = json_schema["properties"]

    columns = []

    for key, value in schema_properties.items():
        if "type" not in value:
            raise Exception(f"Required key 'type' not found for {key}.")

        column = {
            "Name": convert_to_lower_with_underscore(key),
            "Type": glue_column_type_from_json_schema(value)
        }
    
        columns.append(column)

    return columns

class GlueTableManager:
    def __init__(self, database_name, table_name, columns, location, input_format, output_format, serde_info, partition_keys=None, table_type='EXTERNAL_TABLE', description='', parameters=None):
        """
        Initialize the GlueTableManager.

        Args:
            database_name (str): Name of the database.
            table_name (str): Name of the table.
            columns (list): List of column definitions.
            location (str): S3 location of the table data.
            input_format (str): Input format of the table.
            output_format (str): Output format of the table.
            serde_info (dict): Serde information for the table.
            partition_keys (list, optional): List of partition key definitions. Defaults to None.
            table_type (str, optional): Type of the table. Defaults to 'EXTERNAL_TABLE'.
            description (str, optional): Description of the table. Defaults to ''.
            parameters (dict, optional): Additional parameters for the table. Defaults to None.
        """
        self.glue_client = boto3.client('glue')
        self.table_type = table_type
        self.description = description
        self.database_name = database_name
        self.table_name = table_name
        self.columns = columns
        self.location = location
        self.input_format = input_format
        self.output_format = output_format
        self.serde_info = serde_info
        self.partition_keys = partition_keys if partition_keys else []
        self.parameters = parameters if parameters else {}

    def create_database_if_not_exist(self):
        """
        Creates the database if it does not exist.
        """
        try:
            # Attempt to retrieve the database
            self.glue_client.get_database(Name=self.database_name)
        except self.glue_client.exceptions.EntityNotFoundException:
            # Database does not exist, create it
            self.glue_client.create_database(DatabaseInput={'Name': self.database_name})

    def create_or_update_table(self):
        """
        Creates or updates a table in AWS Glue.

        Returns:
            dict: The response from the Glue API.
        """
        self.create_database_if_not_exist()

        table_input = {
            'Name': self.table_name,
            'Description': self.description,
            'StorageDescriptor': {
                'Columns': self.columns,
                'Location': self.location,
                'InputFormat': self.input_format,
                'OutputFormat': self.output_format,
                'SerdeInfo': self.serde_info,
            },
            'PartitionKeys': self.partition_keys,
            'TableType': self.table_type,
            'Parameters': self.parameters
        }

        try:
            # Try to create the table
            response = self.glue_client.create_table(
                DatabaseName=self.database_name,
                TableInput=table_input
            )
        except self.glue_client.exceptions.AlreadyExistsException:
            # Table already exists, update it instead
            response = self.glue_client.update_table(
                DatabaseName=self.database_name,
                TableInput=table_input
            )
        return response


def create_or_update_raw_table(config, schema, partition_keys):
    """
    Create or update a raw table in AWS Glue based on the provided configuration.

    Args:
        config (dict): The configuration for the raw table.
        schema (dict): The schema definition of the table.
        partition_keys (list): The list of partition keys for the table.

    Returns:
        None
    """
    # Convert JSON schema to Glue columns
    raw_glue_columns = convert_json_schema_to_glue_columns(schema)

    # Retrieve raw zone configuration from the provided config
    raw_zone_config = config["storage_zones"]["raw"]
    raw_database = raw_zone_config["database"]
    raw_table = raw_zone_config["table"]
    raw_s3_location = raw_zone_config["s3_location"]

    # Retrieve Glue data formats based on the specified data format in the config
    raw_glue_data_formats = glue_data_formats_mapping[config["data_format"].lower()]

    # Extract individual components from the Glue data formats
    raw_input_format = raw_glue_data_formats["input_format"]
    raw_output_format = raw_glue_data_formats["output_format"]
    raw_serde_info = raw_glue_data_formats["serde_info"]
    raw_parameters = raw_glue_data_formats["parameters"]

    # Create a GlueTableManager instance for the raw table
    raw_table_manager = GlueTableManager(
        table_type='EXTERNAL_TABLE',
        description='This is a sample table',
        database_name=raw_database,
        table_name=raw_table,
        columns=raw_glue_columns,
        location=raw_s3_location,
        input_format=raw_input_format,
        output_format=raw_output_format,
        serde_info=raw_serde_info,
        partition_keys=partition_keys,
        parameters=raw_parameters
    )

    # Create or update the raw table in AWS Glue
    raw_table_manager.create_or_update_table()


def create_or_update_staging_table(config, schema, partition_keys):
    """
    Create or update a staging table in AWS Glue based on the provided configuration.

    Args:
        config (dict): The configuration for the staging table.
        schema (dict): The schema definition of the table.
        partition_keys (list): The list of partition keys for the table.

    Returns:
        None
    """
    # Convert JSON schema to flattened Glue columns
    staging_glue_columns = convert_json_schema_to_glue_columns(schema, flatten=True, delimiter='__')

    # Retrieve staging zone configuration from the provided config
    staging_zone_config = config["storage_zones"]["staging"]
    staging_database = staging_zone_config["database"]
    staging_table = staging_zone_config["table"]
    staging_s3_location = staging_zone_config["s3_location"]

    # Retrieve Glue data formats for staging table
    staging_glue_data_formats = glue_data_formats_mapping["parquet"]

    # Extract individual components from the Glue data formats
    staging_input_format = staging_glue_data_formats["input_format"]
    staging_output_format = staging_glue_data_formats["output_format"]
    staging_serde_info = staging_glue_data_formats["serde_info"]
    staging_parameters = staging_glue_data_formats["parameters"]

    # Create a GlueTableManager instance for the staging table
    staging_table_manager = GlueTableManager(
        table_type='EXTERNAL_TABLE',
        description='This is a sample table',
        database_name=staging_database,
        table_name=staging_table,
        columns=staging_glue_columns,
        location=staging_s3_location,
        input_format=staging_input_format,
        output_format=staging_output_format,
        serde_info=staging_serde_info,
        partition_keys=partition_keys,
        parameters=staging_parameters
    )

    # Create or update the staging table in AWS Glue
    staging_table_manager.create_or_update_table()


def create_spark_schema_from_glue_columns(schema, partition_keys):
    """
    Create a Spark schema from the provided Glue column schema.

    Args:
        schema (dict): The schema definition of the table.
        partition_keys (list): The list of partition keys for the table.

    Returns:
        Tuple[StructType, List[str]]: A tuple containing the Spark schema and a list of partition column names.
    """
    # Convert JSON schema to flattened Glue columns
    processed_glue_columns = convert_json_schema_to_glue_columns(schema, flatten=True, delimiter='__')

    # Join column names and types using a comma
    schema_str = ",".join(f'{column["Name"]} {column["Type"]}' for column in processed_glue_columns + partition_keys)

    # Parse the datatype string to create a Spark schema
    spark_schema = _parse_datatype_string(schema_str)

    # Extract partition column names from the provided partition_keys
    partition_columns = [column["Name"] for column in partition_keys]

    # # Modify the Spark schema fields to set nullable property based on partition columns
    # schema_fields = []
    # for field in spark_schema.fields:
    #     nullable = not field.name in partition_columns
    #     new_field = StructField(field.name, field.dataType, nullable=nullable, metadata=field.metadata)
    #     schema_fields.append(new_field)

    # # Create a new StructType with modified schema fields
    # spark_schema = StructType(schema_fields)

    return spark_schema, partition_columns


def create_or_update_processed_delta_table(config, schema, partition_keys):
    """
    Create or update a Delta table in the processed zone based on the provided configuration, schema, and partition keys.

    Args:
        config (dict): The configuration for the storage zones.
        schema (dict): The schema definition of the table.
        partition_keys (list): The list of partition keys.

    Returns:
        None
    """
    processed_zone_config = config["storage_zones"]["processed"]
    processed_database = processed_zone_config["database"]
    processed_table = processed_zone_config["table"]
    processed_s3_location = processed_zone_config["s3_location"]

    # Retrieve the schema and partition keys for the processed table
    schema, partition_columns = create_spark_schema_from_glue_columns(schema, partition_keys)

    # Create an empty DataFrame with the retrieved schema
    source_df = spark.createDataFrame([], schema)

    # Print the schema of the empty source DataFrame
    source_df.printSchema()

    # Define additional options for writing the DataFrame
    additional_options = {
        "path": processed_s3_location
    }

    glue_client = boto3.client("glue")

    try:
        glue_client.get_database(Name=processed_database)
        print(f"Glue database '{processed_database}' already exists.")
    except glue_client.exceptions.EntityNotFoundException:
        # Database does not exist, create a new one
        print(f"Glue database '{processed_database}' does not exist. Creating...")

        # Extract the bucket location from the S3 URI
        parsed_uri = urlparse(processed_s3_location)
        bucket_location = f"s3://{parsed_uri.netloc}/"

        glue_client.create_database(DatabaseInput={"Name": processed_database, "LocationUri": bucket_location})
        print(f"Glue database '{processed_database}' created successfully with S3 location '{bucket_location}'.")

    if not DeltaTable.isDeltaTable(spark, processed_s3_location):
        print(f"Path {processed_s3_location} is not a Delta table")

        # Write the DataFrame as a Delta table, appending to the existing data
        source_df.write \
            .format("delta") \
            .options(**additional_options) \
            .mode("append") \
            .partitionBy(*partition_columns) \
            .saveAsTable(f"{processed_database}.{processed_table}")
    else:
        print(f"Path {processed_s3_location} is a Delta table")

        try:
            table_response = glue_client.get_table(DatabaseName=processed_database, Name=processed_table)
            table_path = table_response["Table"]["StorageDescriptor"]["SerdeInfo"]["Parameters"]["path"]

            assert table_path == processed_s3_location
        except glue_client.exceptions.EntityNotFoundException:
            raise Exception(f"Table {processed_table} not found in database {processed_database}")
        except Exception as e:
            raise e

        delta_table = DeltaTable.forName(spark, f"{processed_database}.{processed_table}")
        delta_table_df = delta_table.toDF()
        print("Delta table")
        delta_table_df.printSchema()
        delta_table_df.show(100)

        if delta_table_df.schema == source_df.schema:
            print("No change in schema")
        else:
            print("Schema change between source and target Delta table")

            source_df.write \
                .format("delta") \
                .options(**additional_options) \
                .option("mergeSchema", "true") \
                .mode("append") \
                .partitionBy(*partition_columns) \
                .saveAsTable(f"{processed_database}.{processed_table}")

            updated_table = DeltaTable.forName(spark, f"{processed_database}.{processed_table}")
            updated_target_df = updated_table.toDF()
            print("Updated DataFrame")
            updated_target_df.printSchema()
            updated_target_df.show(100)

            reordered_columns = source_df.schema.fieldNames()
            reordered_target_df = updated_target_df.select(*reordered_columns)

            print("Reordered DataFrame")
            reordered_target_df.printSchema()
            reordered_target_df.show(100)

            reordered_target_df.write \
                .format("delta") \
                .options(**additional_options) \
                .option("overwriteSchema", "true") \
                .mode("overwrite") \
                .partitionBy(*partition_columns) \
                .saveAsTable(f"{processed_database}.{processed_table}")


glue_data_formats_mapping ={
    "csv": {
        "input_format": "org.apache.hadoop.mapred.TextInputFormat",
        "output_format": "org.apache.hadoop.hive.ql.io.HivelgnoreKeyTextOutputFormat",
        "serde_info": {
            "SerializationLibrary": "org.apache.hadoop.hive.serde2.OpenCSVSerde",
            "Parameters": {
                "separatorChar": ","
            }
        },
        "parameters": {
            "classification": "csv"
        }
    },
    "json": {
        "input_format": "org.apache.hadoop.mapred.TextInputFormat",
        "output_format": "org.apache.hadoop.hive.qlio.HivelgnoreKeyTextOutputFormat",
        "serde_info": {
            "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe"
        },
        "parameters": {
            "classification": "json"
        }
    },
    "parquet": {
        "input_format": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        "output_format": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        "serde_info": {
            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            "Parameters": {
                "serialization.format": "1"
            }
        },
        "parameters": {
            "classification": "parquet"
        }
    }
}

placeholders = {
    "[aws_env]": "sandbox",
    "[logical_env]": "sai"
}

partition_key_list = [
    "year:string",
    "month:string",
    "day:string",
    "hour:string",
    "minute:string"
]

if __name__ == "__main__":

    args = getResolvedOptions(sys.argv,
        [
            'JOB_NAME',
            'schema_files_bucket',
            'schema_files_prefix'
        ]
    )

    job.init(args["JOB_NAME"], args)

    extracted_files_path = download_extract_file_from_s3(
        bucket_name=args["schema_files_bucket"],
        object_key=args["schema_files_prefix"],
        temp_file="schema_files.zip"
    )

    config_file_path = find_config_file(extracted_files_path)

    if not config_file_path:
        raise Exception("Config file not found.")

    config = load_yaml_config(config_file_path, placeholders)

    current_working_directory = os.getcwd()
    os.chdir(extracted_files_path)

    main_schema_file = config["main_schema_file"]
    schema = load_json_schema(main_schema_file)

    os.chdir(current_working_directory)

    partition_keys = create_partition_keys(partition_key_list)

    try:
        print("Creating or updating raw table")
        create_or_update_raw_table(config, schema, partition_keys)

        print("Creating or updating staging table")
        create_or_update_staging_table(config, schema, partition_keys)

        print("Creating or updating processed delta table")
        create_or_update_processed_delta_table(config, schema, partition_keys)
    except Exception as e:
        raise e

    job.commit()