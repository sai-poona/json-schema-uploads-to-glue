import re
from pyspark.sql.types import _parse_datatype_string


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
        if "contentEncoding" in value and value["contentEncoding"] == "base64":
            return "BINARY"
        return "STRING"
    elif value["type"] == "integer":
        return "INT"
    elif value["type"] == "number":
        return "FLOAT"
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


def flatten_json_schema(json_schema, prefix="", delimiter="_"):
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
            flattened_properties = flatten_json_schema(
                value, prefix=f"{prefix}{key}{delimiter}", delimiter=delimiter
            )
            flattened_schema.update(flattened_properties)
        else:
            # Create the flattened property name
            flattened_key = f"{prefix}{key}"
            flattened_schema[flattened_key] = value

    return flattened_schema


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
    converted_string = re.sub(r"[^a-z0-9_]+", "_", lowercase_string)

    return converted_string


def convert_json_schema_to_glue_columns(json_schema, flatten=False, delimiter="_"):
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
            "Type": glue_column_type_from_json_schema(value),
        }

        columns.append(column)

    return columns


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
    processed_glue_columns = convert_json_schema_to_glue_columns(
        schema, flatten=True, delimiter="__"
    )

    # Join column names and types using a comma
    schema_str = ",".join(
        f'{column["Name"]} {column["Type"]}'
        for column in processed_glue_columns + partition_keys
    )

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


glue_data_formats_mapping = {
    "csv": {
        "input_format": "org.apache.hadoop.mapred.TextInputFormat",
        "output_format": "org.apache.hadoop.hive.ql.io.HivelgnoreKeyTextOutputFormat",
        "serde_info": {
            "SerializationLibrary": "org.apache.hadoop.hive.serde2.OpenCSVSerde",
            "Parameters": {"separatorChar": ","},
        },
        "parameters": {"classification": "csv"},
    },
    "json": {
        "input_format": "org.apache.hadoop.mapred.TextInputFormat",
        "output_format": "org.apache.hadoop.hive.qlio.HivelgnoreKeyTextOutputFormat",
        "serde_info": {"SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe"},
        "parameters": {"classification": "json"},
    },
    "parquet": {
        "input_format": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        "output_format": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        "serde_info": {
            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            "Parameters": {"serialization.format": "1"},
        },
        "parameters": {"classification": "parquet", "useGlueParquetWriter": "true"},
    },
}


def create_partition_keys(partition_keys_list: list):
    """
    Creates a list of partition keys from a given partition key list.

    Args:
        partition_keys_list (list): A list of partition keys in the format "key_name:key_type".

    Returns:
        list: A list of partition keys as dictionaries, with each dictionary containing "Name" and "Type" keys.

    Example:
        Input: ["year:int", "month:string"]
        Output: [{"Name": "year", "Type": "int"}, {"Name": "month", "Type": "string"}]
    """
    partition_keys = []
    for partition_key in partition_keys_list:
        # Split the partition key into name and type
        key_name, key_type = partition_key.split(":")

        # Create a dictionary for the partition key
        partition_key_dict = {"Name": key_name.strip(), "Type": key_type.strip()}

        # Add the partition key to the list
        partition_keys.append(partition_key_dict)

    return partition_keys
