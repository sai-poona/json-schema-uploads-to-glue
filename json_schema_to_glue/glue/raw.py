from json_schema_to_glue.glue.table_manager import GlueTableManager
from json_schema_to_glue.glue.utils import (
    convert_json_schema_to_glue_columns,
    glue_data_formats_mapping,
)


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
        table_type="EXTERNAL_TABLE",
        description="This is a sample table",
        database_name=raw_database,
        table_name=raw_table,
        columns=raw_glue_columns,
        location=raw_s3_location,
        input_format=raw_input_format,
        output_format=raw_output_format,
        serde_info=raw_serde_info,
        partition_keys=partition_keys,
        parameters=raw_parameters,
    )

    # Create or update the raw table in AWS Glue
    raw_table_manager.create_or_update_table()
