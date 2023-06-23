from json_schema_to_glue.glue.table_manager import GlueTableManager
from json_schema_to_glue.glue.utils import (
    convert_json_schema_to_glue_columns,
    glue_data_formats_mapping,
)


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
    staging_glue_columns = convert_json_schema_to_glue_columns(
        schema, flatten=True, delimiter="__"
    )

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
        table_type="EXTERNAL_TABLE",
        description="This is a sample table",
        database_name=staging_database,
        table_name=staging_table,
        columns=staging_glue_columns,
        location=staging_s3_location,
        input_format=staging_input_format,
        output_format=staging_output_format,
        serde_info=staging_serde_info,
        partition_keys=partition_keys,
        parameters=staging_parameters,
    )

    # Create or update the staging table in AWS Glue
    staging_table_manager.create_or_update_table()
