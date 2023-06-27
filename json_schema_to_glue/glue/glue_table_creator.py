from json_schema_to_glue.glue.raw import create_or_update_raw_table
from json_schema_to_glue.glue.staging import create_or_update_staging_table
from json_schema_to_glue.glue.processed import create_or_update_processed_delta_table
from json_schema_to_glue.glue.utils import create_partition_keys


def create_glue_tables_from_schema(spark, config, schema, partition_keys_list: list = None):
    partition_keys = create_partition_keys(partition_keys_list)

    print("Creating or updating raw table")
    create_or_update_raw_table(config, schema, partition_keys)

    print("Creating or updating staging table")
    create_or_update_staging_table(config, schema, partition_keys)

    print("Creating or updating processed delta table")
    create_or_update_processed_delta_table(spark, config, schema, partition_keys)
