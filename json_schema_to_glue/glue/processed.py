import boto3
from delta.tables import DeltaTable
from json_schema_to_glue.glue.utils import create_spark_schema_from_glue_columns
from urllib.parse import urlparse


def create_or_update_processed_delta_table(spark, config, schema, partition_keys):
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
    schema, partition_columns = create_spark_schema_from_glue_columns(
        schema, partition_keys
    )

    # Create an empty DataFrame with the retrieved schema
    source_df = spark.createDataFrame([], schema)

    # Print the schema of the empty source DataFrame
    source_df.printSchema()

    # Define additional options for writing the DataFrame
    additional_options = {"path": processed_s3_location}

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

        glue_client.create_database(
            DatabaseInput={"Name": processed_database, "LocationUri": bucket_location}
        )
        print(
            f"Glue database '{processed_database}' created successfully with S3 location '{bucket_location}'."
        )

    if not DeltaTable.isDeltaTable(spark, processed_s3_location):
        print(f"Path {processed_s3_location} is not a Delta table")

        # Write the DataFrame as a Delta table, appending to the existing data
        source_df.write.format("delta").options(**additional_options).mode(
            "append"
        ).partitionBy(*partition_columns).saveAsTable(
            f"{processed_database}.{processed_table}"
        )
    else:
        print(f"Path {processed_s3_location} is a Delta table")

        try:
            table_response = glue_client.get_table(
                DatabaseName=processed_database, Name=processed_table
            )
            table_path = table_response["Table"]["StorageDescriptor"]["SerdeInfo"][
                "Parameters"
            ]["path"]

            assert table_path == processed_s3_location
        except glue_client.exceptions.EntityNotFoundException:
            raise Exception(
                f"Table {processed_table} not found in database {processed_database}"
            )
        except Exception as e:
            raise e

        delta_table = DeltaTable.forName(
            spark, f"{processed_database}.{processed_table}"
        )
        delta_table_df = delta_table.toDF()
        print("Delta table")
        delta_table_df.printSchema()
        delta_table_df.show(100)

        if delta_table_df.schema == source_df.schema:
            print("No change in schema")
        else:
            print("Schema change between source and target Delta table")

            source_df.write.format("delta").options(**additional_options).option(
                "mergeSchema", "true"
            ).mode("append").partitionBy(*partition_columns).saveAsTable(
                f"{processed_database}.{processed_table}"
            )

            updated_table = DeltaTable.forName(
                spark, f"{processed_database}.{processed_table}"
            )
            updated_target_df = updated_table.toDF()
            print("Updated DataFrame")
            updated_target_df.printSchema()
            updated_target_df.show(100)

            reordered_columns = source_df.schema.fieldNames()
            reordered_target_df = updated_target_df.select(*reordered_columns)

            print("Reordered DataFrame")
            reordered_target_df.printSchema()
            reordered_target_df.show(100)

            reordered_target_df.write.format("delta").options(
                **additional_options
            ).option("overwriteSchema", "true").mode("overwrite").partitionBy(
                *partition_columns
            ).saveAsTable(
                f"{processed_database}.{processed_table}"
            )
