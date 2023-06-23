import boto3


class GlueTableManager:
    def __init__(
        self,
        database_name,
        table_name,
        columns,
        location,
        input_format,
        output_format,
        serde_info,
        partition_keys=None,
        table_type="EXTERNAL_TABLE",
        description="",
        parameters=None,
    ):
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
        self.glue_client = boto3.client("glue")
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
            self.glue_client.create_database(DatabaseInput={"Name": self.database_name})

    def create_or_update_table(self):
        """
        Creates or updates a table in AWS Glue.

        Returns:
            dict: The response from the Glue API.
        """
        self.create_database_if_not_exist()

        table_input = {
            "Name": self.table_name,
            "Description": self.description,
            "StorageDescriptor": {
                "Columns": self.columns,
                "Location": self.location,
                "InputFormat": self.input_format,
                "OutputFormat": self.output_format,
                "SerdeInfo": self.serde_info,
            },
            "PartitionKeys": self.partition_keys,
            "TableType": self.table_type,
            "Parameters": self.parameters,
        }

        try:
            # Try to create the table
            response = self.glue_client.create_table(
                DatabaseName=self.database_name, TableInput=table_input
            )
        except self.glue_client.exceptions.AlreadyExistsException:
            # Table already exists, update it instead
            response = self.glue_client.update_table(
                DatabaseName=self.database_name, TableInput=table_input
            )
        return response
