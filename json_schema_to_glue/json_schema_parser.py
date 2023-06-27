import boto3
import json
import os
import re
import requests
import shutil
import yaml

from urllib.parse import urlparse


class JSONRefResolver:
    """
    A class for resolving $refs in a JSON schema.
    """

    def __init__(self, schema_file: str):
        """
        Initialize the JSONRefResolver.

        Args:
            schema_file (str): The main JSON schema file
        """
        self.schema_file = schema_file
        # Load the JSON schema
        self.schema = json.load(open(self.schema_file))
        self.resolved_refs = {}

    def resolved_schema(self) -> dict:
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
            raise Exception(
                f"Error resolving local file reference: Schema file not found - {ref_value}"
            )

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
            raise Exception(
                f"Error resolving external URL reference: {response.status_code} - {ref_value}"
            )


class YamlConfigParser:
    def __init__(self, config_file: str, place_holders: dict = None):
        self.config_file = config_file
        self._config: dict = None
        self._place_holders = place_holders

    @property
    def config(self) -> dict:
        if self._config is None:
            with open(self.config_file) as f:
                # Load the YAML content
                _config = yaml.safe_load(f)
                if self._place_holders is not None:
                    self._config = self.replace_placeholders(
                        _config, self._place_holders
                    )
        return self._config

    @classmethod
    def replace_placeholders(cls, config: dict, place_holders: dict):
        replaced_dict = {}
        for key, value in config.items():
            if isinstance(value, dict):
                # Recursively process nested dictionaries
                replaced_dict[key] = cls.replace_placeholders(value, place_holders)
            elif isinstance(value, str):
                # Replace placeholders in string values using regex
                pattern = re.compile(r"\[(.*?)\]")
                replaced_value = pattern.sub(
                    lambda x: place_holders.get(x.group(), x.group()), value
                )
                replaced_dict[key] = replaced_value
            else:
                # Keep non-string, non-dict values as is
                replaced_dict[key] = value
        return replaced_dict


class ZipSchema:
    def __init__(self, zip_schema: str, extract_base_path: str, place_holders: dict = None):
        assert zip_schema.endswith(".zip")
        self.zip_schema = zip_schema
        self.extract_base_path = extract_base_path
        self._place_holders = place_holders

        self._config = None
        self._main_config_file = None
        self._main_schema_file = None
        self._unzipped: bool = False

    def _unzip(self):
        if self._unzipped:
            return

        # Create the temporary directory if it doesn't exist
        if not os.path.exists(self.extract_base_path):
            os.makedirs(self.extract_base_path)

        # Generate a unique file name for the temporary file
        file_name = os.path.join(
            self.extract_base_path, os.path.basename(self.zip_schema)
        )

        if "s3://" in self.zip_schema or "s3a://" in self.zip_schema:
            _, _, bucket_name, prefix = self.zip_schema.split("/", 3)
            # Initialize the S3 client
            s3 = boto3.client("s3")
            # Download the file from S3 to the temporary path
            s3.download_file(bucket_name, prefix, file_name)
        else:
            file_name = self.zip_schema

        # Extract the contents of the file to the temporary directory
        shutil.unpack_archive(file_name, self.extract_base_path)
        self._unzipped = True

    @property
    def config(self) -> dict:
        if self._config is not None:
            return self._config

        self._unzip()

        for root, dirs, files in os.walk(self.extract_base_path):
            for file in files:
                if file.endswith(".config.yml") or file.endswith(".config.yaml"):
                    self._config = YamlConfigParser(
                        os.path.join(root, file), self._place_holders
                    ).config
                    return self._config

        raise RuntimeError(f"No .config.y(a)ml file found at {self.extract_base_path}")

    def get_main_schema_file(self):
        return self.config["main_schema_file"]

    def parse_schema(self):
        current_working_directory = os.getcwd()
        os.chdir(self.extract_base_path)

        schema = JSONRefResolver(self.get_main_schema_file()).resolved_schema()

        os.chdir(current_working_directory)
        return schema
