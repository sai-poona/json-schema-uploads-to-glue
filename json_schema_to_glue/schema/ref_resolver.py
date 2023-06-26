import json
import os
import requests

from urllib.parse import urlparse


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
