import os
import re
import yaml


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
            pattern = re.compile(r"\[(.*?)\]")
            replaced_value = pattern.sub(lambda x: placeholders.get(x.group(), x.group()), value)
            replaced_dict[key] = replaced_value
        else:
            # Keep non-string, non-dict values as is
            replaced_dict[key] = value
    return replaced_dict


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
