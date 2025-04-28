from io import BytesIO

import yaml
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

from utils.common.aws_util import get_s3_client
from utils.framework.custom_logger_util import get_logger

LOGGER = get_logger()


def read_table_yaml_from_s3(s3_path_prefix: str, table_name: str, bucket_name: str) -> dict:
    """
    Reads a YAML file from an S3 bucket and returns its contents as a dictionary

    This function uses the S3 client provided by the AWS utility module to interact
    with AWS S3, ensuring proper handling of credentials

    Args:
        s3_path_prefix (str): The S3 path prefix leading to the file
        table_name (str): The name of the table for which the YAML configuration is being retrieved
        bucket_name (str): The name of the S3 bucket (default is 'sysco-seed-eu-np-code-worm-audit')

    Returns:
        dict: A dictionary representing the YAML configuration

    Raises:
        FileNotFoundError: If the YAML file does not exist in the specified S3 path
        NoCredentialsError: If AWS credentials are not available
        PartialCredentialsError: If incomplete AWS credentials are provided
        RuntimeError: For any other error that occurs during the operation
    """

    # Retrieve the S3 client from the AWS utility module
    s3_client = get_s3_client()

    # Construct the full S3 path for the YAML file
    s3_file_key = f"{s3_path_prefix}{table_name}.yaml"

    try:
        # Get the object from S3 using the client
        s3_response = s3_client.get_object(Bucket=bucket_name, Key=s3_file_key)

        # Read the contents of the file
        file_content = s3_response['Body'].read()

        # Parse the YAML content
        yaml_content = yaml.safe_load(BytesIO(file_content))

        return yaml_content

    except s3_client.exceptions.NoSuchKey:
        raise FileNotFoundError(f"YAML configuration file not in S3: {s3_file_key}")
    except NoCredentialsError:
        raise NoCredentialsError
    except PartialCredentialsError:
        raise PartialCredentialsError
    except Exception as e:
        raise RuntimeError(f"An error occurred while reading the YAML file from S3: {e}")


def read_default_scope_yml_from_s3(s3_path_prefix: str, scope: str, bucket_name: str) -> dict:
    """
    Reads a YML file from an S3 bucket and returns its contents as a dictionary.

    This function uses the S3 client provided by the AWS utility module to interact
    with AWS S3, ensuring proper handling of credentials.

    Args:
        s3_path_prefix (str): The S3 path prefix leading to the file
        scope (str): The scope for which the YML configuration is being retrieved
        bucket_name (str): The name of the S3 bucket (default is 'sysco-seed-eu-np-code-worm-audit')

    Returns:
        dict: A dictionary representing the YML configuration

    Raises:
        FileNotFoundError: If the YML file does not exist in the specified S3 path
        NoCredentialsError: If AWS credentials are not available
        PartialCredentialsError: If incomplete AWS credentials are provided
        RuntimeError: For any other error that occurs during the operation
    """

    # Retrieve the S3 client from the AWS utility module
    s3_client = get_s3_client()

    # Construct the full S3 path for the YML file
    s3_file_key = f"{s3_path_prefix}{scope}.yml"

    try:
        # Get the object from S3 using the client
        s3_response = s3_client.get_object(Bucket=bucket_name, Key=s3_file_key)

        # Read the contents of the file
        file_content = s3_response['Body'].read()

        # Parse the YML content
        yml_content = yaml.safe_load(BytesIO(file_content))

        return yml_content

    except s3_client.exceptions.NoSuchKey:
        raise FileNotFoundError(f"YML configuration file not in S3: {s3_file_key}")
    except NoCredentialsError:
        raise NoCredentialsError
    except PartialCredentialsError:
        raise PartialCredentialsError
    except Exception as e:
        raise RuntimeError(f"An error occurred while reading the YML file from S3: {e}")
