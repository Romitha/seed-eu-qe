from typing import Any

import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError


def get_s3_client() -> Any:
    """
    Create and return a boto3 S3 client for interacting with S3 service

    Returns:
        boto3.S3.Client: S3 client object

    Raises:
        NoCredentialsError: If AWS credentials are not available
        PartialCredentialsError: If incomplete AWS credentials are provided

    Examples:
        >>> s3 = get_s3_client()
        >>> s3.list_buckets()
    """
    try:
        session = boto3.Session()
        return session.client("s3")
    except NoCredentialsError:
        raise NoCredentialsError()
    except PartialCredentialsError:
        raise PartialCredentialsError(provider="aws", cred_var="access_key")


def get_glue_client() -> Any:
    """
    Create and return a boto3 Glue client for interacting with Glue service

    Returns:
        boto3.Glue.Client: Glue client object

    Raises:
        NoCredentialsError: If AWS credentials are not available
        PartialCredentialsError: If incomplete AWS credentials are provided

    Examples:
        >>> glue = get_glue_client()
        >>> glue.get_jobs()
    """
    try:
        return boto3.client("glue")
    except NoCredentialsError:
        raise NoCredentialsError()
    except PartialCredentialsError:
        raise PartialCredentialsError(provider="aws", cred_var="access_key")


def get_secrets_manager_client() -> Any:
    """
    Create and return a boto3 Secrets Manager client for interacting with Secrets Manager service

    Returns:
        boto3.SecretsManager.Client: Secrets Manager client object

    Raises:
        NoCredentialsError: If AWS credentials are not available
        PartialCredentialsError: If incomplete AWS credentials are provided

    Examples:
        >>> secrets = get_secrets_manager_client()
        >>> secrets.list_secrets()
    """
    try:
        return boto3.client("secretsmanager")
    except NoCredentialsError:
        raise NoCredentialsError()
    except PartialCredentialsError:
        raise PartialCredentialsError(provider="aws", cred_var="access_key")


def get_parameter_store_client() -> Any:
    """
    Create and return a boto3 Secrets Manager client for interacting with Secrets Manager service

    Returns:
        boto3.SecretsManager.Client: Secrets Manager client object

    Raises:
        NoCredentialsError: If AWS credentials are not available
        PartialCredentialsError: If incomplete AWS credentials are provided

    Examples:
        >>> secrets = get_parameter_store_client()
        >>> secrets.list_secrets()
    """
    try:
        return boto3.client("ssm")
    except NoCredentialsError:
        raise NoCredentialsError()
    except PartialCredentialsError:
        raise PartialCredentialsError(provider="aws", cred_var="access_key")


def get_ses_client() -> Any:
    """
    Create and return a boto3 SES client for interacting with SES service

    Returns:
        boto3.SES.Client: SES client object

    Raises:
        NoCredentialsError: If AWS credentials are not available
        PartialCredentialsError: If incomplete AWS credentials are provided

    Examples:
        >>> ses = get_ses_client()
        >>> ses.list_identities()
    """
    try:
        return boto3.client("ses")
    except NoCredentialsError:
        raise NoCredentialsError()
    except PartialCredentialsError:
        raise PartialCredentialsError(provider="aws", cred_var="access_key")
