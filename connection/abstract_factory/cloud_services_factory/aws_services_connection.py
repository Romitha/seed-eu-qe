import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

from connection.abstract_factory.cloud_services_factory.cloud_services_connection import \
    CloudServiceConnection


class AwsServiceConnection(CloudServiceConnection):
    def __init__(self, client_type) -> None:
        """
        Initialize the AWS service connection by getting the boto3 client
        """
        self.aws_session = boto3.Session()
        self.client_type = client_type

    def connect(self) -> object:
        """
        Establish a connection to AWS services

        Returns:
            object: The boto3 client object
        """
        try:
            return boto3.client(self.client_type)
        except NoCredentialsError:
            raise NoCredentialsError()
        except PartialCredentialsError:
            raise PartialCredentialsError(provider="aws", cred_var="access_key")
