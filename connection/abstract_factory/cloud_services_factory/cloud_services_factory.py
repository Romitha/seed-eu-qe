from connection.abstract_factory.abstract_factory import AbstractFactory
from connection.abstract_factory.cloud_services_factory.aws_services_connection import \
    AwsServiceConnection
from connection.abstract_factory.cloud_services_factory.cloud_services_connection import \
    CloudServiceConnection


class CloudServiceFactory(AbstractFactory):
    def create_connection(self, service_type: str, config: dict[str, any]) -> CloudServiceConnection:

        service, client_type = service_type.split("_")

        if service == 'aws':
            connection = AwsServiceConnection(client_type)
            return connection

        # Future implementations for GCP Storage, Azure Blobstore can be added here
        if service_type.startswith('gcp'):
            pass
        if service_type.startswith('azure'):
            pass

        else:
            raise ValueError(f"Unsupported service type: {service_type}")
