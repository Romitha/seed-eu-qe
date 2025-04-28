from connection.abstract_factory.abstract_factory import AbstractFactory
from connection.abstract_factory.alchemy_db_factory.alchemy_db_connection import \
    SQLAlchemyConnection


class SQLAlchemyFactory(AbstractFactory):
    def create_connection(self, db_name: str, config: dict[str, str]) -> SQLAlchemyConnection:
        """
        Create a SQLAlchemy connection using the provided database name and configuration

        Args:
            db_name (str): The name of the database to connect to
            config (dict[str, str]): A dictionary containing configuration settings (e.g. username, password)

        Returns:
            SQLAlchemyConnection: A connection object to interact with the database
        """
        return SQLAlchemyConnection(db_name, config)
