from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from connection.abstract_factory.abstract_connection import AbstractConnection
from utils.common.sqlalchemy_util import create_sqlalchemy_url


class SQLAlchemyConnection(AbstractConnection):
    """
    SQLAlchemyConnection manages a database connection using SQLAlchemy
    It establishes and manages the connection, ensuring proper cleanup when disconnecting
    """

    def __init__(self, db_name: str, config: dict[str, str]) -> None:
        """
        Initialize the SQLAlchemyConnection with the database name and configuration

        Args:
            db_name (str): The name of the database to connect to
            config (dict[str, str]): A dictionary containing database connection parameters
            such as host, port, user, password, etc
        """
        # Generate the connection string based on the database name and configuration
        self.connection_string = create_sqlalchemy_url(db_name, config)

        # The SQLAlchemy engine will be initialized when the connection is established
        self.engine: Engine | None = None

    def connect(self) -> Engine:
        """
        Establish the connection to the database using SQLAlchemy

        Returns:
            Engine: A SQLAlchemy engine object that allows interaction with the database
        """
        # Create the engine with the connection string and allow SSL connections
        self.engine = create_engine(
            self.connection_string, connect_args={'sslmode': 'allow'}, isolation_level="AUTOCOMMIT")
        return self.engine  # Return the engine for querying the database

    def disconnect(self) -> None:
        """
        Disconnect from the database by disposing of the SQLAlchemy engine

        Notes:
            - This method ensures that the database connection is properly closed and resources are released
        """
        if self.engine:
            # Dispose of the engine, closing all connections
            self.engine.dispose()
