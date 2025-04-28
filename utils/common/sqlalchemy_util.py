import re

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import SQLAlchemyError


def create_sqlalchemy_url(db_name: str, db_config: dict[str, str]) -> URL:
    """
    Creates a SQLAlchemy URL for the specified database type using the provided configuration

    Args:
        db_name (str): The name of the database type ('aws_redshift', 'aws_aurora', or 'snowflake')
        db_config (dict[str, str]): A dictionary containing database connection details

    Returns:
        URL: A SQLAlchemy URL instance for database connection

    Raises:
        ValueError: If the database type is unsupported

    Examples:
        >>> sample_db_config = {
        ...     "redshift_host": "example.com",
        ...     "redshift_database": "mydb",
        ...     "redshift_username": "user",
        ...     "redshift_password": "pass"
        ... }
        >>> create_sqlalchemy_url("aws_redshift", sample_db_config)
        redshift+redshift_connector://user:***@example.com/mydb
    """
    if db_name == 'aws_redshift':
        return URL.create(
            drivername='redshift+redshift_connector',
            host=db_config['redshift_host'],
            port=db_config.get('redshift_port', 5439),
            database=db_config['redshift_database'],
            username=db_config['redshift_username'],
            password=db_config['redshift_password'],
        )
    elif db_name == 'aws_aurora':
        return URL.create(
            drivername='postgresql+psycopg2',
            host=db_config['aurora_host'],
            port=db_config.get('aurora_port', 5432),
            database=db_config['aurora_database'],
            username=db_config['aurora_username'],
            password=db_config['aurora_password'],
        )
    elif db_name == 'snowflake':
        return URL.create(
            drivername='snowflake',
            host=db_config['snowflake_host'],
            port=db_config.get('snowflake_port', 443),
            database=db_config['snowflake_database'],
            username=db_config['snowflake_username'],
            password=db_config['snowflake_password'],
            query={'account': db_config['snowflake_account']}
        )
    else:
        raise ValueError(f"Unsupported database name: {db_name}")


def read_sql_query(db_engine: Engine, query: str) -> list[dict[str, any]]:
    """
    Executes a SQL query using the provided SQLAlchemy engine and returns the result as a list of dictionaries

    Args:
        db_engine (Engine): SQLAlchemy engine to use for the database connection
        query (str): SQL query to execute

    Returns:
        list[dict[str, any]]: List of dictionaries containing the query results

    Raises:
        RuntimeError: If the query execution fails

    Examples:
        >>> engine = create_engine("sqlite:///:memory:")
        >>> read_sql_query(engine, "SELECT 1 AS value")
        [{'value': 1}]
    """
    try:
        query = process_query_columns(query)
        with db_engine.connect() as connection:
            result = connection.execute(query)
            rows = result.fetchall()
            columns = result.keys()
            return [dict(zip(columns, row)) for row in rows]
    except SQLAlchemyError as e:
        raise RuntimeError(f"Failed to execute query: {query}") from e


def run_sql_query(db_engine: Engine, query: str) -> None:
    """
    Executes a SQL query using the provided SQLAlchemy engine

    Args:
        db_engine (Engine): SQLAlchemy engine to use for the database connection
        query (str): SQL query to execute

    Raises:
        RuntimeError: If the query execution fails

    Examples:
        >>> engine = create_engine("sqlite:///:memory:")
        >>> run_sql_query(engine, "CREATE TABLE test (id INTEGER)")
    """
    try:
        query = process_query_columns(query)
        with db_engine.connect() as connection:
            connection.execution_options(isolation_level="AUTOCOMMIT").execute(query)
    except SQLAlchemyError as e:
        raise RuntimeError(f"Failed to execute query: {query}") from e


def process_query_columns(query: str) -> str:
    """
    Processes a SQL query by wrapping identifiers that are preceded by a space and
    start with a digit followed by an underscore, with double quotes.

    For instance:
    - 'SELECT blah blah from customer1 varchar(10)' remains unchanged.
    - 'SELECT blah blah from 2_colname numeric(31, 8)' becomes:
      'SELECT blah blah from "2_colname" numeric(31, 8)'
    """
    # This regex looks for a whitespace character
    # immediately followed by one or more digits, an underscore, and one or more word characters
    pattern = r'(?<=\s)(\d+_\w+)'

    # The replacement wraps the captured group in double quotes.
    processed_query = re.sub(pattern, r'"\1"', query)
    return processed_query
