from sqlalchemy.exc import SQLAlchemyError

from utils.common.sqlalchemy_util import run_sql_query
from utils.framework.custom_logger_util import get_logger

LOGGER = get_logger()


def create_spectrum_schema_in_wh_if_not_exists(wh_client, spectrum_schema, external_db_name):
    try:
        # Check if schema exists
        query = f"SELECT schemaname FROM svv_external_schemas WHERE schemaname = '{spectrum_schema}' " \
                f"AND databasename = '{external_db_name}';"
        result = wh_client.execute(query).fetchone()

        if not result:
            # Create external schema
            create_schema_query = f"""
            CREATE EXTERNAL SCHEMA IF NOT EXISTS {spectrum_schema}
            FROM DATA CATALOG
            DATABASE '{external_db_name}'
            IAM_ROLE default;
            """
            wh_client.execute(create_schema_query)
            LOGGER.info(f"External schema {spectrum_schema} created successfully.")
        else:
            LOGGER.info(f"External schema {spectrum_schema} already exists.")
    except SQLAlchemyError as e:
        LOGGER.error(f"Error creating/checking schema: {str(e)}")


def create_external_db_if_not_exists(glue_client, ext_db_name):
    try:
        # Check if the database exists
        existing_dbs = glue_client.get_databases()
        db_names = {db["Name"] for db in existing_dbs.get("DatabaseList", [])}

        if ext_db_name in db_names:
            LOGGER.info(f"Glue database {ext_db_name} already exists")
            return

        # Create the database
        glue_client.create_database(DatabaseInput={"Name": ext_db_name})
        LOGGER.info(f"Glue database {ext_db_name} created successfully")

    except Exception as e:
        LOGGER.error(f"Error creating Glue database {ext_db_name}: {str(e)}")
        raise


def check_table_exists_in_external_db(glue_client, external_db_name, table_name):
    try:
        # Retrieve the list of tables from the Glue database
        response = glue_client.get_tables(DatabaseName=external_db_name)

        # Check if the table name exists in the list of tables
        tables = [table['Name'] for table in response['TableList']]
        if table_name in tables:
            LOGGER.info(f"Table '{table_name}' exists in the database '{external_db_name}'")
            return True
        else:
            LOGGER.info(f"Table '{table_name}' does not exist in the database '{external_db_name}'")
            return False

    except glue_client.exceptions.DatabaseNotFoundException as e:
        LOGGER.info(f"Database '{external_db_name}' not found. Error: {str(e)}")
        return False

    except Exception as e:
        LOGGER.info(f"An error occurred while checking if table exists: {str(e)}")
        return False


def create_external_table(
        wh_client, table_name, s3_uri, stored_as, skip_head_line_count, row_format_serde,
        sep_char, quote_char, escape_char, test_columns):
    try:
        # Log the test_columns to verify its structure before generating the column definitions
        LOGGER.info(f"test_columns: {test_columns}")

        # Ensure test_columns is a list of strings, each representing a column definition
        if not isinstance(test_columns, list):
            raise ValueError(f"test_columns should be a list, but got {type(test_columns)}")

        # Split each string into column name and type (assuming the format is 'name type')
        column_definitions = ", ".join(test_columns)

        if not column_definitions:
            raise ValueError("Column definitions cannot be empty")

        # Construct the SQL query
        create_table_query = f"""
            CREATE EXTERNAL TABLE {table_name} (
                {column_definitions}
            )
            ROW FORMAT SERDE '{row_format_serde}'
            WITH SERDEPROPERTIES (
                'field.delim' = '{sep_char}',   
                'escape.delim' = '{escape_char}'
            )
            STORED AS {stored_as}
            LOCATION '{s3_uri}'
            TABLE PROPERTIES ('skip.header.line.count'='{skip_head_line_count}');
            """

        # Log the query for debugging
        LOGGER.debug(f"Generated CREATE TABLE query: {create_table_query}")

        # Execute the query
        run_sql_query(wh_client, create_table_query)
        LOGGER.info(f"External table {table_name} created successfully.")
    except SQLAlchemyError as e:
        LOGGER.error(f"Error creating external table: {str(e)}")
    except ValueError as e:
        LOGGER.error(f"Error in table definition: {str(e)}")


def delete_spectrum_table_in_catalog_if_exists(ext_db_client, external_db_name, table_name):
    try:
        ext_db_client.delete_table(DatabaseName=external_db_name, Name=table_name)
        LOGGER.info(f"Existing table {table_name} deleted from Glue database {external_db_name}")
    except ext_db_client.exceptions.EntityNotFoundException:
        LOGGER.warning(f"Table {table_name} not in Glue database {external_db_name}")
    except Exception as e:
        LOGGER.error(f"Error deleting table {table_name}: {str(e)}")
        raise
