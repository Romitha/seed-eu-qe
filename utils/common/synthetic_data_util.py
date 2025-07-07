import re
import secrets
from datetime import datetime
from decimal import Decimal

import sqlalchemy
from faker import Faker
from sqlalchemy import MetaData, Table, delete, insert

from utils.framework.custom_logger_util import get_logger

LOGGER = get_logger()

fake = Faker()


def generate_table_schema_from_columns(expected_columns: list) -> dict:
    """
    Generate a schema dictionary mapping column names to their Python-like data types

    Args:
        expected_columns (list): A list of strings where each string represents a column definition
                                 in the format "column_name DATA_TYPE"

    Returns:
        dict: A dictionary where keys are column names and values are lists representing Python-like data types
              - int: ["int", 10]
              - str: ["str", varchar_limit]
              - float: ["float", precision - scale + 0.1]
              - decimal: ["decimal", (precision, scale)]
    """
    table_schema = {}

    for col_def in expected_columns:
        parts = col_def.split(" ", 1)
        if len(parts) < 2:
            continue

        col_name, col_type = parts[0], parts[1]

        if "VARCHAR" in col_type.upper():
            match = re.search(r"VARCHAR\((\d+)\)", col_type, re.IGNORECASE)
            length = int(match.group(1)) if match else 255
            table_schema[col_name] = ["str", length]

        elif "NUMERIC" in col_type.upper():
            match = re.search(r"NUMERIC\((\d+),\s*(\d+)\)", col_type, re.IGNORECASE)
            if match:
                precision = int(match.group(1))
                scale = int(match.group(2))
                table_schema[col_name] = ["decimal", (precision, scale)]
            else:
                table_schema[col_name] = ["decimal", (10, 2)]  # Default precision and scale

        elif "INT" in col_type.upper():
            table_schema[col_name] = ["int", 10]

        elif "DATE" in col_type.upper():
            table_schema[col_name] = ["date", None]

        elif "TIMESTAMP" in col_type.upper():
            table_schema[col_name] = ["timestamp", None]

        elif "BOOLEAN" in col_type.upper():
            table_schema[col_name] = ["boolean", None]

        elif "REAL" in col_type.upper() or "FLOAT" in col_type.upper():
            table_schema[col_name] = ["float", 10.1]

        else:
            # Default to string for unknown types
            table_schema[col_name] = ["str", 255]

    return table_schema


def ensure_src_sys_cd_column(table_schema: dict) -> dict:
    """
    Ensures that 'src_sys_cd', 'insrt_dttm', and 'updt_dttm' columns exist in the schema.

    Args:
        table_schema (dict): A dictionary representing the schema

    Returns:
        dict: The updated schema with required metadata columns
    """
    for col in ["src_sys_cd", "insrt_dttm", "updt_dttm"]:
        if col not in table_schema:
            table_schema[col] = ["str", 10] if col == "src_sys_cd" else ["timestamp", None]
    return table_schema


def generate_synthetic_data(table_schema: dict, num_rows: int) -> list:
    """
    Generate synthetic data based on a provided table schema

    Args:
        table_schema (dict): The schema dictionary mapping column names to data types.
        num_rows (int): The number of rows to generate

    Returns:
        list: A list of dictionaries where each dictionary represents a row of synthetic data.
    """
    table_schema = ensure_src_sys_cd_column(table_schema)

    def generate_row():
        row = {}
        for col, (dtype, limit) in table_schema.items():
            if col == "src_sys_cd":
                row[col] = "XYZ"
            elif col == "co_nbr":
                row[col] = str(secrets.choice(range(1, 201)))  # Random int as str (1-200)
            elif re.search(r"_dt$", col):
                row[col] = ""
            elif dtype == "int":
                row[col] = secrets.choice(range(1, 11))
            elif dtype == "str":
                # Generate string within the specified length limit
                max_length = min(limit, 50) if limit else 50  # Cap at 50 chars for readability
                generated_word = fake.word()[:max_length - 1]  # Leave space for safety
                row[col] = generated_word
            elif dtype == "float":
                row[col] = round(secrets.randbelow(10) + secrets.randbits(10) / (2 ** 10), 2)
            elif dtype == "decimal":
                precision, scale = limit
                # Generate a decimal value that fits within the precision and scale constraints
                max_integer_digits = precision - scale

                # Ensure we don't exceed the precision
                if max_integer_digits <= 0:
                    # If no room for integer part, generate a small fractional number
                    integer_part = 0
                else:
                    max_integer_value = min(10 ** max_integer_digits - 1, 999999)  # Cap for safety
                    integer_part = secrets.randbelow(max_integer_value + 1)

                if scale > 0:
                    fractional_part = secrets.randbelow(10 ** scale)
                    # Format the decimal string properly
                    decimal_str = f"{integer_part}.{fractional_part:0{scale}d}"
                else:
                    decimal_str = str(integer_part)

                decimal_value = Decimal(decimal_str)

                # Ensure the value fits within the precision constraint
                # Convert to string and check total length (excluding decimal point)
                decimal_str_check = str(decimal_value).replace('.', '')
                if len(decimal_str_check) > precision:
                    # If too long, generate a smaller value
                    safe_integer_digits = max(1, precision - scale)
                    safe_integer_value = min(10 ** safe_integer_digits - 1, 999)
                    if scale > 0:
                        safe_fractional = secrets.randbelow(10 ** scale)
                        decimal_value = Decimal(f"{safe_integer_value}.{safe_fractional:0{scale}d}")
                    else:
                        decimal_value = Decimal(str(safe_integer_value))

                row[col] = decimal_value
            elif dtype == "boolean":
                row[col] = secrets.choice([True, False])
            elif dtype == "date":
                row[col] = fake.date_this_century()
            elif dtype == "timestamp":
                row[col] = datetime.now().replace(microsecond=0)
            else:
                row[col] = None
        return row

    return [generate_row() for _ in range(num_rows)]


def delete_synthetic_data(client, schema_name: str, table_name: str) -> None:
    """
    Deletes rows from a table where 'src_sys_cd' equals 'XYZ'

    Args:
        client (sqlalchemy.engine.base.Engine): A SQLAlchemy Engine instance connected to the database
        schema_name (str): The name of the schema
        table_name (str): The name of the table

    Raises:
        sqlalchemy.exc.SQLAlchemyError: If the delete operation fails
    """
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=client, schema=schema_name)

    stmt = delete(table).where(table.c.src_sys_cd == "XYZ")

    with client.connect() as conn:
        result = conn.execute(stmt)

    if result.rowcount > 0:
        LOGGER.info(f"Deleted {result.rowcount} rows from {schema_name}.{table_name} where src_sys_cd = 'XYZ'")


def insert_synthetic_data(client, schema_name: str, table_name: str, synthetic_data: list) -> None:
    """
    Inserts synthetic data into a table

    Args:
        client (sqlalchemy.engine.base.Engine): A SQLAlchemy Engine instance connected to the database
        schema_name (str): The name of the schema
        table_name (str): The name of the table
        synthetic_data (list): A list of dictionaries representing synthetic rows to insert

    Raises:
        sqlalchemy.exc.SQLAlchemyError: If the insert operation fails
    """
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=client, schema=schema_name)

    stmt = insert(table).values(synthetic_data)

    with client.connect() as conn:
        conn.execute(stmt)
        LOGGER.info(f"Inserted {len(synthetic_data)} rows into {schema_name}.{table_name}")