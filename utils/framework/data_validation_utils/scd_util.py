from utils.common.file_util import load_multiline_sql_file_in_path
from utils.common.github_util import fetch_file_content, parse_github_url
from utils.common.sqlalchemy_util import read_sql_query, run_sql_query
from utils.framework.custom_logger_util import get_logger
from utils.framework.data_quality_utils.completeness_util import \
    check_unexpected_nulls

LOGGER = get_logger()


def get_scd_default_cols(scd_settings):
    """
    Generate a list of columns to treat as scd defaults by combining
    ind_columns, dt_columns (with dt_prefix), and hash_columns

    Args:
        scd_settings (dict): A dictionary containing SCD settings

    Returns:
        list: A list of default scd columns (scd_default_cols)
    """
    # Extract relevant data
    dt_prefix = scd_settings.get('dt_prefix', '')
    dt_columns = scd_settings.get('dt_columns', [])
    ind_columns = scd_settings.get('ind_columns', [])
    hash_columns = scd_settings.get('hash_columns', [])

    # Add prefix to all dt_columns
    prefixed_dt_columns = [f"{dt_prefix}{col}" for col in dt_columns]

    # Merge lists into scd_default_cols
    scd_default_cols = ind_columns + prefixed_dt_columns + hash_columns

    return scd_default_cols


def run_lndp_edwp_script_for_scd_tables(db_client, scd_settings, pat, run_mode):
    for sql_url in scd_settings.get('lndp_to_edwp_sqls', []):
        repo_owner, repo_name, branch, file_path = parse_github_url(sql_url)

        # Fetch SQL content
        sql_content = fetch_file_content(repo_owner, repo_name, file_path, branch, pat, run_mode)

        queries = load_multiline_sql_file_in_path(sql_content)
        for query in queries:
            run_sql_query(db_client, query)


def check_scd_nulls(db_client, edwp_schema_name, edwp_table_name, scd_columns):
    try:
        if not scd_columns:
            LOGGER.warning(f"No SCD null checks in {edwp_schema_name}.{edwp_table_name}. Check default or table Yaml")
            return

        LOGGER.info(f"Column currently under null check: {scd_columns}")
        check_unexpected_nulls(db_client, edwp_schema_name, edwp_table_name, scd_columns)

        LOGGER.info(f"SCD Null checks completed for {edwp_table_name} on columns: {scd_columns}")

    except Exception as e:
        LOGGER.error(f"Error during SCD columns null checks: {str(e)}")
        raise


def get_columns(scd_settings, column_type):
    """Retrieve major or minor columns from settings."""
    return scd_settings.get(f"{column_type}_columns", [])


def fetch_records(db_client, schema_name, table_name, columns):
    """Fetch records where src_sys_cd = 'XYZ'."""
    if not columns:
        return []

    query = f"""
        SELECT {', '.join(columns)}
        FROM {schema_name}.{table_name}
        WHERE src_sys_cd = 'XYZ';
    """
    return read_sql_query(db_client, query)


def fetch_column_data_types(db_client, schema_name, table_name, columns):
    """Fetch data types and character limits for specified columns."""
    if not columns:
        return []

    dtype_query = f"""
        SELECT column_name, data_type, character_maximum_length
        FROM information_schema.columns
        WHERE table_schema = '{schema_name}' 
        AND table_name = '{table_name}'
        AND column_name IN ({', '.join([f"'{col}'" for col in columns])});
    """
    return read_sql_query(db_client, dtype_query)


def build_update_clause(column, col_type, max_length):
    """Generate an appropriate update clause based on column type."""
    if "char" in col_type.lower():
        return f"""{column} = 
            CASE 
                WHEN {column} IS NULL OR {column} = '' THEN {column}
                WHEN RIGHT({column}, 1) ~ '[a-y]' THEN LEFT({column}, LENGTH({column}) - 1) || chr(ascii(RIGHT({column}, 1)) + 1)
                WHEN RIGHT({column}, 1) = 'z' THEN LEFT({column}, LENGTH({column}) - 1) || 'A'
                WHEN RIGHT({column}, 1) ~ '[A-Y]' THEN LEFT({column}, LENGTH({column}) - 1) || chr(ascii(RIGHT({column}, 1)) + 1)
                WHEN RIGHT({column}, 1) = 'Z' THEN LEFT({column}, LENGTH({column}) - 1) || '0'
                WHEN RIGHT({column}, 1) ~ '[0-8]' THEN LEFT({column}, LENGTH({column}) - 1) || chr(ascii(RIGHT({column}, 1)) + 1)
                WHEN RIGHT({column}, 1) = '9' THEN LEFT({column}, LENGTH({column}) - 1) || 'a'
                ELSE {column}
            END"""
    elif "boolean" in col_type.lower():
        return f"{column} = NOT {column}"
    elif "int" in col_type.lower():
        return f"{column} = COALESCE({column}, 0) + 1"
    elif "date" in col_type.lower() or "time" in col_type.lower():
        return f"{column} = {column} + INTERVAL '1 day'"
    else:
        LOGGER.warning(f"Skipping column {column} with unknown type {col_type}")
        return None


def generate_update_statements(columns, data_types):
    """Generate update statements for each column in the provided list."""
    set_clauses = [
        build_update_clause(col, col_info["data_type"], col_info.get("character_maximum_length"))
        for col in columns
        if (col_info := next((d for d in data_types if d["column_name"] == col), None))
    ]
    return [stmt for stmt in set_clauses if stmt]


def execute_update(db_client, schema_name, table_name, set_clauses):
    """Execute the update query on the database."""
    if not set_clauses:
        LOGGER.warning("No valid columns to update")
        return

    update_query = f"""
        UPDATE {schema_name}.{table_name}
        SET {', '.join(set_clauses)}
        WHERE src_sys_cd = 'XYZ';
    """
    run_sql_query(db_client, update_query)
    LOGGER.info(f"Updated records in {schema_name}.{table_name} where src_sys_cd = 'XYZ'")


def update_scd_maj_min_columns(db_client, schema_name, table_name, scd_settings, column_type):
    """
    Update major or minor columns in the database.
    """
    try:
        columns = get_columns(scd_settings, column_type)
        if not columns:
            LOGGER.warning(f"No {column_type} columns defined for {schema_name}.{table_name}")
            return

        records = fetch_records(db_client, schema_name, table_name, columns)
        if not records:
            LOGGER.info(f"No records found in {schema_name}.{table_name} where src_sys_cd = 'XYZ'")
            return

        data_types = fetch_column_data_types(db_client, schema_name, table_name, columns)
        if not data_types:
            LOGGER.warning(f"Could not fetch data types for {column_type} columns in {schema_name}.{table_name}")
            return

        set_clauses = generate_update_statements(columns, data_types)
        execute_update(db_client, schema_name, table_name, set_clauses)

    except Exception as e:
        LOGGER.error(f"Error in update_columns for {schema_name}.{table_name} ({column_type} columns): {e}",
                     exc_info=True)
        raise


def check_scd_values_for_major_columns(db_client, schema_name, table_name, scd_columns):
    try:
        # Step 1: Identify required SCD columns
        eff_dt_column = next((col for col in scd_columns if "eff_dt" in col.lower()), None)
        end_dt_column = next((col for col in scd_columns if "end_dt" in col.lower()), None)
        major_hash_column = next((col for col in scd_columns if "maj_atr_md5_hsh_cd" in col.lower()), None)
        minor_hash_column = next((col for col in scd_columns if "mnr_atr_md5_hsh_cd" in col.lower()), None)

        if not eff_dt_column or not end_dt_column or not major_hash_column or not minor_hash_column:
            LOGGER.error("One or more required SCD columns (eff_dt, end_dt, major_hash, minor_hash) not found")
            return

        # Step 2: SQL to validate the oldest record (MIN insrt_dttm)
        oldest_record_query = f"""
            SELECT *
            FROM {schema_name}.{table_name}
            WHERE src_sys_cd = 'XYZ' 
            AND insrt_dttm = (SELECT MIN(insrt_dttm) FROM {schema_name}.{table_name} WHERE src_sys_cd = 'XYZ')
            AND {eff_dt_column} <> '1900-01-01';
        """

        invalid_oldest_records = read_sql_query(db_client, oldest_record_query)

        # Step 3: SQL to validate the latest record (MAX insrt_dttm)
        latest_record_query = f"""
            SELECT *
            FROM {schema_name}.{table_name}
            WHERE src_sys_cd = 'XYZ' 
            AND insrt_dttm = (SELECT MAX(insrt_dttm) FROM {schema_name}.{table_name} WHERE src_sys_cd = 'XYZ')
            AND {end_dt_column} <> '9999-12-31';
        """

        invalid_latest_records = read_sql_query(db_client, latest_record_query)

        # Step 4: SQL to validate major hash change using WITH (CTE)
        major_hash_validation_query = f"""
            WITH old_record AS (
                SELECT *
                FROM {schema_name}.{table_name}
                WHERE src_sys_cd = 'XYZ'
                AND insrt_dttm = (SELECT MIN(insrt_dttm) FROM {schema_name}.{table_name} WHERE src_sys_cd = 'XYZ')
            ),
            new_record AS (
                SELECT *
                FROM {schema_name}.{table_name}
                WHERE src_sys_cd = 'XYZ'
                AND insrt_dttm = (SELECT MAX(insrt_dttm) FROM {schema_name}.{table_name} WHERE src_sys_cd = 'XYZ')
            )
            SELECT *
            FROM old_record, new_record
            WHERE old_record.{major_hash_column} = new_record.{major_hash_column};  
        """

        invalid_major_hash_records = read_sql_query(db_client, major_hash_validation_query)

        # Step 5: SQL to validate minor hash remains the same using WITH (CTE)
        minor_hash_validation_query = f"""
            WITH old_record AS (
                SELECT *
                FROM {schema_name}.{table_name}
                WHERE src_sys_cd = 'XYZ'
                AND insrt_dttm = (SELECT MIN(insrt_dttm) FROM {schema_name}.{table_name} WHERE src_sys_cd = 'XYZ')
            ),
            new_record AS (
                SELECT *
                FROM {schema_name}.{table_name}
                WHERE src_sys_cd = 'XYZ'
                AND insrt_dttm = (SELECT MAX(insrt_dttm) FROM {schema_name}.{table_name} WHERE src_sys_cd = 'XYZ')
            )
            SELECT *
            FROM old_record, new_record
            WHERE old_record.{minor_hash_column} <> new_record.{minor_hash_column}; 
        """

        invalid_minor_hash_records = read_sql_query(db_client, minor_hash_validation_query)

        # Step 6: Log validation results
        if invalid_oldest_records and len(invalid_oldest_records) > 0:
            LOGGER.warning(f"Oldest record in history does NOT have {eff_dt_column} = '1900-01-01'")
            LOGGER.debug(f"Invalid Oldest Record: {invalid_oldest_records[0]}")
        else:
            LOGGER.info(f"Oldest record correctly has {eff_dt_column} = '1900-01-01'")

        if invalid_latest_records and len(invalid_latest_records) > 0:
            LOGGER.warning(f"Latest record in history does NOT have {end_dt_column} = '9999-12-31'")
            LOGGER.debug(f"Invalid Latest Record: {invalid_latest_records[0]}")
        else:
            LOGGER.info(f"Latest record correctly has {end_dt_column} = '9999-12-31'")

        if invalid_major_hash_records and len(invalid_major_hash_records) > 0:
            LOGGER.warning(f"Major hash has NOT changed, which is incorrect")
            LOGGER.debug(f"Invalid Major Hash Record: {invalid_major_hash_records[0]}")
        else:
            LOGGER.info(f"Major hash changed correctly")

        if invalid_minor_hash_records and len(invalid_minor_hash_records) > 0:
            LOGGER.warning(f"Minor hash has changed, which is incorrect")
            LOGGER.debug(f"Invalid Minor Hash Record: {invalid_minor_hash_records[0]}")
        else:
            LOGGER.info(f"Minor hash remained the same as expected")

        # Step 7: Get major and minor hash values from latest record
        latest_hash_query = f"""
            SELECT {major_hash_column}, {minor_hash_column}
            FROM {schema_name}.{table_name}
            WHERE src_sys_cd = 'XYZ'
            AND insrt_dttm = (
                SELECT MAX(insrt_dttm)
                FROM {schema_name}.{table_name}
                WHERE src_sys_cd = 'XYZ'
            );
        """
        latest_hash_result = read_sql_query(db_client, latest_hash_query)

        if latest_hash_result and len(latest_hash_result) > 0:
            latest_major_hash = latest_hash_result[0][major_hash_column]
            minor_hash = latest_hash_result[0][minor_hash_column]

            return {
                "latest_maj_hash": latest_major_hash,
                "all_records_min_hash": minor_hash
            }
        else:
            LOGGER.warning("No major/minor hash found in latest record.")
            return {
                "latest_maj_hash": None,
                "all_records_min_hash": None
            }

    except Exception as e:
        LOGGER.error(f"Error in check_scd_values_for_updated_records for {schema_name}.{table_name}: {e}",
                     exc_info=True)
        raise


def check_scd_values_for_minor_columns(db_client, schema_name, table_name, scd_columns, hash_info):
    try:
        # Step 1: Identify required SCD columns
        eff_dt_column = next((col for col in scd_columns if "eff_dt" in col.lower()), None)
        end_dt_column = next((col for col in scd_columns if "end_dt" in col.lower()), None)
        major_hash_column = next((col for col in scd_columns if "maj_atr_md5_hsh_cd" in col.lower()), None)
        minor_hash_column = next((col for col in scd_columns if "mnr_atr_md5_hsh_cd" in col.lower()), None)

        if not eff_dt_column or not end_dt_column or not major_hash_column or not minor_hash_column:
            LOGGER.error("One or more required SCD columns (eff_dt, end_dt, major_hash, minor_hash) not found")
            return

        # Step 2: Validate oldest record's eff_dt
        oldest_record_query = f"""
            SELECT *
            FROM {schema_name}.{table_name}
            WHERE src_sys_cd = 'XYZ' 
            AND insrt_dttm = (SELECT MIN(insrt_dttm) FROM {schema_name}.{table_name} WHERE src_sys_cd = 'XYZ')
            AND {eff_dt_column} <> '1900-01-01';
        """
        invalid_oldest_records = read_sql_query(db_client, oldest_record_query)

        # Step 3: Validate latest record's end_dt
        latest_record_query = f"""
            SELECT *
            FROM {schema_name}.{table_name}
            WHERE src_sys_cd = 'XYZ' 
            AND insrt_dttm = (SELECT MAX(insrt_dttm) FROM {schema_name}.{table_name} WHERE src_sys_cd = 'XYZ')
            AND {end_dt_column} <> '9999-12-31';
        """
        invalid_latest_records = read_sql_query(db_client, latest_record_query)

        # Step 4: Validate major hash didn't change and minor hash DID change
        latest_hash_query = f"""
            SELECT {major_hash_column}, {minor_hash_column}
            FROM {schema_name}.{table_name}
            WHERE src_sys_cd = 'XYZ'
            AND insrt_dttm = (
                SELECT MAX(insrt_dttm)
                FROM {schema_name}.{table_name}
                WHERE src_sys_cd = 'XYZ'
            );
        """
        latest_hash_result = read_sql_query(db_client, latest_hash_query)

        major_hash_changed = False
        minor_hash_unchanged = False

        if latest_hash_result and len(latest_hash_result) > 0:
            latest_major_hash = latest_hash_result[0][major_hash_column]
            latest_minor_hash = latest_hash_result[0][minor_hash_column]

            # Compare with values from hash_info
            if latest_major_hash != hash_info["latest_maj_hash"]:
                major_hash_changed = True
            if latest_minor_hash == hash_info["all_records_min_hash"]:
                minor_hash_unchanged = True
        else:
            LOGGER.warning("No latest record found to validate hashes.")

        # Step 5: Log validation results
        if invalid_oldest_records:
            LOGGER.warning(f"Oldest record in history does NOT have {eff_dt_column} = '1900-01-01'")
            LOGGER.debug(f"Invalid Oldest Record: {invalid_oldest_records[0]}")
        else:
            LOGGER.info(f"Oldest record correctly has {eff_dt_column} = '1900-01-01'")

        if invalid_latest_records:
            LOGGER.warning(f"Latest record in history does NOT have {end_dt_column} = '9999-12-31'")
            LOGGER.debug(f"Invalid Latest Record: {invalid_latest_records[0]}")
        else:
            LOGGER.info(f"Latest record correctly has {end_dt_column} = '9999-12-31'")

        if major_hash_changed:
            LOGGER.warning("Major hash changed — which is INCORRECT for minor column update.")
        else:
            LOGGER.info("Major hash remained the same as expected.")

        if minor_hash_unchanged:
            LOGGER.warning("Minor hash did NOT change — which is INCORRECT for minor column update.")
        else:
            LOGGER.info("Minor hash changed as expected.")

    except Exception as e:
        LOGGER.error(f"Error in check_scd_values_for_minor_columns for {schema_name}.{table_name}: {e}", exc_info=True)
        raise



def validate_deleted_records_for_scd_table(db_client, schema_name, table_name, scd_settings):
    # Step 2: Fetch records where curr_rec_ind = 'Y' and src_del_ind = 'Y'
    try:
        query = f"""
            SELECT * FROM {schema_name}.{table_name}
            WHERE curr_rec_ind = 'Y' AND src_del_ind = 'Y';
        """
        deleted_records = read_sql_query(db_client, query)

        if deleted_records:
            LOGGER.info(f"Found {len(deleted_records)} records marked for deletion in {schema_name}.{table_name}")

            # Get the date prefix from scd_settings
            end_dt_column = scd_settings.get("dt_prefix", "") + "_end_dt"

            if end_dt_column not in deleted_records[0]:  # Check if column exists
                LOGGER.warning(f"Expected column '{end_dt_column}' not in {schema_name}.{table_name}")
                return

            # Step 3: Check if _end_dt is '9999-12-31'
            invalid_records = []
            for record in deleted_records:
                end_dt_value = record.get(end_dt_column)
                if str(end_dt_value) != "9999-12-31":
                    invalid_records.append(record)

            # Log the result
            if invalid_records:
                LOGGER.warning(f"Found {len(invalid_records)} records where {end_dt_column} is NOT '9999-12-31'")
                for rec in invalid_records:
                    LOGGER.debug(f"Invalid Record: {rec}")
            else:
                LOGGER.info(f"All deleted records have {end_dt_column} = '9999-12-31'")

        else:
            LOGGER.info(f"No records found for deletion in {schema_name}.{table_name}")

    except Exception as e:
        LOGGER.error(f"Error while fetching records for deletion: {str(e)}")
        raise
