from utils.common.s3_util import (check_s3_files_exist,
                                  convert_s3_files_to_utf8,
                                  get_s3_uri_with_bucket_prefix,
                                  list_recoverable_s3_file_versions,
                                  recover_latest_s3_files)
from utils.common.spectrum_util import (
    check_table_exists_in_external_db, create_external_db_if_not_exists,
    create_external_table, create_spectrum_schema_in_wh_if_not_exists,
    delete_spectrum_table_in_catalog_if_exists)
from utils.framework.custom_logger_util import get_logger

LOGGER = get_logger()


def get_col_mapping_for_layer(reference_layer, test_layer_name, column_info):

    col_mapping_info = column_info.get("column_mapping", {})

    expected_cols = column_info.get("expected_columns", {})

    map_key = f"{reference_layer}_{test_layer_name}"
    mapping_list = col_mapping_info.get(map_key)

    if not mapping_list or not isinstance(mapping_list, list) or not mapping_list[0]:
        LOGGER.info(f"No valid mapping found for key: {map_key}, Proceeding without mapped columns")
        return None
    else:
        mapped_cols_dict = {}

        # Iterate through each dictionary in the list and update the merged_dict
        for item in mapping_list:
            mapped_cols_dict.update(item)

        LOGGER.info(f"Valid mapping found for key: {map_key}, Proceeding with mapped columns")

        ref_col_list = []
        trg_col_list = []

        for ref_col in expected_cols:
            ref_col_list.append(ref_col)

            # If there's a mapped value, use it. Else fallback to ref_col
            trg_col = mapped_cols_dict.get(ref_col, ref_col)
            trg_col_list.append(trg_col)

        # ref_col_list = layer_cosl_mapping(reference_col_mapping, expected_cols)
        # trg_col_list = layer_cosl_mapping(target_col_mapping, expected_cols)

        final_dict = {
            reference_layer: ref_col_list,
            test_layer_name: trg_col_list
        }

        return final_dict


def layer_cosl_mapping(layer_col_mapping, expected_cols):
    override_dict = {}
    for item in layer_col_mapping:
        col_name, dtype = item.split(maxsplit=1)
        override_dict[col_name] = dtype

    new_list = []
    for item in expected_cols:
        col_name, dtype = item.split(maxsplit=1)
        new_list.append(f"{col_name} {override_dict.get(col_name, dtype)}")

    return new_list


def log_layer_test_settings(layer_name, test_scope):
    verify = test_scope
    data_validation = bool(verify.get("data_validation"))
    data_quality = bool(verify.get("data_quality"))
    LOGGER.info(f"  - Data Validation: {f'Enabled for {layer_name}' if data_validation else 'Disabled'}")
    LOGGER.info(f"  - Data Quality: {f'Enabled for {layer_name}' if data_quality else 'Disabled'}")


def initiate_spectrum_creation(
        wh_client, src_client, ext_db_client, source_table_settings, table_name, test_columns, source_bucket):

    if not source_table_settings:
        raise ValueError(f"No source configuration found - cannot proceed to create spectrum table")

    # extract source table settings
    spectrum_schema = source_table_settings.get('spectrum_schema')
    external_db_name = source_table_settings.get('external_db')
    uri = source_table_settings.get('uri')
    source_encoding = source_table_settings.get('encoding')
    stored_as = source_table_settings.get('stored_as')
    source_file_type = source_table_settings.get('source_file_type')
    row_format_serde = source_table_settings.get('row_format_serde')
    sep_char = source_table_settings.get('sep_char')
    quote_char = source_table_settings.get('quote_char')
    escape_char = source_table_settings.get('escape_char')
    skip_head_line_count = source_table_settings.get('skip_head_line_count')

    # create the external DB if not exist
    create_external_db_if_not_exists(ext_db_client, external_db_name)

    # if not in utf-8, convert files source location as utf-8
    if source_encoding.lower() != 'utf-8':
        convert_s3_files_to_utf8(src_client, source_bucket, uri, source_encoding, source_file_type)

    # check s3 has data either existing or recoverable
    s3_uri = get_s3_uri_with_bucket_prefix(source_bucket, uri)
    s3_has_data = check_s3_files_exist(src_client, source_bucket, uri)
    s3_has_recoverable_data = list_recoverable_s3_file_versions(src_client, source_bucket, uri)

    # if files in s3 then good to create spectrum schema
    if s3_has_data:
        LOGGER.info(f"Files exist in S3 URI: {s3_uri}. No recovery needed.")

    # otherwise if s3 has only recoverable data, recover the latest data
    elif s3_has_recoverable_data:
        latest_files = recover_latest_s3_files(src_client, source_bucket, uri)
        if latest_files:
            recovered_keys = [file['Key'] for file in latest_files]
            LOGGER.info(f"Recovered the latest data files: {', '.join(recovered_keys)}")
            s3_uris = [f"s3://{source_bucket}/{file['Key']}" for file in latest_files]
            LOGGER.debug(f"Full paths for debugging: {s3_uris}")
        else:
            raise FileNotFoundError(f"No data files found in S3 URI: {s3_uri}")

    else:
        LOGGER.warning(f"Creating spectrum table with 0 data. Any tests referencing source likely to fail")

    # create spectrum schema if it's not existing already
    create_spectrum_schema_in_wh_if_not_exists(wh_client, spectrum_schema, external_db_name)

    # create table only if it's not existing already, otherwise delete existing and create
    table_exists = check_table_exists_in_external_db(ext_db_client, external_db_name, table_name)

    if table_exists:
        delete_spectrum_table_in_catalog_if_exists(ext_db_client, external_db_name, table_name)

    create_external_table(
        wh_client, f"{spectrum_schema}.{table_name}", s3_uri, stored_as, skip_head_line_count,
        row_format_serde, sep_char, quote_char, escape_char, test_columns
    )

    LOGGER.info(f"Table {table_name} created/recreated in Glue database {external_db_name}")
