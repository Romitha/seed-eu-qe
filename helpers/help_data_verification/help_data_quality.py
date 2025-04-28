import re

from utils.framework.custom_data_verification_util import (
    convert_dict_dtypes, find_string_dates_needing_cast,
    get_col_dict_from_expected_cols)
from utils.framework.custom_logger_util import get_logger
from utils.framework.data_quality_utils.accuracy_util import \
    check_numeric_precision_for_column
from utils.framework.data_quality_utils.completeness_util import (
    check_blank_rows, check_missing_column, check_src_blank_rows,
    check_src_missing_column, check_src_unexpected_nulls,
    check_unexpected_nulls, validate_external_table_schema,
    validate_internal_table_schema)
from utils.framework.data_quality_utils.consistency_util import (
    check_col_and_row_data_consistency, check_column_count_consistency,
    check_row_count_consistency)
from utils.framework.data_quality_utils.duplication_util import (
    check_src_column_name_duplicates, check_src_row_duplicates,
    check_trg_column_name_duplicates, check_trg_latest_row_duplicates)
from utils.framework.data_quality_utils.timeliness_util import \
    check_timeliness_in_latest_batch

LOGGER = get_logger()


class DataQualityHelper:
    """
    A helper class for executing various data quality checks and returning their results
    """

    def __init__(self, layer_name, layer_settings, client):
        """
        Initialize the helper with client and required settings or info
        """
        self.layer_name = layer_name
        self.layer_settings = layer_settings
        self.client = client

    def finalize_and_run_timeliness_checks(self):
        """
        Runs timeliness checks for both source and target layers, validating whether
        data in the latest batch falls within the expected time frame

        Returns:
            dict: Dictionary containing all timeliness check results
        """

        # Skip timeliness checks for the source layer
        if self.layer_name == "source":
            LOGGER.info(
                f"Skipping timeliness checks for {self.layer_name} "
                f"in unsupported layer {self.layer_settings.get('schema_name')}"
            )
            return {
                'status': "Skipped",
                'test_details': {
                    'message':
                        f"Skipping timeliness checks for "
                        f"{self.layer_name} in unsupported layer {self.layer_settings.get('schema_name')}"
                }
            }

        schema_name = self.layer_settings.get('schema_name')
        table_name = self.layer_settings.get('table_name')

        sys_dt_cols_with_expected_time = self.layer_settings['columns_info']['timeliness_columns']
        timeliness_check_results = {}

        try:
            if sys_dt_cols_with_expected_time:
                # Check if all expected hours are 0
                if all(value == 0 for value in sys_dt_cols_with_expected_time.values()):
                    LOGGER.warning(
                        f"Skipping timeliness checks because expected hours are all 0 for {table_name}"
                    )
                    return {
                        'status': "Skipped",
                        'test_details': {
                            'message': f"Skipping timeliness checks because expected hours are all 0 for {table_name}"
                        }
                    }

                # Filter out columns where expected_within_hours == 0
                filtered_cols = {k: v for k, v in sys_dt_cols_with_expected_time.items() if v != 0}

                # Run timeliness checks for each column
                for sys_dt_col, expected_within_hours in filtered_cols.items():
                    try:
                        timeliness_check_results[sys_dt_col] = check_timeliness_in_latest_batch(
                            self.client, schema_name, table_name, sys_dt_col, expected_within_hours
                        )
                    except Exception as e:
                        timeliness_check_results[sys_dt_col] = {
                            'status': False,
                            'test_details': {
                                'message': f"Failed: {str(e)}"
                            }
                        }

            LOGGER.info(f"Timeliness checks completed for {table_name}: {timeliness_check_results}")

            return timeliness_check_results

        except Exception as e:
            LOGGER.error(f"Error during timeliness checks for {table_name}: {str(e)}")
            return {
                'status': "Failed",
                'test_details': {
                    'message': f"Error during timeliness checks: {str(e)}"
                }
            }

    def finalize_and_run_duplication_checks(self):
        """
        Runs duplication checks for both source and target layers, validating duplicate column names
        and duplicate rows in the latest batch

        Returns:
            dict: Dictionary containing all duplication check results
        """

        unique_columns = self.layer_settings['columns_info']['unique_columns']
        expected_columns = self.layer_settings['columns_info']['expected_columns']
        has_mapping = self.layer_settings['has_mapping']

        final_cols = self.layer_settings['mapped_expected_cols'] if has_mapping else expected_columns
        sys_insert_col = self.layer_settings['columns_info']['system_columns'][1]

        # Always define table_name before usage
        schema_name = self.layer_settings.get('schema_name')
        table_name = self.layer_settings.get('table_name')

        duplication_check_results = {}

        try:
            if self.layer_name == "source":
                schema_name = self.layer_settings.get('spectrum_schema')
                table_name = self.layer_settings.get('table_identifier')

                duplication_check_results = {
                    'check_column_name_duplicates': check_src_column_name_duplicates(self.client, schema_name,
                                                                                     table_name),
                    'check_row_duplicates': check_src_row_duplicates(self.client, schema_name, table_name,
                                                                     final_cols, unique_columns)
                }

            elif self.layer_name in ["target_lndp", "target_edwp"]:
                duplication_check_results = {
                    'check_column_name_duplicates': check_trg_column_name_duplicates(self.client, schema_name,
                                                                                     table_name),
                    'check_latest_row_duplicates': check_trg_latest_row_duplicates(self.client, schema_name, table_name,
                                                                                   final_cols, unique_columns,
                                                                                   sys_insert_col)
                }

            LOGGER.info(f"Duplication checks completed for {table_name}: {duplication_check_results}")

            return duplication_check_results

        except Exception as e:
            LOGGER.error(f"Error during duplication checks for {table_name}: {str(e)}")
            return {
                'status': "Failed",
                'test_details': {
                    'message': f"Error during duplication checks for {table_name}: {str(e)}"
                }
            }

    def finalize_and_run_completeness_checks(self):
        """
        Runs completeness checks for both source and target layers, validating schema,
        missing columns, blank rows, and unexpected nulls

        Returns:
            dict: Dictionary containing all completeness checks results
        """
        expected_columns = self.layer_settings['columns_info']['expected_columns']
        has_mapping = self.layer_settings['has_mapping']

        final_cols = self.layer_settings['mapped_expected_cols'] if has_mapping else expected_columns

        cols_dict = get_col_dict_from_expected_cols(final_cols)
        column_names = list(cols_dict.keys())

        dtype_mapping = self.layer_settings['columns_info']['internal_external_data_type_mapping']
        converted_cols_dict = convert_dict_dtypes(dtype_mapping, cols_dict)

        completeness_check_results = {}

        try:
            if self.layer_name == "source":
                schema_name = self.layer_settings.get('spectrum_schema')
                table_name = self.layer_settings.get('table_identifier')

                completeness_check_results = {
                    'schema_validation': validate_external_table_schema(self.client, schema_name, table_name,
                                                                        converted_cols_dict),
                    'check_missing_columns': check_src_missing_column(self.client, schema_name, table_name,
                                                                      column_names),
                    'check_blank_rows': check_src_blank_rows(self.client, schema_name, table_name),
                }

                null_columns = self.layer_settings['columns_info']['null_columns']
                test_cols_for_nulls = [col for col in column_names if null_columns is None or col not in null_columns]

                if test_cols_for_nulls:
                    completeness_check_results['check_unexpected_nulls'] = check_src_unexpected_nulls(
                        self.client, schema_name, table_name, test_cols_for_nulls)

            elif self.layer_name in ["target_lndp", "target_edwp"]:
                schema_name = self.layer_settings.get('schema_name')
                table_name = self.layer_settings.get('table_name')

                completeness_check_results = {
                    'schema_validation': validate_internal_table_schema(self.client, schema_name, table_name,
                                                                        cols_dict),
                    'check_missing_columns': check_missing_column(self.client, schema_name, table_name, column_names),
                    'check_blank_rows': check_blank_rows(self.client, schema_name, table_name),
                }

                null_columns = self.layer_settings['columns_info']['null_columns']
                test_cols_for_nulls = [col for col in column_names if null_columns is None or col not in null_columns]

                if test_cols_for_nulls:
                    completeness_check_results['check_unexpected_nulls'] = check_unexpected_nulls(self.client,
                                                                                                  schema_name,
                                                                                                  table_name,
                                                                                                  test_cols_for_nulls)

            LOGGER.info(f"Completeness checks completed: {completeness_check_results}")
            return completeness_check_results

        except Exception as e:
            LOGGER.error(f"Error during completeness checks: {str(e)}")
            raise

    def finalize_and_run_consistency_checks(self):
        """
        Runs consistency checks for all layers except the source

        Returns:
            dict: Dictionary containing all consistency check results
        """

        # Skip consistency checks for the source layer
        if self.layer_name == "source" or self.layer_settings.get('no_src_support'):
            LOGGER.info(
                f"Skipping consistency checks for {self.layer_name} "
                f"in unsupported layer {self.layer_settings.get('schema_name')}"
            )
            return {
                'status': "Skipped",
                'test_details': {
                    'message': f"Skipping consistency checks for {self.layer_name} "
                               f"in unsupported layer {self.layer_settings.get('schema_name')}"
                }
            }

        # Define schema and table names
        src_schema = self.layer_settings.get('spectrum_schema')
        src_table = self.layer_settings.get('table_identifier')
        trg_schema = self.layer_settings.get('schema_name')
        trg_table = self.layer_settings.get('table_name')

        sys_cols = len(self.layer_settings['columns_info']['system_columns'])
        scd_cols = 0
        internal_schema = None
        where_clause = None
        using_synthetic_data = self.layer_settings['confirm_synth_data_gen']

        if self.layer_settings.get('lndp_settings'):
            src_schema = self.layer_settings['lndp_settings']['schema_name']
            src_table = self.layer_settings['lndp_settings']['table_name']
            sys_cols = 0
            internal_schema = True

        if 'scd_settings' in self.layer_settings:
            scd_enabled = bool(self.layer_settings['scd_settings']['enable_scd_validations'])
            if scd_enabled and self.layer_name.endswith('edwp'):
                scd_cols = len(self.layer_settings['scd_default_columns'])
                where_clause = "WHERE curr_rec_ind = 'Y' AND src_del_ind = 'N'"

        consistency_check_results = {}

        try:
            # Column Count Consistency Check
            consistency_check_results['check_column_count_consistency'] = check_column_count_consistency(
                self.client, internal_schema, src_schema, src_table, trg_schema, trg_table, sys_cols, scd_cols
            )
            LOGGER.info(f'Consistency check phase-1: {consistency_check_results["check_column_count_consistency"]}')

            # Row Count Consistency Check
            consistency_check_results['check_row_count_consistency'] = check_row_count_consistency(
                self.client, src_schema, src_table, trg_schema, trg_table, using_synthetic_data,
                where_clause=where_clause
            )
            LOGGER.info(f'Consistency check phase-2: {consistency_check_results["check_row_count_consistency"]}')

            # # Column & Row Data Consistency Check
            # unique_columns = self.layer_settings['columns_info']['unique_columns']
            # expected_columns = self.layer_settings['columns_info']['expected_columns']
            # has_mapping = self.layer_settings['has_mapping']
            # mapped_columns = self.layer_settings['mapped_expected_cols'] if has_mapping else expected_columns
            # clean_mapped_cols = list(get_col_dict_from_expected_cols(mapped_columns).keys())
            # cols_needing_cast = find_string_dates_needing_cast(expected_columns, mapped_columns)
            # consistency_check_results['check_col_and_row_data_consistency'] = check_col_and_row_data_consistency(
            #     self.client, src_schema, src_table, trg_schema, trg_table, unique_columns, clean_mapped_cols, cols_needing_cast,False)
            # LOGGER.info(f'Consistency check phase-3: {consistency_check_results["check_col_and_row_data_consistency"]}')

            LOGGER.info('Consistency check phase-3: development in progress')
            return consistency_check_results

        except Exception as e:
            LOGGER.error(f"Error during consistency checks for {trg_table}: {str(e)}")
            return {
                'status': "Failed",
                'test_details': {
                    'message': f"Error during consistency checks for {trg_table}: {str(e)}"
                }
            }

    def finalize_and_run_accuracy_checks(self):
        """
        Runs accuracy checks for all layers except the source

        Returns:
            dict: Dictionary containing all accuracy check results
        """

        # Skip accuracy checks for the source layer
        if self.layer_name == "source":
            LOGGER.info(
                f"Skipping accuracy checks for {self.layer_name} "
                f"in unsupported layer {self.layer_settings.get('schema_name')}"
            )
            return {
                'status': "Skipped",
                'test_details': {
                    'message':
                        f"Skipping accuracy checks for "
                        f"{self.layer_name} in unsupported layer {self.layer_settings.get('schema_name')}"
                }
            }

        # Always define table_name before usage
        schema_name = self.layer_settings.get('schema_name')
        table_name = self.layer_settings.get('table_name')

        if self.layer_name == 'target_edwp':
            # Get mapped columns for edwp
            # Get mapped columns for edwp
            mapped_columns = self.layer_settings["columns_info"]["mapped_cols"]
            if mapped_columns:
                expected_cols_with_numeric_type = mapped_columns['edwp']
            else:
                expected_cols_with_numeric_type = self.layer_settings["columns_info"]["expected_columns"]
        else:
            expected_cols_with_numeric_type = self.layer_settings["columns_info"]["expected_columns"]

        # Regex pattern to match NUMERIC(p, s)
        numeric_pattern = re.compile(r'(\w+)\s+NUMERIC\((\d+),\s*(\d+)\)')

        # Extract numeric columns into a dictionary
        numeric_cols_dict = {
            match.group(1): {
                "num_precision": int(match.group(2)),
                "numeric_scale": int(match.group(3))
            }
            for col in expected_cols_with_numeric_type if (match := numeric_pattern.match(col))
        }

        accuracy_check_results = {}

        try:
            if numeric_cols_dict:
                # Run numeric precision checks for each column
                for column, settings in numeric_cols_dict.items():
                    try:
                        accuracy_check_results[column] = check_numeric_precision_for_column(
                            self.client, schema_name, table_name, column, settings
                        )
                    except Exception as e:
                        accuracy_check_results[column] = {
                            'status': False,
                            'test_details': {
                                'message': f"Failed: {str(e)}"
                            }
                        }

                LOGGER.info(f"Accuracy checks completed for {table_name}: {accuracy_check_results}")

                return accuracy_check_results

            else:
                LOGGER.info(f"No numeric precision checks provided for {self.layer_name}")
                return {
                    'status': "Skipped",
                    'test_details': {
                        'message': f"No numeric precision checks provided for {self.layer_name}"
                    }
                }

        except Exception as e:
            LOGGER.error(f"Error during accuracy checks for {table_name}: {str(e)}")
            return {
                'status': "Failed",
                'test_details': {
                    'message': f"Error during accuracy checks for {table_name}: {str(e)}"
                }
            }
