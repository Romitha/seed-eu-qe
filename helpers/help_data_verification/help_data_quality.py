import re

from utils.framework.custom_data_verification_util import (
    convert_dict_dtypes, find_string_dates_needing_cast,
    get_col_dict_from_expected_cols, generate_lndp_and_edwp_col_values)
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
from utils.framework.data_quality_utils.history_validation_util import check_history_timestamps, \
    check_latest_history_matches, check_row_counts, check_history_table_existence
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
        expected_columns = []
        unique_columns = []
        config_unique_columns = self.layer_settings['columns_info']['unique_columns']
        config_expected_columns = self.layer_settings['columns_info']['expected_columns']
        has_mapping = self.layer_settings['has_mapping']

        lndp_columns, ewdp_columns = generate_lndp_and_edwp_col_values(config_expected_columns)
        if self.layer_name == "target_lndp":
            expected_columns = lndp_columns
        elif self.layer_name == "target_edwp":
            expected_columns = ewdp_columns

        unique_lndp_columns, unique_ewdp_columns = generate_lndp_and_edwp_col_values(config_unique_columns)
        if self.layer_name == "target_lndp":
            unique_columns = unique_lndp_columns
        elif self.layer_name == "target_edwp":
            unique_columns = unique_ewdp_columns

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
        expected_columns = []
        config_columns = self.layer_settings['columns_info']['expected_columns']
        lndp_columns, ewdp_columns = generate_lndp_and_edwp_col_values(config_columns)
        if self.layer_name == "target_lndp":
            expected_columns = lndp_columns
        elif self.layer_name == "target_edwp":
            expected_columns = ewdp_columns
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
            if consistency_check_results["check_row_count_consistency"]['status'] == 'Warning':
                LOGGER.warning(f'Consistency check phase-2: {consistency_check_results["check_row_count_consistency"]}')
            else:
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

    def finalize_and_run_history_validation(self):
        """
        Runs history validation checks for truncate-load tables, ensuring that
        historical data is properly maintained during load operations.

        Returns:
            dict: Dictionary containing all history validation check results
        """
        # Skip for source layer or if load_strategy is not appropriate
        history_validation_results ={}
        LOGGER.info("=" * 80)
        LOGGER.info("STARTING HISTORY VALIDATION CHECKS")
        LOGGER.info("=" * 80)

        # Skip for source layer or if load_strategy is not appropriate
        history_validation_results = {}
        if self.layer_name in ["source", "target_lndp"]:
            skip_message = f"Skipping history validation for {self.layer_name} layer - only applicable for EWP layer with truncate-load strategy"
            LOGGER.info(skip_message)
            LOGGER.info(f"RESULT: ⚠️  SKIPPED - {self.layer_name} layer not supported")
            LOGGER.info("=" * 80)
            return {
                'status': "Skipped",
                'test_details': {
                    'message': skip_message
                }
            }

        # Get the table load strategy
        load_strategy = self.layer_settings.get('load_strategy')
        if load_strategy != 'truncate_load':
            skip_message = f"History validation is only applicable for truncate_load strategy, not {load_strategy}"
            LOGGER.info(f"Current load strategy: {load_strategy}")
            LOGGER.info(skip_message)
            LOGGER.info("RESULT: ⚠️  SKIPPED - Non-truncate-load strategy")
            LOGGER.info("=" * 80)
            return {
                'status': "Skipped",
                'test_details': {
                    'message': skip_message
                }
            }

        # Get schema and table names
        schema_name = self.layer_settings.get('schema_name')
        table_name = self.layer_settings.get('table_name')
        history_table_name = self.layer_settings.get('history_table_name')
        unique_columns = self.layer_settings['columns_info']['unique_columns']

        LOGGER.info(f"📊 Target Table: {schema_name}.{table_name}")
        LOGGER.info(f"📚 History Table: {schema_name}.{history_table_name}")
        LOGGER.info(f"🔑 Unique Columns: {unique_columns}")
        LOGGER.info(f"⚙️  Load Strategy: {load_strategy}")
        LOGGER.info("-" * 80)

        # Initialize detailed results structure
        detailed_results = {
            'table_info': {
                'schema_name': schema_name,
                'table_name': table_name,
                'history_table_name': history_table_name,
                'load_strategy': load_strategy,
                'unique_columns': unique_columns
            },
            'checks_performed': {
                'history_table_existence': {},
                'row_count_comparison': {},
                'latest_history_matching': {},
                'timestamp_progression': {}
            },
            'summary': {}
        }

        try:
            # Check 1: Verify history table exists
            history_check = check_history_table_existence(self.client, schema_name, table_name, history_table_name)

            if not history_check['status']:
                LOGGER.warning(f"History table not found: {history_check['message']}")
                return {
                    'status': "Warning",  # Use "Warning" instead of False
                    'test_details': {
                        'message': history_check['message'],
                        'recommendation': "Consider implementing a history table for this truncate-load table to maintain historical records"
                    }
                }
            # Store Check 1 results
            detailed_results['checks_performed']['history_table_existence'] = {
                'status': history_check['status'],
                'message': history_check['message'],
                'history_table_name': history_check.get('history_table_name'),
                'check_completed': True
            }

            # Check 2: Compare row counts
            count_check = check_row_counts(self.client, schema_name, table_name, history_table_name)
            if not count_check['status']:
                LOGGER.warning(f"Issue of Compare row counts: {count_check['message']}")
                return {
                    'status': "Warning",
                    'test_details': {
                        'message': count_check['message'],
                        'main_count': count_check.get('main_count'),
                        'history_count': count_check.get('history_count'),
                    }
                }
            # Store Check 2 results
            detailed_results['checks_performed']['row_count_comparison'] = {
                'status': count_check['status'],
                'message': count_check['message'],
                'main_count': count_check.get('main_count'),
                'history_count': count_check.get('history_count'),
                'check_completed': True
            }

            # Check 3: Verify latest history matches main table
            match_check = check_latest_history_matches(self.client, schema_name, table_name, history_table_name, unique_columns)
            if not match_check['status']:
                LOGGER.warning(f"❌ CHECK 3 FAILED: {match_check['message']}")
                return {
                    'status': False,
                    'test_details': {
                        'message': match_check['message'],
                        'unmatched_count': match_check.get('unmatched_count')
                    }
                }
            # Store Check 3 results
            detailed_results['checks_performed']['latest_history_matching'] = {
                'status': match_check['status'],
                'message': match_check['message'],
                'main_count': match_check.get('main_count'),
                'history_count': match_check.get('history_count'),
                'matched_count': match_check.get('matched_count'),
                'expected_matches': match_check.get('expected_matches'),
                'latest_history_date': match_check.get('latest_history_date'),
                'unique_columns_used': match_check.get('unique_columns_used'),
                'unmatched_count': match_check.get('unmatched_count'),
                'check_completed': True
            }
            # Check 4: Verify timestamp progression
            timestamp_check = check_history_timestamps(self.client, schema_name, history_table_name)

            if not timestamp_check['status']:
                LOGGER.error(f"❌ CHECK 4 FAILED: {timestamp_check['message']}")
                return {
                    'status': False,
                    'test_details': {
                        'message': timestamp_check['message'],
                        'distinct_timestamps': timestamp_check.get('distinct_timestamps')
                    }
                }
            # Store Check 4 results
            detailed_results['checks_performed']['timestamp_progression'] = {
                'status': timestamp_check['status'],
                'message': timestamp_check['message'],
                'distinct_timestamps': timestamp_check.get('distinct_timestamps'),
                'first_timestamp': timestamp_check.get('first_timestamp'),
                'last_timestamp': timestamp_check.get('last_timestamp'),
                'total_records': timestamp_check.get('total_records'),
                'check_completed': True
            }
            # All checks passed - Create summary
            detailed_results['summary'] = {
                'total_checks': 4,
                'passed_checks': 4,
                'failed_checks': 0,
                'warning_checks': 0,
                'overall_status': 'Passed',
                'validation_date': str(timestamp_check.get('last_timestamp', 'Unknown')),
                'data_quality_score': '100%'
            }

            # All checks passed
            LOGGER.info("-" * 80)
            LOGGER.info("🎉 ALL HISTORY VALIDATION CHECKS PASSED!")
            LOGGER.info(f"✅ History validation successful for {schema_name}.{table_name}")

            history_validation_results = {
                'status': True,
                'test_details': {
                    'message': f"History validation successful for {schema_name}.{table_name}",
                    **detailed_results
                }
            }

            # Log summary
            LOGGER.info("📊 VALIDATION SUMMARY:")
            LOGGER.info(f"   Main table rows: {count_check.get('main_count'):,}")
            LOGGER.info(f"   History table rows: {count_check.get('history_count'):,}")
            LOGGER.info(f"   Timestamp versions: {timestamp_check.get('distinct_timestamps')}")
            LOGGER.info(f"   Records matched: {match_check.get('matched_count', 0):,}")
            LOGGER.info("=" * 80)

            return history_validation_results

        except Exception as e:
            error_message = f"Error during history validation for {table_name}: {str(e)}"
            LOGGER.error(f"💥 CRITICAL ERROR: {error_message}")
            LOGGER.error("=" * 80)
            # Mark all checks as failed due to exception
            for check_name in detailed_results['checks_performed']:
                if not detailed_results['checks_performed'][check_name].get('check_completed'):
                    detailed_results['checks_performed'][check_name] = {
                        'status': 'Error',
                        'message': f"Check failed due to exception: {str(e)}",
                        'check_completed': False
                    }

            return {
                'status': False,
                'test_details': {
                    'message': error_message,
                    **detailed_results
                }
            }