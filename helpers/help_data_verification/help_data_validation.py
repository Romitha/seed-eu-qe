from utils.framework.custom_logger_util import get_logger
from utils.framework.data_validation_utils.scd_util import (
    check_scd_nulls, check_scd_values_for_major_columns,
    check_scd_values_for_minor_columns, get_scd_default_cols,
    run_lndp_edwp_script_for_scd_tables, update_scd_maj_min_columns,
    validate_deleted_records_for_scd_table)
from utils.framework.data_validation_utils.validation_rule_util import \
    validate_rules

LOGGER = get_logger()


class DataValidationHelper:
    """
    A helper class for executing various data validation checks and storing their results.
    """

    def __init__(self, layer_name, layer_settings, client):
        """
        Initialize the helper with client and required settings or info to run data validation checks
        """
        self.layer_name = layer_name
        self.layer_settings = layer_settings
        self.client = client

    def finalize_and_run_scd_checks(self):
        """
        Finalize scd settings and run scd validation for generated data or existing data
        """
        if self.layer_name == "source":
            LOGGER.info(
                f"Skipping scd checks for "
                f"{self.layer_name} in unsupported layer {self.layer_settings.get('schema_name')}")
            return {
                'status': "Skipped",
                'test_details': {
                    'message': f"Skipping scd checks for {self.layer_name} in unsupported layer"
                }
            }

        lndp_schema_name = self.layer_settings['lndp_settings']['schema_name']
        lndp_table_name = self.layer_settings['lndp_settings']['table_name']

        edwp_schema_name = self.layer_settings['schema_name']
        edwp_table_name = self.layer_settings['table_name']

        scd_settings = self.layer_settings['scd_settings']
        scd_columns = get_scd_default_cols(scd_settings)

        pat = self.layer_settings["git_pat"]
        run_mode = self.layer_settings["run_mode"]

        try:
            if self.layer_settings["confirm_synth_data_gen"]:

                # Step 0: run landing to target script since new record (s) already inserted in lndp (script run)
                run_lndp_edwp_script_for_scd_tables(self.client, scd_settings, pat, run_mode)

                # Step 1 - Check target table's SCD columns for null values (inserted)
                check_scd_nulls(self.client, edwp_schema_name, edwp_table_name, scd_columns)

                # Step 2 - Update columns (updated maj)
                update_scd_maj_min_columns(self.client, lndp_schema_name, lndp_table_name, scd_settings, "major")

                # Step 3: - run the load script for scd logics (script run)
                run_lndp_edwp_script_for_scd_tables(self.client, scd_settings, pat, run_mode)

                # Step 4: - validate or check the scd column's value for the major cols update scenario
                hash_info = check_scd_values_for_major_columns(self.client, edwp_schema_name, edwp_table_name,
                                                               scd_columns)

                # step 5: Update columns (updated min)
                update_scd_maj_min_columns(self.client, lndp_schema_name, lndp_table_name, scd_settings, "minor")

                # Step 6: - run the load script for scd logics (script run)
                run_lndp_edwp_script_for_scd_tables(self.client, scd_settings, pat, run_mode)

                # Step 7: - validate or check the scd column's value for the min cols update scenario
                check_scd_values_for_minor_columns(self.client, edwp_schema_name, edwp_table_name, scd_columns,
                                                   hash_info)

                # Step 8: Check current record status 'Y' and src del status 'Y' records having end dt '9999-12-31' date
                validate_deleted_records_for_scd_table(self.client, edwp_schema_name, edwp_table_name, scd_settings)

            else:

                # Step 1 - Check target table's SCD columns for null values
                check_scd_nulls(self.client, edwp_schema_name, edwp_table_name, scd_columns)

                # Step 2: Check current record status 'Y' and src del status 'Y' records having end dt '9999-12-31' date
                validate_deleted_records_for_scd_table(self.client, edwp_schema_name, edwp_table_name, scd_settings)

            # If we reach here, all SCD checks passed
            return {
                'status': True,
                'test_details': {
                    'message': f"SCD validation completed successfully for {edwp_schema_name}.{edwp_table_name}"
                }
            }

        except Exception as e:
            error_message = f"Error during scd validation: {str(e)}"
            LOGGER.error(error_message)
            # Return False status instead of "Error" to properly fail the test
            return {
                'status': False,
                'test_details': {
                    'message': error_message,
                    'error_type': 'SCD_VALIDATION_ERROR'
                }
            }

    def finalize_and_run_rule_checks(self):
        """
        Runs rule-based validation checks for both source and target layers

        Returns:
            dict: Dictionary containing all validation rule check results
        """
        # Skip rule based validation on columns if data is synthetically generated

        if self.layer_settings.get('no_src_support'):
            LOGGER.info(
                f"Skipping rule_checks checks for {self.layer_name} "
                f"in unsupported layer {self.layer_settings.get('schema_name')}"
            )
            return {
                'status': "Skipped",
                'test_details': {
                    'message': f"Skipping rule_checks validation for {self.layer_name} since data gen is enabled"
                }
            }

        rule_check_results = {}
        table_name = None  # Ensure table_name is initialized before any usage

        try:
            # Determine schema and table name based on layer type
            if self.layer_name == "source":
                schema_name = self.layer_settings.get('spectrum_schema')
                table_name = self.layer_settings.get('table_identifier')
            elif self.layer_name in ["target_lndp", "target_edwp"]:
                schema_name = self.layer_settings.get('schema_name')
                table_name = self.layer_settings.get('table_name')
            else:
                return {
                    'status': "Skipped",
                    'test_details': {
                        'message': f"Skipping rule validation checks for unsupported layer {self.layer_name}"
                    }
                }

            # Ensure table_name is properly defined
            if not table_name:
                return {
                    'status': False,
                    'test_details': {
                        'message': f"Table name is missing for layer {self.layer_name}"
                    }
                }

            # Extract validation rules and expected columns
            validation_rules = self.layer_settings['columns_info'].get('validation_rules', {})
            expected_columns = self.layer_settings['columns_info'].get('expected_columns', [])

            # Run validation
            results = validate_rules(self.client, validation_rules, schema_name, table_name, expected_columns)

            # Handle case where validation query returned no results
            if not results or 'test_details' not in results:
                return {
                    'status': False,
                    'test_details': {
                        'message': f"Validation query returned no results for {table_name}"
                    }
                }
            else:
                return results

        except Exception as e:
            LOGGER.error(f"Error during validation rule checks for {table_name or 'Unknown Table'}: {str(e)}")
            return {
                'status': False,
                'test_details': {
                    'message': f"Error during validation rule checks for {table_name or 'Unknown Table'}: {str(e)}"
                }
            }
