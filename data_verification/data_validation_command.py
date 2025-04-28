from helpers.help_data_verification.help_data_validation import \
    DataValidationHelper


class DataValidationCommand:

    def run_verification(self, client, layer_name: str, layer_info: dict, results: dict) -> None:
        """
        Run the data validation checks and store results under 'data_validation' key.
        """

        data_validation_scope = layer_info.get('scope', {})
        layer_settings = layer_info.get('layer_settings', {})

        if 'data_validation' in data_validation_scope:
            # Ensure 'data_validation' key exists in results (created only once per run)
            if 'data_validation' not in results:
                results['data_validation'] = {}

            # Initialize Data Validation Helper
            dv_helper = DataValidationHelper(layer_name, layer_settings, client)

            # Run all available checks and append results
            self.run_checks(data_validation_scope['data_validation'], dv_helper, results['data_validation'])

        else:
            results['data_validation'] = "No data validation checks found in scope"

    @staticmethod
    def run_checks(checks_to_run, dv_helper, data_validation_results):
        """
        Run each data validation check if it exists in checks_to_run and append results.
        """

        available_checks = {
            'rule_checks': dv_helper.finalize_and_run_rule_checks,
            'scd_checks': dv_helper.finalize_and_run_scd_checks
        }

        for check_name, check_method in available_checks.items():
            if check_name in checks_to_run:
                data_validation_results[check_name] = check_method()  # Append results dynamically
