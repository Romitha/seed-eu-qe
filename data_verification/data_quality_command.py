from helpers.help_data_verification.help_data_quality import DataQualityHelper
from utils.framework.custom_logger_util import get_logger

class DataQualityCommand:

    def run_verification(self, client, layer_name: str, layer_info: dict, results: dict) -> None:
        """
        Run the data quality checks and store results under 'data_quality' key
        """
        data_quality_scope = layer_info.get('scope', {})
        layer_settings = layer_info.get('layer_settings', {})

        if 'data_quality' in data_quality_scope:
            # Ensure 'data_quality' key exists in results (only created once per run)
            if 'data_quality' not in results:
                results['data_quality'] = {}

            # Initialize Data Quality Helper
            dq_helper = DataQualityHelper(layer_name, layer_settings, client)

            # Run all available checks and append results
            self.run_checks(data_quality_scope['data_quality'], dq_helper, results['data_quality'])

        else:
            results['data_quality'] = "No data quality checks found in scope"

    @staticmethod
    def run_checks(checks_to_run, dq_helper, data_quality_results):
        """
        Run each data quality check if it exists in checks_to_run and append results
        """
        available_checks = {
            'timeliness': dq_helper.finalize_and_run_timeliness_checks,
            'duplication': dq_helper.finalize_and_run_duplication_checks,
            'completeness': dq_helper.finalize_and_run_completeness_checks,
            'consistency': dq_helper.finalize_and_run_consistency_checks,
            'accuracy': dq_helper.finalize_and_run_accuracy_checks,
            'history_validation': dq_helper.finalize_and_run_history_validation
        }

        for check_name, check_method in available_checks.items():
            if check_name in checks_to_run:
                data_quality_results[check_name] = check_method()  # Append results dynamically