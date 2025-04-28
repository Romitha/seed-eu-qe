from data_verification.verification_builder import VerificationBuilder
from utils.framework.custom_logger_util import get_logger

LOGGER = get_logger()


class BuilderCommandHelper:
    def __init__(self):

        self.builder = VerificationBuilder()

    def get_builder_commands_for_data_verification(self, layer_info, load_strategy):

        layer_scope = layer_info['scope']

        if layer_scope.get("data_validation", False) or load_strategy == 'scd':
            self.builder.build_with_validation()
        else:
            LOGGER.info("Skipping data validation for layer")

        if layer_scope.get("data_quality", False):
            self.builder.build_with_quality()
        else:
            LOGGER.info("Skipping data quality checks for layer")

        return self.builder.build()

