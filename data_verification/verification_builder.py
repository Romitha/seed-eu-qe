from data_verification.data_quality_command import DataQualityCommand
from data_verification.data_validation_command import DataValidationCommand
from data_verification.verification_protocol import VerificationProtocol


class VerificationBuilder:
    def __init__(self):
        self.commands: list[VerificationProtocol] = []

    def build_with_validation(self) -> 'VerificationBuilder':
        """
        Adds data validation checks to the builder
        """
        self.commands.append(DataValidationCommand())
        return self

    def build_with_quality(self) -> 'VerificationBuilder':
        """
        Adds data quality checks to the builder
        """
        self.commands.append(DataQualityCommand())
        return self

    def build(self) -> list[VerificationProtocol]:
        """
        Returns the assembled list of verification commands
        """
        return self.commands
