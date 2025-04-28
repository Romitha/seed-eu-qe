from typing import Protocol


class VerificationProtocol(Protocol):
    def run_verification(self, client, layer_name: str, layer_info: dict, results: dict) -> None:
        """
        Protocol for running a verification check on a layer.
        """
        ...
