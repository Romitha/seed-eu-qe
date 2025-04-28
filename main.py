import argparse
import json
import os

import pytest


def parse_arguments() -> argparse.Namespace:
    """
    Parses command-line arguments

    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description="Run test automation with pytest.")

    # Mandatory CLI arguments
    parser.add_argument("--run_mode", choices=["cicd", "etl", "local"], help="Run mode: 'cicd', 'etl', or 'local'.")
    parser.add_argument("--test_env", help="Comma-separated list of testing environments.")
    parser.add_argument("--file_names", help="Comma-separated list of file names to test.")

    # Argument mode selection
    parser.add_argument("--args_mode", choices=["cli", "json"], default="cli",
                        help="Mode of arguments. Default is 'cli'.")
    parser.add_argument("--json_path", help="Path to JSON configuration file (required for 'json' mode).")

    # Default or some Optional arguments here
    parser.add_argument("--detect_env_vars", action="store_true", help="Detect environment variables.")
    parser.add_argument("--remote_secrets_src_type", default="secrets_manager",
                        help="Type of remote secrets source. Default is 'secrets_manager'.")
    parser.add_argument("--remote_settings_src_type", default="parameter_store",
                        help="Type of remote settings source. Default is 'parameter_store'.")
    return parser.parse_args()


def validate_required_args(args: argparse.Namespace) -> None:
    """
    Validates the required CLI arguments

    Args:
        args (argparse.Namespace): Parsed arguments

    Raises:
        ValueError: If any required argument is missing
    """
    required_args = ["run_mode", "test_env", "file_names"]
    missing_args = [arg for arg in required_args if not getattr(args, arg, None)]

    if missing_args:
        raise ValueError(f"Missing required argument(s): {', '.join(f'--{arg}' for arg in missing_args)}")


def build_pytest_args(args: argparse.Namespace) -> list[str]:
    """
    Constructs pytest arguments based on the provided CLI arguments

    Args:
        args (argparse.Namespace): Parsed arguments

    Returns:
        list[str]: List of pytest arguments
    """
    validate_required_args(args)

    pytest_args = [
        "--run_mode", args.run_mode,
        "--test_env", args.test_env,
        "--file_names", args.file_names,
    ]
    pytest_args.extend(collect_optional_arguments(args))

    return pytest_args


def collect_optional_arguments(args: argparse.Namespace) -> list[str]:
    """
    Collects optional pytest arguments based on parsed arguments

    Args:
        args (argparse.Namespace): Parsed arguments

    Returns:
        list[str]: List of optional pytest arguments
    """
    optional_args = []

    if args.detect_env_vars:
        optional_args.append("--detect_env_vars")

    if args.remote_secrets_src_type:
        optional_args.extend(["--remote_secrets_src_type", f"{args.remote_secrets_src_type}_remote_config"])

    if args.remote_settings_src_type:
        optional_args.extend(["--remote_settings_src_type", f"{args.remote_settings_src_type}_remote_config"])

    return optional_args


def load_json_config(json_path: str) -> dict:
    """
    Loads and validates the JSON configuration file

    Args:
        json_path (str): The path to the JSON configuration file

    Returns:
        dict: Parsed JSON configuration

    Raises:
        FileNotFoundError: If the JSON file is not found
        ValueError: If the JSON file is invalid.
    """
    if not os.path.exists(json_path):
        raise FileNotFoundError(f"JSON configuration file not found: {json_path}")

    try:
        with open(json_path, "r", encoding="utf-8") as file:
            return json.load(file)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON configuration file: {json_path}") from e


def extract_json_arguments(json_config: dict) -> list[str]:
    """
    Extracts necessary arguments from the JSON configuration

    Args:
        json_config (dict): Parsed JSON configuration

    Returns:
        list[str]: List of extracted pytest arguments

    Raises:
        ValueError: If required fields are missing in JSON
    """
    team = json_config.get("teams", [{}])[0]

    # Ensure mandatory fields exist
    run_mode = team.get("run_mode")
    if not run_mode:
        raise ValueError("The 'run_mode' field is required in the JSON configuration.")

    pytest_args = ["--run_mode", run_mode]

    # Convert team dict into a Namespace safely
    team_namespace = argparse.Namespace(**team)

    # Extract optional arguments correctly
    optional_args = collect_optional_arguments(team_namespace)
    pytest_args.extend(optional_args)

    return pytest_args


def main() -> None:
    """
    Main entry point for running the test automation
    """
    args = parse_arguments()

    # Base pytest arguments
    pytest_args = ["-p", "no:warnings", "-s", "./tests/test_data_flow.py", "--args_mode", args.args_mode]

    if args.args_mode == "cli":
        pytest_args.extend(build_pytest_args(args))

    elif args.args_mode == "json":
        if not args.json_path:
            raise ValueError("In 'json' mode, --json_path is required.")

        json_config = load_json_config(args.json_path)
        pytest_args.extend(["--json_path", args.json_path])
        pytest_args.extend(extract_json_arguments(json_config))

    exit_code = pytest.main(pytest_args)
    exit(exit_code)


if __name__ == "__main__":
    main()
