import pytest

from custom_conf.initialize_config import ConfigInitializer
from helpers.help_conftest.help_fixtures import ConftestHelper

# use helper
helper = ConftestHelper()

# helper fix any path issues before run
helper.fix_any_path_issue_before_run()

# helper get logging capabilities
LOGGER = helper.get_logger()

"""
Pytest Configuration and Fixtures

This script provides additional command-line options for pytest and
configures the test environment by setting up logging and providing
fixtures for use across the test session.

References:
    - Pytest documentation: https://docs.pytest.org/
"""


def pytest_addoption(parser):
    """
    Define custom command-line options for pytest

    Args:
        parser: The argument parser object for pytest

    Command-line options:
        --args_mode (str): Mode of arguments, either 'cli' or 'json'. Default is 'cli'
        --test_env (str): Comma-separated list of testing environments (mandatory)
        --file_names (str): Comma-separated list of file names to test (mandatory)
        --detect_env_vars (bool): Option to detect environment variables
        --remote_secrets_src_type (str): Type of remote secrets source
        --run_mode (str): The mode in which the framework is running: 'cicd', 'etl', or 'local' (mandatory)
    """
    # Mandatory options
    parser.addoption(
        "--args_mode",
        action="store",
        default="cli",
        help="Provide cli or json, default is cli",
    )
    parser.addoption(
        "--json_path",
        action="store",
        default=None,
        help="Absolute path to the JSON configuration file (mandatory for json mode)"
    )
    parser.addoption(
        "--test_env",
        action="store",
        default=None,
        help="List of testing environments (mandatory)",
    )
    parser.addoption(
        "--file_names",
        action="store",
        default=None,
        help="List of file names to test (mandatory)",
    )
    parser.addoption(
        "--run_mode",
        action="store",
        required=True,
        choices=['cicd', 'etl', 'local'],
        help="The mode in which the framework is running: 'cicd', 'etl', or 'local'.",
    )

    # Optional options to override default values
    parser.addoption(
        "--detect_env_vars",
        action="store_true",
        default=None,
        help="Detect environment variables",
    )
    parser.addoption(
        "--remote_secrets_src_type",
        action="store",
        default=None,
        help="Type of remote config source",
    )
    parser.addoption(
        "--remote_settings_src_type",
        action="store",
        default=None,
        help="Type of remote config source",
    )


def pytest_configure(config):
    """
    Configure pytest settings before tests run

    This function sets up logging for the pytest session based on the
    provided command-line options

    Args:
        config (pytest.Config): The pytest configuration object
    """
    helper.initiate_setup_config(config)
    LOGGER.info("Pytest configuration and logging setup completed.")


@pytest.fixture(scope="session")
def all_tables_test_results():
    return {}


def pytest_generate_tests(metafunc):
    """
    Custom test generation based on teams, environments, and file names
    This function is automatically invoked by pytest
    """
    if "config_fixture" in metafunc.fixturenames:
        # Load the initial configuration
        config = get_initial_config(metafunc.config)

        param_combinations = helper.get_param_combination(config)

        # Parametrize the test with the generated parameters, using indirect=True
        metafunc.parametrize(
            "config_fixture",
            param_combinations,
            ids=[f"{team['team_key']}::{team['environment']}::{table}" for team, table in param_combinations],
            indirect=True,
        )


def get_initial_config(config):
    args_mode = config.getoption("--args_mode")
    run_mode = config.getoption("--run_mode")

    LOGGER.info(f"Test session activated with run mode: {run_mode}")

    # Determine configuration based on args_mode
    if args_mode == "cli":
        cli_config = {
            "test_environments": config.getoption("--test_env"),
            "detect_env_vars": config.getoption("--detect_env_vars"),
            "remote_secrets_src_type": config.getoption("--remote_secrets_src_type"),
            "remote_settings_src_type": config.getoption("--remote_settings_src_type"),
            "file_names": config.getoption("--file_names"),
            "run_mode": run_mode,
        }
        initial_config = helper.load_cli_initial_config(cli_config)
    elif args_mode == "json":
        json_path = config.getoption("--json_path")
        initial_config = helper.load_json_initial_config(json_path)
    else:
        LOGGER.error(f"Invalid args_mode '{args_mode}' specified. Use 'cli' or 'json'.")
        raise ValueError(f"Failed to load configuration. Invalid args_mode: {args_mode}")

    # Validate and log configuration
    helper.validate_and_log_config(initial_config)
    return initial_config


@pytest.fixture(scope="session")
def config_fixture(request):
    """
    Fixture to initialize configuration for each test function
    This fixture will be refreshed for each table and environment testing
    """
    team_config, _ = request.param
    run_mode = team_config["run_mode"]

    # Initialize the configuration using the ConfigInitializer class
    initializer = ConfigInitializer(
        team_key=team_config["team_key"],
        environment=team_config["environment"],
        detect_env_vars=team_config["detect_env_vars"],
        remote_secrets_src_type=team_config["remote_secrets_src_type"],
        remote_settings_src_type=team_config["remote_settings_src_type"]
    )

    # The config initialization happens here
    config = initializer.initialize()

    table_name = team_config["table_name"]
    config.set_settings("table_name", table_name)
    config.set_settings("run_mode", run_mode)

    # must refactor local and other run mode similar steps into helper classes
    if run_mode == "local":
        helper.prepare_for_local_run(run_mode, team_config, table_name, config)

    elif run_mode == "cicd":
        helper.prepare_for_cicd_run(run_mode, team_config, table_name, config)

    else:
        LOGGER.info(f"Unsupported run mode provided {run_mode}")

    # refactoring possibilities are high for sending the emails (to be planned in notification package)
    pytest.session_email_sender_name = str(config.get_settings('email_sender_name'))
    pytest.session_email_receiver_names = str(config.get_settings('email_receiver_names'))
    pytest.session_email_smtp_server_name = str(config.get_settings('email_smtp_server_name'))
    pytest.session_email_smtp_port = str(config.get_settings('email_smtp_port'))

    # Ensure the configuration is properly initialized
    assert config is not None, "Configuration manager should not be None"
    assert hasattr(config, "settings"), "Configuration manager should have settings attribute"

    yield config

    LOGGER.info(
        f"Tearing down config for "
        f"{team_config['team_key']} - {team_config['environment']} - {team_config['table_name']}")

    config.clear()


def pytest_sessionfinish(session, exitstatus):
    """
    Hook that is called after the test session has completed.

    Args:
        session: The pytest session object.
        exitstatus: The exit status of the test run.
    """

    # in future development, we can call reporting package here to create reports based on all results
    # example: final_report = report_mgr.create_report(all_tables_test_results)

    # in future development, we can also call notification package to notify team members by sharing reports
    # example:
    # Initialize the NotificationManager, which acts as the Subject (Publisher)
    # This will manage and notify all registered notifiers (Observers)
    # notifier_manager = NotificationManager()

    # Create instances of different notification channels (Observers)
    # Each notifier (email, slack, teams) is a concrete implementation of the Observer interface
    # email_notifier = EmailNotifier(config)  # Email notification setup
    # slack_notifier = SlackNotifier(config)  # Slack notification setup
    # teams_notifier = TeamsNotifier(config)  # Microsoft Teams notification setup

    # Register each notifier with the NotificationManager
    # This ensures they receive updates whenever a notification is sent
    # notifier_manager.register(email_notifier)
    # notifier_manager.register(slack_notifier)
    # notifier_manager.register(teams_notifier)

    # Send a notification with a message, subject, and optional attachments
    # This triggers the 'notify' method, which in turn calls 'update' on all registered notifiers
    # notifier_manager.notify(
    #     message="System Alert: Task Completed!",  # The notification message content
    #     subject="Success",  # Notification subject (optional, but useful for emails)
    #     attachments=["report.pdf"]  # List of file attachments (e.g., reports, logs)
    # )

    LOGGER.info(f"Test session finished with exit status {exitstatus}")

    # Get the logs directory from the session config
    logs_dir = session.config.logs_dir
    log_file, email_body = helper.get_log_file_and_email_body(logs_dir)

    # Email details
    sender_email = getattr(pytest, "session_email_sender_name", None)
    recipient_email = getattr(pytest, "session_email_receiver_names", None)
    email_smtp_server_name = str(getattr(pytest, "session_email_smtp_server_name", None))
    email_smtp_port = int(getattr(pytest, "session_email_smtp_port", 25))

    helper.send_logs_via_email(
        sender_email, recipient_email, email_smtp_server_name, email_smtp_port, email_body, log_file)
