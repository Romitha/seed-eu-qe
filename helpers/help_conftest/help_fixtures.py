import copy
import sys

from utils.common.email_util import prepare_log_email, send_email_via_smtp
from utils.common.file_util import file_exists_in_path, load_yaml_file_in_path
from utils.common.json_util import dump_json_data
from utils.framework.custom_conf_util import apply_overlay_to_default_yaml
from utils.framework.custom_flags_util import (load_json_configuration,
                                               process_file_names,
                                               validate_team_config)
from utils.framework.custom_logger_util import get_logger, setup_logging
from utils.framework.custom_path_util import (get_cloud_data_checks_team_path,
                                              get_framework_root_path,
                                              get_team_folder_path_with_key,
                                              get_teams_root_folder_path)
from utils.framework.custom_s3_util import (read_default_scope_yml_from_s3,
                                            read_table_yaml_from_s3)

LOGGER = get_logger()


class ConftestHelper:
    def __init__(self):
        """
        Initialization can take place in future developments
        """

    @staticmethod
    def load_cli_initial_config(cli_config):
        initial_config = {
            "teams": [
                {
                    "run_mode": cli_config["run_mode"],
                    "test_environments": (
                        cli_config["test_environments"].split(",")
                        if cli_config["test_environments"]
                        else []
                    ),
                    "detect_env_vars": cli_config["detect_env_vars"],
                    "remote_secrets_src_type": cli_config["remote_secrets_src_type"],
                    "remote_settings_src_type": cli_config["remote_settings_src_type"],
                    "file_names": cli_config["file_names"].split(",")
                }
            ]
        }
        LOGGER.info("Configuration initialized using CLI flags")
        LOGGER.debug(initial_config)
        return initial_config

    @staticmethod
    def load_json_initial_config(json_path):
        if not json_path:
            raise ValueError("In 'json' mode, --json_path is required.")

        LOGGER.info(f"Using JSON configuration from: {json_path}")
        initial_config = load_json_configuration(json_path)
        LOGGER.info("Configuration initialized using JSON file for input parameters.")
        LOGGER.debug(initial_config)
        return initial_config

    @staticmethod
    def validate_and_log_config(initial_config):
        for team_config in initial_config.get("teams", []):
            validate_team_config(team_config)

        dumped_data = dump_json_data(initial_config, 4)
        LOGGER.debug(dumped_data)

    @staticmethod
    def prepare_for_local_run(run_mode, team_config, table_name, config):
        # Load the YAML file from the local path
        LOGGER.info(f"Reading local YAML configuration for team: {team_config['team_key']}")
        LOGGER.info(f"Reading with table name -> '{table_name} table' ")
        base_path = get_teams_root_folder_path()
        team_path = get_team_folder_path_with_key(base_path, team_config["team_key"])
        table_config_path = team_path / f"{table_name}.yaml"
        default_scope_path = team_path / "DEFAULT_SCOPE.yml"

        if file_exists_in_path(table_config_path):
            table_config = load_yaml_file_in_path(table_config_path)
            table_config['test_scope'] = table_config['test_scope'][run_mode]
            config.settings[table_name] = table_config
        else:
            raise FileNotFoundError(f"Table YAML configuration file not found: {table_config_path}")

        if file_exists_in_path(default_scope_path):
            white_list_scope = load_yaml_file_in_path(default_scope_path)
            white_list_scope['test_scope'] = white_list_scope['test_scope'][run_mode]
            black_list_scope = table_config
            final_white_list_scope = apply_overlay_to_default_yaml(white_list_scope, black_list_scope)
            config.settings[table_name] = final_white_list_scope
        else:
            raise FileNotFoundError(f"Default scope YAML configuration file not found: {default_scope_path}")

    @staticmethod
    def prepare_for_cicd_run(run_mode, team_config, table_name, config):
        # Load the YAML file from S3 in CICD
        LOGGER.info(f"Reading YAML configuration from S3 for team: {team_config['team_key']} - {table_name} table")
        aws_yaml_bucket = config.settings['aws_yaml_bucket_name']
        aws_s3_path = get_cloud_data_checks_team_path(team_config["team_key"])

        LOGGER.debug(f"Looking for YAML files in the following S3 bucket: {aws_yaml_bucket}")
        table_config = read_table_yaml_from_s3(aws_s3_path, table_name, aws_yaml_bucket)
        table_config['test_scope'] = table_config['test_scope'][run_mode]
        config.settings[table_name] = table_config
        white_list_scope = read_default_scope_yml_from_s3(aws_s3_path, "DEFAULT_SCOPE", aws_yaml_bucket)
        white_list_scope['test_scope'] = white_list_scope['test_scope'][run_mode]
        black_list_scope = table_config
        final_white_list_scope = apply_overlay_to_default_yaml(white_list_scope, black_list_scope)
        config.settings[table_name] = final_white_list_scope

    @staticmethod
    def prepare_for_etl_run():
        """
        NEED to refactor and add ETL mode related logics here
        """
        pass

    @staticmethod
    def fix_any_path_issue_before_run():
        # Add the project root to sys.path for module resolution
        sys.path.insert(0, str(get_framework_root_path()))
        LOGGER.info(f"Framework root path added to sys.path: {get_framework_root_path()}")

    @staticmethod
    def get_param_combination(config):

        param_combinations = []

        for team in config.get("teams", []):
            # Process file names to get tables
            processed_files = process_file_names(team["file_names"])
            LOGGER.debug(f"Team to tables mapped dictionary (placeholder values) {processed_files}")

            for team_key, tables in processed_files.items():
                team["team_key"] = team_key
                for table_name, key_names in tables.items():
                    for env in team["test_environments"]:
                        # Use deep copy to avoid unintended changes across iterations
                        updated_team = copy.deepcopy(team)
                        updated_team["table_name"] = table_name
                        updated_team["environment"] = env

                        # Add to param_combinations
                        param_combinations.append((updated_team, table_name))

        # Log the generated param combinations for debugging
        LOGGER.debug(f"Generated parameter combinations: {param_combinations}")

        return param_combinations

    @staticmethod
    def get_logger():
        return LOGGER

    @staticmethod
    def initiate_setup_config(config):
        setup_logging(config)

    @staticmethod
    def get_log_file_and_email_body(logs_dir):
        return prepare_log_email(logs_dir)

    @staticmethod
    def send_logs_via_email(
            sender_email, recipient_email, email_smtp_server_name, email_smtp_port, email_body, log_file):

        # Split the recipient email if it's a comma-separated string
        if recipient_email and isinstance(recipient_email, str):
            recipient_email = [email.strip() for email in recipient_email.split(",")]

        if not sender_email or not recipient_email:
            LOGGER.error("Sender or recipient email(s) not set. Cannot send logs.")
            return

        subject = "Pytest Test Session Logs"

        try:
            send_email_via_smtp(
                sender_email, recipient_email, subject, email_body,
                email_smtp_server_name, email_smtp_port, log_file
            )
        except Exception as e:
            LOGGER.error(f"Failed to send email with logs: {e}")
        else:
            LOGGER.info(f"Test logs sent to {', '.join(recipient_email)}")
