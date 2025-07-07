import ast
import logging
import re
from pathlib import Path
from typing import Any, Dict, List
from pprint import pprint

import pandas as pd
import yaml
import json
from utils.common.github_util import parse_github_url, fetch_file_content
from utils.common.aws_util import get_secrets_manager_client
import argparse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class CleanYAMLDumper(yaml.SafeDumper):
    """Custom YAML dumper for cleaner output"""

    def ignore_aliases(self, data):
        """Prevent anchors/aliases in YAML output"""
        return True

    def write_literal(self, text):
        """Handle literal strings without quotes when possible"""
        hints = self.determine_block_hints(text)
        self.write_indicator('|' + hints, True)
        if hints[-1:] == '+':
            self.open_ended = True
        self.write_line_break()
        breaks = True
        start = end = 0
        while end <= len(text):
            ch = None
            if end < len(text):
                ch = text[end]
            if breaks:
                if ch is None or ch not in '\n\x85\u2028\u2029':
                    if not breaks and ch is not None and ch != ' ' and start < end:
                        self.write_indent()
                        self.write(text[start:end])
                        start = end
                    if ch is not None:
                        self.write_indent()
                    breaks = False
            else:
                if ch is None or ch in '\n\x85\u2028\u2029':
                    self.write(text[start:end])
                    if ch is not None:
                        self.write_line_break()
                    start = end
                    breaks = True
            end += 1


class YAMLConfigGenerator:
    """Generates YAML configuration files for the data verification framework"""

    def __init__(self, project_root: str = None, team_path: str = None, env: str = None, load_strategy: str = None):
        self.project_root = self._find_project_root()
        self.team_path = team_path
        self.custom_conf_path = self.project_root / "custom_conf" / "teams"
        self.env = env
        self.load_strategy = load_strategy

        if team_path:
            self.team_dir = self.custom_conf_path / team_path
            self.default_config_path = self.team_dir / "DEFAULT_SCOPE.yml"
        else:
            self.team_dir = None
            self.default_config_path = None

        self.default_config = self._load_default_config()

    def _find_project_root(self) -> Path:
        """Find the project root directory by looking for marker files"""
        current_path = Path(__file__).resolve()
        markers = ["README.md", "Dockerfile", "poetry.lock", "pyproject.toml"]

        while current_path != current_path.parent:
            if any((current_path / marker).exists() for marker in markers):
                return current_path
            current_path = current_path.parent

        return Path.cwd()

    def _load_default_config(self) -> Dict[str, Any]:
        """Load the default YAML configuration"""
        if self.default_config_path and self.default_config_path.exists():
            try:
                with open(self.default_config_path, 'r') as file:
                    return yaml.safe_load(file)
            except Exception as e:
                logger.warning(f"Failed to load default config from {self.default_config_path}: {e}")

        return self._get_default_template()

    def load_secrets(self) -> Dict[str, Any]:
        """Load secrets from AWS Secrets Manager"""
        parts = self.team_path.split('/')
        team_key = "_".join(parts)
        secret_path = f"{team_key}/{self.env}/secrets"
        response = get_secrets_manager_client().get_secret_value(SecretId=secret_path)
        secret_string = response['SecretString']

        try:
            secret_dict = json.loads(secret_string)
        except json.JSONDecodeError:
            secret_dict = {"raw_secret": secret_string}

        return secret_dict

    def _fetch_s3_mapping_from_github(self) -> Dict[str, str]:
        """Fetch S3 mapping from GitHub repository"""
        git_url = "https://github.com/SyscoCorporation/seed-eu/blob/stg/scripts/common/EU_S3_SOURCE_STRUCTURE.py"
        try:
            secret_dict = self.load_secrets()
            repo_owner, repo_name, branch, file_path = parse_github_url(git_url)
            json_content = fetch_file_content(
                repo_owner, repo_name, file_path, branch,
                secret_dict['seed-eu-git-pat'], run_mode="local"
            )
            match = re.search(r's3_path\s*=\s*({.*})', json_content, re.DOTALL)

            if match:
                dictionary_string = match.group(1)
                try:
                    s3_path_dict = ast.literal_eval(dictionary_string)
                    return s3_path_dict
                except (ValueError, SyntaxError) as e:
                    logger.error(f"Error evaluating dictionary string: {e}")
            else:
                logger.warning("Could not find 's3_path = { ... }' pattern in the fetched content.")
                return {}

        except Exception as e:
            logger.error(f"Failed to fetch S3 mapping from GitHub: {e}")
            raise

    def _get_default_template(self) -> Dict[str, Any]:
        """Return a basic default template if config file is not available"""
        return {
            "test_scope": {
                "local": {
                    "source": {
                        "data_validation": ["rule_checks"],
                        "data_quality": ["completeness", "duplication"]
                    },
                    "target_lndp": {
                        "data_validation": ["rule_checks"],
                        "data_quality": ["timeliness", "completeness", "duplication", "consistency", "accuracy"]
                    },
                    "target_edwp": {
                        "data_validation": ["rule_checks"],
                        "data_quality": ["timeliness", "completeness", "duplication", "consistency", "accuracy"]
                    }
                }
            }
        }

    def read_design_document(self, file_path: str, sheet_name: str = None) -> pd.DataFrame:
        """Read design document from Excel or CSV file"""
        try:
            file_ext = Path(file_path).suffix.lower()
            if file_ext in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path, sheet_name=sheet_name) if sheet_name else pd.read_excel(file_path)
            elif file_ext == '.csv':
                df = pd.read_csv(file_path)
            else:
                raise ValueError(f"Unsupported file format: {file_ext}")

            logger.info(f"Successfully read design document: {file_path}")
            return df

        except Exception as e:
            logger.error(f"Failed to read design document: {e}")
            raise

    def find_header_row(self, df: pd.DataFrame) -> int:
        """Find the row index that contains the actual column headers"""
        expected_headers = [
            'source table/file name', 'lndp table name', 'edwp table name',
            'lndp column name', 'edwp column name', 'lndp column type', 'edwp data type'
        ]

        for idx, row in df.iterrows():
            row_values = [str(val).lower().strip() if pd.notna(val) else "" for val in row]
            header_matches = sum(1 for header in expected_headers if any(header in cell for cell in row_values))

            if header_matches >= 3:
                logger.debug(f"Found header row at index: {idx}")
                return idx

        logger.debug("Using fallback header row at index: 3")
        return 3  # Fallback to row 3

    def _format_column_entry(self, column_name: str, column_type: str, layer: str) -> str:
        """Format column entry without quotes in YAML"""
        return f"{column_name} {column_type} {layer}"

    def _create_expected_columns(self, lndp_columns_dict: Dict[str, str],
                                 edwp_columns_dict: Dict[str, str],
                                 audit_columns_set: set) -> List[str]:
        """Create expected columns list with proper formatting"""
        expected_columns = []

        # Process LNDP columns
        for lndp_col, lndp_type in lndp_columns_dict.items():
            if lndp_col in audit_columns_set:
                continue

            if lndp_col in edwp_columns_dict:
                edwp_type = edwp_columns_dict[lndp_col]
                if lndp_type == edwp_type:
                    expected_columns.append(self._format_column_entry(lndp_col, lndp_type, "both"))
                else:
                    expected_columns.append(self._format_column_entry(lndp_col, lndp_type, "only_lndp"))
                    expected_columns.append(self._format_column_entry(lndp_col, edwp_type, "only_edwp"))
            else:
                expected_columns.append(self._format_column_entry(lndp_col, lndp_type, "only_lndp"))

        # Add EDWP-only columns
        for edwp_col, edwp_type in edwp_columns_dict.items():
            if edwp_col in audit_columns_set or edwp_col in lndp_columns_dict:
                continue
            expected_columns.append(self._format_column_entry(edwp_col, edwp_type, "only_edwp"))

        return expected_columns

    def _create_unique_columns(self, key_columns: List[str]) -> List[str]:
        """Create unique columns list with proper formatting"""
        unique_columns = []
        for key_col in key_columns:
            # Remove layer identifier for unique columns
            clean_key = key_col.rsplit(' ', 1)[0] if ' ' in key_col else key_col
            layer_info = key_col.rsplit(' ', 1)[1] if ' ' in key_col else "both"
            unique_columns.append(f"{clean_key} {layer_info}")
        return unique_columns

    def extract_table_info(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Extract table information from design document DataFrame"""
        table_info = {}

        # Extract metadata from top rows
        metadata_fields = {
            "script file path": "script_file_path",
            "ddl file path": "ddl_file_path",
            "dependent scripts": "dependent_scripts"
        }

        for idx, row in df.head(10).iterrows():
            first_col_value = str(row.iloc[0]).lower() if pd.notna(row.iloc[0]) else ""
            second_col_value = str(row.iloc[1]) if pd.notna(row.iloc[1]) else ""

            for search_term, field_name in metadata_fields.items():
                if search_term in first_col_value:
                    table_info[field_name] = second_col_value
                    break

        # Find header row and process data
        header_row_idx = self.find_header_row(df)

        df.columns = df.iloc[header_row_idx]
        data_df = df.iloc[header_row_idx + 1:].reset_index(drop=True).dropna(how='all')

        # Column mapping for standardization
        column_mapping = {
            'Source Table/File Name': 'table_name',
            'LNDP Table Name': 'lndp_table_name',
            'EDWP Table Name': 'edwp_table_name',
            'Lndp column name': 'lndp_column_name',
            'Lndp column type': 'lndp_column_type',
            'EDWP column name': 'edwp_column_name',
            'EDWP Data Type': 'edwp_data_type',
            'EDWP Column Type': 'edwp_column_type',
            'Mandatory/Not Null': 'mandatory_type'
        }

        for old_col, new_col in column_mapping.items():
            if old_col in data_df.columns:
                data_df = data_df.rename(columns={old_col: new_col})

        # Extract table names
        table_fields = ['table_identifier', 'lndp_table_name', 'edwp_table_name']
        column_fields = ['table_name', 'lndp_table_name', 'edwp_table_name']

        for table_field, column_field in zip(table_fields, column_fields):
            table_info[table_field] = ""
            if column_field in data_df.columns:
                for _, row in data_df.iterrows():
                    if pd.notna(row[column_field]) and row[column_field]:
                        table_info[table_field] = row[column_field]
                        break

        # Get S3 URI
        try:
            s3_mapping = self._fetch_s3_mapping_from_github()
            table_name = (table_info.get('lndp_table_name', '').upper().split(".")[1]
                          if table_info.get('lndp_table_name') and '.' in table_info.get('lndp_table_name', '')
                          else "")
            table_info['source_uri'] = s3_mapping.get(table_name, "")
        except Exception as e:
            logger.warning(f"Could not fetch S3 mapping: {e}")
            table_info['source_uri'] = ""

        table_info['load_strategy'] = self.load_strategy or "truncate_load"

        # Process columns
        self._process_columns(data_df, table_info)

        return table_info

    def _process_columns(self, data_df: pd.DataFrame, table_info: Dict[str, Any]):
        """Process column information from the DataFrame"""
        # Initialize column tracking structures
        column_categories = [
            'major_columns', 'minor_columns', 'key_columns', 'audit_columns',
            'nkey_columns', 'skey_columns', 'mandatory_columns', 'not_null_columns'
        ]
        for category in column_categories:
            table_info[category] = []

        lndp_columns_dict = {}
        edwp_columns_dict = {}

        # Process each row
        for _, row in data_df.iterrows():
            lndp_col = row.get('lndp_column_name', '') if pd.notna(row.get('lndp_column_name')) else ""
            lndp_type = row.get('lndp_column_type', '') if pd.notna(row.get('lndp_column_type')) else ""
            edwp_col = row.get('edwp_column_name', '') if pd.notna(row.get('edwp_column_name')) else ""
            edwp_type = row.get('edwp_data_type', '') if pd.notna(row.get('edwp_data_type')) else ""

            if (not lndp_col or not lndp_type) and (not edwp_col or not edwp_type):
                continue

            # Store column information
            if lndp_col and lndp_type:
                lndp_columns_dict[lndp_col] = lndp_type
            if edwp_col and edwp_type:
                edwp_columns_dict[edwp_col] = edwp_type

            # Categorize by EDWP Column Type
            self._categorize_column(row, edwp_col, lndp_col, lndp_type, edwp_col, edwp_type, table_info)

        # Create formatted column lists
        audit_columns_set = set(table_info.get('audit_columns', []))
        table_info['expected_columns'] = self._create_expected_columns(
            lndp_columns_dict, edwp_columns_dict, audit_columns_set
        )
        table_info['unique_columns'] = self._create_unique_columns(table_info.get('key_columns', []))

    def _categorize_column(self, row, edwp_col: str, lndp_col: str, lndp_type: str,
                           edwp_col_name: str, edwp_type: str, table_info: Dict[str, Any]):
        """Categorize columns based on their type and properties"""
        edwp_column_type = row.get('edwp_column_type', '')
        if pd.notna(edwp_column_type) and edwp_col:
            edwp_column_type_lower = str(edwp_column_type).lower()

            category_map = {
                'major': 'major_columns',
                'minor': 'minor_columns',
                'audit': 'audit_columns',
                'nkey': 'nkey_columns',
                'skey': 'skey_columns'
            }

            if edwp_column_type_lower in category_map:
                table_info[category_map[edwp_column_type_lower]].append(edwp_col)
            elif edwp_column_type_lower == 'key':
                key_identifier = "both"
                if not lndp_col or not lndp_type:
                    key_identifier = "only_edwp"
                elif not edwp_col_name or not edwp_type:
                    key_identifier = "only_lndp"
                table_info['key_columns'].append(f"{edwp_col} {key_identifier}")

        # Handle mandatory/null flags
        mandatory_flag = row.get('mandatory_type', '')
        if pd.notna(mandatory_flag) and edwp_col:
            mandatory_flag_lower = str(mandatory_flag).lower()
            if mandatory_flag_lower == 'mandatory':
                table_info['mandatory_columns'].append(edwp_col)
            elif mandatory_flag_lower == 'not null':
                table_info['not_null_columns'].append(edwp_col)

    def generate_yaml_config(self, table_info: Dict[str, Any]) -> Dict[str, Any]:
        """Generate YAML configuration from extracted table information"""
        config = {}

        # AWS Redshift configuration
        self._build_aws_config(config, table_info)

        # Columns information
        self._build_columns_config(config, table_info)

        # Metadata
        self._build_metadata_config(config, table_info)

        # SCD configuration
        if self.load_strategy == 'scd':
            self._build_scd_config(config, table_info)

        # Test scope
        self._build_test_scope_config(config)

        # Test information
        self._build_test_info_config(config, table_info)

        # Trigger counter
        config['trigger_counter'] = 1

        return config

    def _build_aws_config(self, config: Dict[str, Any], table_info: Dict[str, Any]):
        """Build AWS Redshift configuration section"""
        # Parse table names
        lndp_full_name = table_info.get('lndp_table_name', 'ts_eu_pgm_lndp.tables_lndp_name')
        edwp_full_name = table_info.get('edwp_table_name', 'ts_eu_pgm_edwp.tables_edwp_name')

        lndp_schema, lndp_table = lndp_full_name.split('.', 1) if '.' in lndp_full_name else (
        'ts_eu_pgm_lndp', lndp_full_name)
        edwp_schema, edwp_table = edwp_full_name.split('.', 1) if '.' in edwp_full_name else (
        'ts_eu_pgm_edwp', edwp_full_name)

        config['aws_redshift_sqlalchemy_db'] = {
            'source': {
                'uri': table_info.get('source_uri', 'provide/s3/uri/to/source/flat-file/')
            },
            'target': {
                'lndp': {
                    'table_name': lndp_table,
                    'schema_name': lndp_schema
                },
                'edwp': {
                    'table_name': edwp_table,
                    'schema_name': edwp_schema
                }
            }
        }

        # Add history table for truncate_load
        if self.load_strategy == 'truncate_load':
            history_table_name = self._generate_history_table_name(edwp_table)
            config['aws_redshift_sqlalchemy_db']['target']['edwp']['history_table_name'] = history_table_name

    def _generate_history_table_name(self, edwp_table: str) -> str:
        """Generate history table name based on EDWP table name"""
        if edwp_table.endswith('_fact'):
            return edwp_table.replace('_fact', '_hist_fact')
        elif edwp_table.endswith('_dim'):
            return edwp_table.replace('_dim', '_hist_dim')
        else:
            return f"{edwp_table}_hist"

    def _build_columns_config(self, config: Dict[str, Any], table_info: Dict[str, Any]):
        """Build columns information configuration section"""
        config['columns_info'] = {
            'expected_columns': table_info.get('expected_columns', []),
            'unique_columns': table_info.get('unique_columns', [])
        }

        if table_info.get('null_columns'):
            config['columns_info']['null_columns'] = table_info['null_columns']

    def _build_metadata_config(self, config: Dict[str, Any], table_info: Dict[str, Any]):
        """Build metadata configuration section"""
        metadata_fields = ['script_file_path', 'ddl_file_path', 'dependent_scripts']
        metadata = {}

        for field in metadata_fields:
            if table_info.get(field):
                if field == 'dependent_scripts':
                    metadata[field] = [script.strip() for script in table_info[field].split(',') if script.strip()]
                else:
                    metadata[field] = table_info[field]

        if metadata:
            config['metadata'] = metadata

    def _build_scd_config(self, config: Dict[str, Any], table_info: Dict[str, Any]):
        """Build SCD configuration section"""
        config['scd_info'] = {
            'enable_scd_validations': True,
            'has_opco': False,
            'dt_prefix': 'itm_rec'
        }

        # Extract business keys
        if table_info.get('key_columns'):
            business_keys = []
            for key_col in table_info['key_columns']:
                clean_key = key_col.rsplit(' ', 1)[0] if ' ' in key_col else key_col
                business_keys.append(clean_key)
            config['scd_info']['business_keys'] = business_keys

        # Add major and minor columns
        for col_type in ['major_columns', 'minor_columns']:
            if table_info.get(col_type):
                config['scd_info'][col_type] = table_info[col_type]

        config['scd_info']['lndp_to_edwp_sqls'] = ""

    def _build_test_scope_config(self, config: Dict[str, Any]):
        """Build test scope configuration section"""
        if hasattr(self, 'default_config') and 'test_scope' in self.default_config:
            import copy
            config['test_scope'] = copy.deepcopy(self.default_config['test_scope'])

            # Modify based on load strategy
            self._modify_test_scope_by_strategy(config['test_scope'])
        else:
            config['test_scope'] = self._get_fallback_test_scope(self.load_strategy)

    def _modify_test_scope_by_strategy(self, test_scope: Dict[str, Any]):
        """Modify test scope based on load strategy"""
        if self.load_strategy == 'scd':
            for scope_name in ['local', 'cicd', 'etl']:
                if scope_name in test_scope and 'target_edwp' in test_scope[scope_name]:
                    if 'data_validation' in test_scope[scope_name]['target_edwp']:
                        if 'scd_checks' not in test_scope[scope_name]['target_edwp']['data_validation']:
                            test_scope[scope_name]['target_edwp']['data_validation'].append('scd_checks')

        elif self.load_strategy == 'truncate_load':
            for scope_name in ['cicd', 'etl']:
                if (scope_name in test_scope and 'target_edwp' in test_scope[scope_name]
                        and 'data_quality' in test_scope[scope_name]['target_edwp']):
                    if 'history_validation' not in test_scope[scope_name]['target_edwp']['data_quality']:
                        test_scope[scope_name]['target_edwp']['data_quality'].append('history_validation')

    def _build_test_info_config(self, config: Dict[str, Any], table_info: Dict[str, Any]):
        """Build test information configuration section"""
        config['test_info'] = {
            'table_identifier': table_info.get('table_identifier') or table_info.get('edwp_table_name',
                                                                                     'unknown_table'),
            'load_strategy': self.load_strategy
        }

        if self.load_strategy == 'scd':
            config['test_info']['use_synthetic_data'] = True

    def _get_fallback_test_scope(self, load_strategy: str) -> Dict[str, Any]:
        """Get fallback test scope configuration"""
        base_test_scope = {
            'local': {
                'source': {
                    'data_validation': ['rule_checks'],
                    'data_quality': ['timeliness', 'completeness', 'duplication', 'consistency', 'accuracy']
                },
                'target_lndp': {
                    'data_validation': ['rule_checks'],
                    'data_quality': ['timeliness', 'completeness', 'duplication', 'consistency', 'accuracy']
                },
                'target_edwp': {
                    'data_validation': ['rule_checks'],
                    'data_quality': ['timeliness', 'completeness']
                }
            }
        }

        # Add additional scopes
        for scope_name in ['cicd', 'etl']:
            base_test_scope[scope_name] = {
                'source': base_test_scope['local']['source'].copy(),
                'target_lndp': base_test_scope['local']['target_lndp'].copy(),
                'target_edwp': {
                    'data_validation': ['rule_checks'],
                    'data_quality': ['timeliness', 'completeness', 'duplication', 'consistency', 'accuracy',
                                     'history_validation']
                }
            }

        if load_strategy == 'scd':
            for scope in base_test_scope.values():
                if 'target_edwp' in scope:
                    scope['target_edwp']['data_validation'].append('scd_checks')

        return base_test_scope

    def save_yaml_config(self, config: Dict[str, Any], table_name: str) -> str:
        """Save YAML configuration to project team directory"""
        if not self.team_dir:
            raise ValueError("Team path not specified. Cannot determine output directory.")

        try:
            self.team_dir.mkdir(parents=True, exist_ok=True)
            output_file = self.team_dir / f"{table_name}.yaml"

            with open(output_file, 'w') as file:
                yaml.dump(
                    config,
                    file,
                    default_flow_style=False,
                    sort_keys=False,
                    indent=2,
                    Dumper=CleanYAMLDumper,
                    allow_unicode=True
                )

            # Post-process to remove quotes from expected_columns and unique_columns
            self._clean_yaml_quotes(output_file)

            logger.info(f"YAML configuration saved to: {output_file}")
            return str(output_file)

        except Exception as e:
            logger.error(f"Failed to save YAML configuration: {e}")
            raise

    def _clean_yaml_quotes(self, file_path: str):
        """Remove unnecessary quotes from YAML file"""
        with open(file_path, 'r') as file:
            content = file.read()

        lines = content.split('\n')
        in_expected_columns = False
        in_unique_columns = False

        for i, line in enumerate(lines):
            stripped = line.strip()

            # Track which section we're in
            if stripped == 'expected_columns:':
                in_expected_columns = True
                in_unique_columns = False
            elif stripped == 'unique_columns:':
                in_unique_columns = True
                in_expected_columns = False
            elif stripped and not stripped.startswith('-') and not stripped.startswith(' '):
                in_expected_columns = False
                in_unique_columns = False

            # Remove quotes from list items in these sections
            if (in_expected_columns or in_unique_columns) and stripped.startswith('- '):
                # Remove surrounding quotes but keep the content
                if stripped.startswith("- '") and stripped.endswith("'"):
                    lines[i] = line.replace("- '", "- ").replace("'", "", 1)  # Remove only the first quote from the end
                elif stripped.startswith('- "') and stripped.endswith('"'):
                    lines[i] = line.replace('- "', "- ").replace('"', "", 1)  # Remove only the first quote from the end

        with open(file_path, 'w') as file:
            file.write('\n'.join(lines))

    def generate_from_design_doc(self, design_doc_path: str, sheet_name: str = None) -> tuple[str, Dict[str, Any]]:
        """
        Complete workflow to generate YAML from design document

        Args:
            design_doc_path: Path to design document
            sheet_name: Excel sheet name (optional)

        Returns:
            Tuple of (Path to generated YAML file, table_info dictionary)
        """
        try:
            # Read design document
            df = self.read_design_document(design_doc_path, sheet_name)

            # Extract table information
            table_info = self.extract_table_info(df)

            # Generate YAML configuration
            config = self.generate_yaml_config(table_info)

            # Determine table name for output file
            table_name = self._extract_table_name(table_info)

            # Save to team directory
            output_path = self.save_yaml_config(config, table_name)
            logger.info("✅ YAML generation completed successfully!")
            return output_path, table_info

        except Exception as e:
            logger.error(f"YAML generation failed: {e}")
            raise

    def _extract_table_name(self, table_info: Dict[str, Any]) -> str:
        """Extract table name for output file naming"""
        lndp_table_name = table_info.get('lndp_table_name', '')
        if lndp_table_name and '.' in lndp_table_name:
            return lndp_table_name.split('.')[1]
        return table_info.get('table_identifier', 'unknown_table')

    def set_team_path(self, team_path: str):
        """
        Set or change the team path

        Args:
            team_path: Team path within custom_conf/teams (e.g., 'seed/intl/pgm')
        """
        self.team_path = team_path
        self.team_dir = self.custom_conf_path / team_path
        self.default_config_path = self.team_dir / "DEFAULT_SCOPE.yml"
        self.default_config = self._load_default_config()


def main():
    """Main function for command line usage"""
    parser = argparse.ArgumentParser(
        description='Generate YAML configuration from design document',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  python script.py --design_doc design_doc.xlsx --team_path seed/intl/pgm --env stg --load_strategy truncate_load

  # With specific sheet
  python script.py --design_doc design_doc.xlsx --team_path seed/intl/pgm --env prod --load_strategy scd --sheet_name "Sheet1"

  # SCD strategy
  python script.py --design_doc design_doc.xlsx --team_path seed/intl/pgm --env dev --load_strategy scd
        """
    )

    parser.add_argument(
        '--design_doc',
        required=True,
        help='Path to design document (Excel/CSV file)'
    )
    parser.add_argument(
        '--team_path',
        required=True,
        help='Team path within custom_conf/teams (e.g., seed/intl/pgm)'
    )
    parser.add_argument(
        '--env',
        required=True,
        choices=['stg', 'prod'],
        help='Environment (stg, prod)'
    )
    parser.add_argument(
        '--load_strategy',
        required=True,
        choices=['truncate_load', 'scd'],
        help='Strategy type (truncate_load, scd)'
    )
    parser.add_argument(
        '--sheet_name',
        help='Excel sheet name (optional, uses first sheet if not specified)'
    )

    args = parser.parse_args()

    # Validate design document path
    design_doc_path = Path(args.design_doc)
    if not design_doc_path.exists():
        print(f"❌ Error: Design document not found: {design_doc_path}")
        return 1

    if design_doc_path.suffix.lower() not in ['.xlsx', '.xls', '.csv']:
        print(f"❌ Error: Unsupported file format: {design_doc_path.suffix}")
        print("Supported formats: .xlsx, .xls, .csv")
        return 1

    try:
        """
        --design_doc "C://Users//Janith Algewatta//Downloads//DT-Framework-Template-for-Design-Truncate.xlsx" --team_path seed/intl/pgm --env stg --load_strategy truncate_load
        --design_doc "C://Users//Janith Algewatta//Downloads//DT-Framework-Template-for-Design-SCD.xlsx" --team_path seed/intl/pgm --env stg --load_strategy scd
        """
        print(f"🚀 Starting YAML generation...")
        print(f"📁 Design Document: {design_doc_path.name}")
        print(f"🏢 Team Path: {args.team_path}")
        print(f"🌍 Environment: {args.env}")
        print(f"🔧 Strategy: {args.load_strategy}")
        print("-" * 50)

        # Initialize generator
        generator = YAMLConfigGenerator(
            team_path=args.team_path,
            env=args.env,
            load_strategy=args.load_strategy
        )

        # Generate YAML from design document
        output_path, table_info = generator.generate_from_design_doc(
            design_doc_path,
            sheet_name=args.sheet_name
        )

        # Extract table name from the already processed table_info
        table_name = generator._extract_table_name(table_info)

        print("-" * 50)
        print(f"✅ Successfully generated YAML configuration!")
        print(f"📄 Table: {table_name}")
        print(f"💾 Output: {output_path}")

    except Exception as e:
        print(f"❌ Error: {e}")
        logger.exception("Detailed error information:")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())