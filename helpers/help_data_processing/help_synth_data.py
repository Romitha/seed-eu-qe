from connection.connection_manager import ConnectionManager
from utils.common.synthetic_data_util import (
    generate_synthetic_data, generate_table_schema_from_columns,
    insert_synthetic_data)
from utils.framework.custom_logger_util import get_logger

LOGGER = get_logger()


class SynthDataHelper:
    def __init__(self, config):
        """
        Initialize the SynthDataHelper with the provided configuration.

        Args:
            config (dict): Configuration dictionary.
        """
        self.config = config
        self.connection_mgr = ConnectionManager()

        self.source_bucket = config.get('source_data_bucket_name')
        self.run_mode = config.get("run_mode")
        self.table_name = config.get("table_name")
        self.table_settings = config.get(self.table_name, {})
        self.connection_system = next(iter(self.table_settings), None)
        self.column_info = self.table_settings.get("columns_info", {})
        self.expected_columns = self.column_info.get("expected_columns", {})
        self.conf_synthetic_data = self.table_settings.get("synthetic_data", {})
        self.confirm_synth_data_gen = None

    def get_appropriate_columns_for_layer(self, test_layer, layer_settings):
        """
        Get the appropriate columns for synthetic data generation based on mapping and target layer.
        Intelligently handles type conversion constraints by using the more restrictive length
        while respecting the target data type.

        Args:
            test_layer (str): The target layer (lndp, edwp)
            layer_settings (dict): Layer settings containing mapping information

        Returns:
            list: List of column definitions appropriate for the target layer
        """
        has_mapping = layer_settings.get('has_mapping', False)

        if has_mapping and 'mapped_expected_cols' in layer_settings:
            # Get the mapping configuration
            column_mapping = self.column_info.get('column_mapping', {})
            lndp_edwp_mapping = column_mapping.get('lndp_edwp', [])

            if lndp_edwp_mapping and test_layer == 'lndp':
                # Create optimized columns for LNDP that respect both source and target constraints
                return self._create_optimized_lndp_columns(lndp_edwp_mapping)
            elif test_layer == 'edwp':
                # For EDWP layer, use the mapped columns
                return layer_settings['mapped_expected_cols']
            else:
                return self.expected_columns
        else:
            # No mapping, use expected columns
            return self.expected_columns

    def _create_optimized_lndp_columns(self, lndp_edwp_mapping):
        """
        Create optimized column definitions for LNDP that consider both source and target constraints.

        Args:
            lndp_edwp_mapping (list): List of mapping dictionaries

        Returns:
            list: Optimized column definitions for synthetic data generation
        """
        import re

        # Create a mapping dictionary from source to target
        mapping_dict = {}
        for item in lndp_edwp_mapping:
            if isinstance(item, dict):
                mapping_dict.update(item)

        optimized_columns = []

        for source_col_def in self.expected_columns:
            # Parse source column definition
            source_parts = source_col_def.split(' ', 1)
            if len(source_parts) < 2:
                optimized_columns.append(source_col_def)
                continue

            source_col_name = source_parts[0]
            source_col_type = source_parts[1]

            # Check if this column has a mapping
            if source_col_def in mapping_dict:
                target_col_def = mapping_dict[source_col_def]
                target_parts = target_col_def.split(' ', 1)

                if len(target_parts) >= 2:
                    target_col_type = target_parts[1]

                    # Apply intelligent constraint resolution
                    optimized_type = self._resolve_column_constraints(
                        source_col_name, source_col_type, target_col_type
                    )
                    optimized_columns.append(f"{source_col_name} {optimized_type}")
                else:
                    optimized_columns.append(source_col_def)
            else:
                optimized_columns.append(source_col_def)

        LOGGER.debug(f"Optimized columns for LNDP: {optimized_columns}")
        return optimized_columns

    def _resolve_column_constraints(self, col_name, source_type, target_type):
        """
        Resolve column constraints by comparing source and target types and choosing
        the more restrictive constraint while respecting the target data type.

        Args:
            col_name (str): Column name
            source_type (str): Source column type (e.g., "VARCHAR(10)")
            target_type (str): Target column type (e.g., "NUMERIC(11,6)")

        Returns:
            str: Optimized column type for synthetic data generation
        """
        import re

        LOGGER.debug(f"Resolving constraints for {col_name}: {source_type} -> {target_type}")

        # Extract source constraints
        source_varchar_match = re.search(r'VARCHAR\((\d+)\)', source_type, re.IGNORECASE)
        source_numeric_match = re.search(r'NUMERIC\((\d+),\s*(\d+)\)', source_type, re.IGNORECASE)

        # Extract target constraints
        target_varchar_match = re.search(r'VARCHAR\((\d+)\)', target_type, re.IGNORECASE)
        target_numeric_match = re.search(r'NUMERIC\((\d+),\s*(\d+)\)', target_type, re.IGNORECASE)

        # Case 1: VARCHAR -> NUMERIC conversion
        if source_varchar_match and target_numeric_match:
            source_length = int(source_varchar_match.group(1))
            target_precision = int(target_numeric_match.group(1))
            target_scale = int(target_numeric_match.group(2))

            # Calculate effective numeric length (precision - scale for integer part + 1 for decimal point + scale)
            # But we need to consider the VARCHAR constraint as well
            effective_target_length = target_precision - target_scale + (1 if target_scale > 0 else 0) + target_scale

            # Use the more restrictive constraint
            if source_length < effective_target_length:
                # Source VARCHAR is more restrictive, but we want numeric data
                # Generate numeric with precision that fits in VARCHAR length
                max_integer_digits = max(1, source_length - target_scale - (1 if target_scale > 0 else 0))
                safe_precision = max_integer_digits + target_scale
                result_type = f"NUMERIC({safe_precision},{target_scale})"
            else:
                # Target numeric constraint is fine
                result_type = target_type

            LOGGER.debug(
                f"VARCHAR({source_length}) -> NUMERIC({target_precision},{target_scale}) resolved to: {result_type}")
            return result_type

        # Case 2: NUMERIC -> VARCHAR conversion
        elif source_numeric_match and target_varchar_match:
            source_precision = int(source_numeric_match.group(1))
            source_scale = int(source_numeric_match.group(2))
            target_length = int(target_varchar_match.group(1))

            # Calculate required length for the numeric value as string
            required_length = source_precision + (1 if source_scale > 0 else 0)  # +1 for decimal point

            if target_length < required_length:
                # Target VARCHAR is more restrictive
                # Adjust numeric precision to fit in VARCHAR
                max_precision = max(1, target_length - (1 if source_scale > 0 else 0))
                safe_precision = max_precision - source_scale if max_precision > source_scale else 1
                result_type = f"NUMERIC({safe_precision + source_scale},{source_scale})"
            else:
                # Source numeric is fine
                result_type = source_type

            LOGGER.debug(
                f"NUMERIC({source_precision},{source_scale}) -> VARCHAR({target_length}) resolved to: {result_type}")
            return result_type

        # Case 3: VARCHAR -> VARCHAR (choose smaller length)
        elif source_varchar_match and target_varchar_match:
            source_length = int(source_varchar_match.group(1))
            target_length = int(target_varchar_match.group(1))
            min_length = min(source_length, target_length)
            result_type = f"VARCHAR({min_length})"
            LOGGER.debug(f"VARCHAR({source_length}) -> VARCHAR({target_length}) resolved to: {result_type}")
            return result_type

        # Case 4: NUMERIC -> NUMERIC (choose more restrictive)
        elif source_numeric_match and target_numeric_match:
            source_precision = int(source_numeric_match.group(1))
            source_scale = int(source_numeric_match.group(2))
            target_precision = int(target_numeric_match.group(1))
            target_scale = int(target_numeric_match.group(2))

            # Use the more restrictive precision and scale
            min_precision = min(source_precision, target_precision)
            min_scale = min(source_scale, target_scale)
            result_type = f"NUMERIC({min_precision},{min_scale})"
            LOGGER.debug(
                f"NUMERIC({source_precision},{source_scale}) -> NUMERIC({target_precision},{target_scale}) resolved to: {result_type}")
            return result_type

        # Default case: no specific resolution needed, use target type
        LOGGER.debug(f"No specific resolution for {source_type} -> {target_type}, using target type")
        return target_type

    def initiate_synthetic_data_gen(self, test_layer, db_client, layer_settings):
        """
        Initiate synthetic data generation for the specified layer.

        Args:
            test_layer (str): The target layer (source, lndp, edwp)
            db_client: Database client
            layer_settings (dict): Layer settings
        """
        # currently this is just logging debug, but fc should be to validate conf_synthetic_data (RR)
        LOGGER.debug(f"Synthetic data generation config set as: {self.conf_synthetic_data}")

        scd_settings = layer_settings.get("scd_settings")

        if scd_settings and test_layer == 'edwp':
            # Special case for SCD tables - generate data for LNDP table
            lndp_table_name = layer_settings['lndp_settings']['table_name']
            lndp_schema_name = layer_settings['lndp_settings']['schema_name']

            # Get appropriate columns for LNDP layer (source format)
            columns = self.get_appropriate_columns_for_layer('lndp', layer_settings)

            # Generate the schema and rows
            table_schema = generate_table_schema_from_columns(columns)
            rows = self.conf_synthetic_data.get('row_count')

            LOGGER.info(f"Generating synthetic data for target schema {lndp_schema_name} table {lndp_table_name}")
            LOGGER.debug(f"Using columns for data generation: {columns}")

            # Pass the generated schema and rows to the data generate method
            synthetic_data = generate_synthetic_data(table_schema, rows)

            # Finally insert into the relevant table
            insert_synthetic_data(db_client, lndp_schema_name, lndp_table_name, synthetic_data)
            LOGGER.info(f"Synthetic data generation completed for reference layer since SCD table {lndp_table_name}")

        elif not scd_settings and test_layer == 'edwp':
            edwp_table_name = layer_settings.get("table_name")
            edwp_schema_name = layer_settings.get("schema_name")

            # Get appropriate columns for EDWP layer
            columns = self.get_appropriate_columns_for_layer('edwp', layer_settings)

            # Generate the schema and rows
            table_schema = generate_table_schema_from_columns(columns)
            rows = self.conf_synthetic_data.get('row_count')

            LOGGER.info(f"Generating synthetic data for target schema {edwp_schema_name} table {edwp_table_name}")
            LOGGER.debug(f"Using columns for data generation: {columns}")

            # Pass the generated schema and rows to the data generate method
            synthetic_data = generate_synthetic_data(table_schema, rows)

            # Finally insert into the relevant table
            insert_synthetic_data(db_client, edwp_schema_name, edwp_table_name, synthetic_data)
            LOGGER.info(f"Synthetic data generation completed for target layer table {edwp_table_name}")

        elif not scd_settings and test_layer == 'lndp':
            lndp_table_name = layer_settings.get("table_name")
            lndp_schema_name = layer_settings.get("schema_name")

            # Get appropriate columns for LNDP layer
            columns = self.get_appropriate_columns_for_layer('lndp', layer_settings)

            # Generate the schema and rows
            table_schema = generate_table_schema_from_columns(columns)
            rows = self.conf_synthetic_data.get('row_count')

            LOGGER.info(f"Generating synthetic data for target schema {lndp_schema_name} table {lndp_table_name}")
            LOGGER.debug(f"Using columns for data generation: {columns}")

            # Pass the generated schema and rows to the data generate method
            synthetic_data = generate_synthetic_data(table_schema, rows)

            # Finally insert into the relevant table
            insert_synthetic_data(db_client, lndp_schema_name, lndp_table_name, synthetic_data)
            LOGGER.info(f"Synthetic data generation completed for target table {lndp_table_name}")