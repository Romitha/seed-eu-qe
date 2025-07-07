import re


def get_col_dict_from_expected_cols(expected_columns):
    col_dict = {}

    for col in expected_columns:
        match = re.match(r'^\S+', col)  # Extract column name (first word)
        if match:
            col_name = match.group()
            col_type = col[len(col_name):].strip()  # Get everything after the first word
            col_dict[col_name] = col_type  # Store in dictionary

    return col_dict


# Converting Internal to External Data Types
def convert_internal_to_external(data_type, redshift_data_type_mapping):
    numeric_match = re.match(r'NUMERIC\((\d+),\s*(\d+)\)', data_type, re.IGNORECASE)
    if numeric_match:
        precision, scale = numeric_match.groups()
        return f"decimal({precision}, {scale})"

    varchar_match = re.match(r'VARCHAR\((\d+)\)', data_type, re.IGNORECASE)
    if varchar_match:
        length = varchar_match.group(1)
        return f"varchar({length})"

    char_match = re.match(r'CHARACTER\((\d+)\)', data_type, re.IGNORECASE)
    if char_match:
        length = char_match.group(1)
        return f"char({length})"

    base_type = data_type.split("(")[0].upper()
    return redshift_data_type_mapping.get(base_type, data_type)


# Converting External to Internal Data Types
def convert_external_to_internal(data_type, redshift_data_type_mapping):
    decimal_match = re.match(r'decimal\((\d+),\s*(\d+)\)', data_type, re.IGNORECASE)
    if decimal_match:
        precision, scale = decimal_match.groups()
        return f"NUMERIC({precision}, {scale})"

    varchar_match = re.match(r'varchar\((\d+)\)', data_type, re.IGNORECASE)
    if varchar_match:
        length = varchar_match.group(1)
        return f"VARCHAR({length})"

    char_match = re.match(r'char\((\d+)\)', data_type, re.IGNORECASE)
    if char_match:
        length = char_match.group(1)
        return f"CHARACTER({length})"

    inverse_mapping = {v.lower(): k for k, v in redshift_data_type_mapping.items()}
    base_type = data_type.split("(")[0].lower()
    return inverse_mapping.get(base_type, data_type)


# Converting an entire dictionary of column datatypes
def convert_dict_dtypes(redshift_data_type_mapping, cols_dict, conversion='external'):
    converted_dict = {}
    for col_name, dtype in cols_dict.items():
        if conversion.lower() == 'external':
            converted_dtype = convert_internal_to_external(dtype, redshift_data_type_mapping)
        elif conversion.lower() == 'internal':
            converted_dtype = convert_external_to_internal(dtype, redshift_data_type_mapping)
        else:
            raise ValueError("conversion parameter must be 'internal' or 'external'")

        converted_dict[col_name] = converted_dtype
    return converted_dict


def find_string_dates_needing_cast(expected_columns, mapped_columns):
    columns_to_fix = []

    for src_col, trg_col in zip(expected_columns, mapped_columns):
        # Extract column name and type from each
        src_parts = src_col.lower().split()
        trg_parts = trg_col.lower().split()

        if len(src_parts) >= 2 and len(trg_parts) >= 2:
            src_name, src_type = src_parts[0], src_parts[1]
            trg_name, trg_type = trg_parts[0], trg_parts[1]

            # Match column names and detect type mismatch
            if src_name == trg_name and 'varchar' in src_type and 'timestamp' in trg_type:
                columns_to_fix.append(src_name)
            if src_name == trg_name and 'varchar' in src_type and 'date' in trg_type:
                columns_to_fix.append(src_name)

    return columns_to_fix

def generate_lndp_and_edwp_col_values(column_values):
    lndp_columns = []
    edwp_columns = []

    if column_values:
        for column_string in column_values:
            # Split the string to get parts. The tag is always the last word.
            # The column name and type can have spaces in the type (e.g., 'NUMERIC(18,12)').
            parts = column_string.rsplit(' ', 1) # Split only on the last space

            if len(parts) == 2:
                name_and_type = parts[0]
                tag = parts[1]

                # Now, separate name and type from 'name TYPE'
                # Find the last space that separates the name from the type
                type_start_index = name_and_type.rfind(' ')
                if type_start_index != -1:
                    column_name = name_and_type[:type_start_index].strip()
                    column_type = name_and_type[type_start_index:].strip()
                else:
                    # Handle cases where there might not be a type, just a name and tag
                    column_name = name_and_type.strip()
                    column_type = "" # Or handle as an error if type is always expected

                column_info = f"{column_name} {column_type}"

                if tag == "both":
                    lndp_columns.append(column_info)
                    edwp_columns.append(column_info)
                elif tag == "only_lndp":
                    lndp_columns.append(column_info)
                elif tag == "only_edwp":
                    edwp_columns.append(column_info)
                else:
                    print(f"Warning: Unknown tag '{tag}' for column '{column_name}'. Skipping.")
            else:
                print(f"Warning: Could not parse column string: '{column_string}'. Skipping.")
    return lndp_columns, edwp_columns