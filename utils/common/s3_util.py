from botocore.exceptions import ClientError
from chardet import detect

from utils.framework.custom_logger_util import get_logger

LOGGER = get_logger()


def get_s3_uri_with_bucket_prefix(bucket_name, uri):
    s3_path_starter = 's3:'
    s3_uri = f"{s3_path_starter}//{bucket_name}/{uri}"
    return s3_uri


def check_s3_files_exist(s3_client, bucket_name, uri):
    """
    Checks if any files exist in the given S3 path.
    Returns True if files are found, otherwise False.
    """
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=uri)
    return 'Contents' in response


def list_recoverable_s3_file_versions(s3_client, bucket_name, uri):
    # List object versions
    response = s3_client.list_object_versions(Bucket=bucket_name, Prefix=uri)

    # Check if there are any versions (including delete markers)
    if 'Versions' in response or 'DeleteMarkers' in response:
        return True
    else:
        return False


def count_s3_files(s3_client, bucket_name, uri):
    total_files = 0
    continuation_token = None

    while True:
        list_kwargs = {'Bucket': bucket_name, 'Prefix': uri}
        if continuation_token:
            list_kwargs['ContinuationToken'] = continuation_token

        response = s3_client.list_objects_v2(**list_kwargs)

        if 'Contents' in response:
            total_files += len(response['Contents'])

        if response.get('IsTruncated'):
            continuation_token = response['NextContinuationToken']
        else:
            break

    return total_files


def delete_s3_files(s3_client, bucket_name, uri):
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=uri)

    if 'Contents' in response:
        objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
        s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': objects_to_delete})
        return len(objects_to_delete)


def convert_s3_files_to_utf8(s3_client, s3_bucket, s3_prefix, source_encoding, source_file_type):
    # List all objects in the S3 location
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix)

    for page in page_iterator:
        if 'Contents' not in page:
            LOGGER.info(f'No files found in - {s3_bucket}/{s3_prefix}')
            return

        for obj in page['Contents']:
            # Get the object key (file path)
            file_key = obj['Key']

            # Skip if it's a directory
            if file_key.endswith('/'):
                continue

            # Skip if the file does not match the specified file type
            if not file_key.lower().endswith(f".{source_file_type.lower()}"):
                LOGGER.info(f"Skipping non-{source_file_type} - {s3_bucket}/{file_key}")
                continue

            LOGGER.info(f"Processing file: {s3_bucket}/{file_key}")

            try:
                # Download the file into memory
                response = s3_client.get_object(Bucket=s3_bucket, Key=file_key)
                file_content = response['Body'].read()

                # Detect the file's encoding
                detected_encoding = detect(file_content)['encoding']

                # Check if the file is already in UTF-8
                if detected_encoding.lower() == 'utf-8':
                    LOGGER.info(f'File is already in UTF-8 - {s3_bucket}/{file_key}')
                    continue

                # Decode the file content from the source encoding
                decoded_content = file_content.decode(source_encoding)

                # Encode the content to UTF-8
                utf8_content = decoded_content.encode('utf-8')

                # Upload the converted file back to S3
                s3_client.put_object(
                    Bucket=s3_bucket,
                    Key=file_key,
                    Body=utf8_content
                )

                LOGGER.info(f"Converted and uploaded - {s3_bucket}/{file_key}")
            except Exception as e:
                LOGGER.info(f"Error processing file {file_key}: {e}")

    LOGGER.info("All files processed.")


def recover_latest_s3_files(s3_client, bucket_name, prefix):
    try:
        LOGGER.info(f"Checking versions for files in {bucket_name}/{prefix}")

        # List versions including delete markers
        response = s3_client.list_object_versions(Bucket=bucket_name, Prefix=prefix)

        versions = response.get('Versions', [])
        delete_markers = response.get('DeleteMarkers', [])

        if not delete_markers:
            LOGGER.warning(f"No delete markers found in {bucket_name}/{prefix}")
            return []

        # Find the latest LastModified timestamp among delete markers
        latest_timestamp = max(dm['LastModified'] for dm in delete_markers)

        # Filter delete markers that have the latest timestamp
        latest_delete_markers = [
            dm for dm in delete_markers if dm['LastModified'] == latest_timestamp
        ]

        recovered_files = []

        for delete_marker in latest_delete_markers:
            key = delete_marker['Key']
            version_id = delete_marker['VersionId']

            LOGGER.info(f"Recovering {key} with version ID: {version_id}")

            # Remove the delete marker to restore the file
            s3_client.delete_object(
                Bucket=bucket_name,
                Key=key,
                VersionId=version_id
            )

            LOGGER.info(f"Delete marker removed for {key}. File restored.")

            # Find the latest version corresponding to the deleted key
            latest_version = max(
                [v for v in versions if v['Key'] == key],
                key=lambda v: v['LastModified']
            )

            recovered_files.append(latest_version)

        return recovered_files

    except ClientError as e:
        LOGGER.error(f"Error recovering file versions from S3: {str(e)}")
        raise
