"""
Copies file from local dir to S3 and copies it from S3 to Redshift.

Usage:
    import main
    main.run("src_dir", "src_file", "table_schema_file", "db_name", "schema.dest_table", "bucket_name")
"""

import sys
import boto3
import botocore

from utils import credential_manager_utils as credential_manager
from utils import redshift_utils as redshift
from utils import redshift_connection_utils as rconn


S3_CREDENTIALS = credential_manager.read("s3_creds")
S3_CLIENT = boto3.client(
    "s3",
    aws_access_key_id=f"{S3_CREDENTIALS['AWS_ACCESS_KEY_ID']}",
    aws_secret_access_key=f"{S3_CREDENTIALS['AWS_SECRET_ACCESS_ID']}",
)
S3_RESOURCE = boto3.resource("s3")
S3_BUCKET = "s3://ai-hotel/"


def check_bucket_exists(bucket: str) -> bool:
    """Returns True is S3 bucket exists, False otherwise.

    Examples:
    # check_bucket_exists("some_bucket_name")
    True

    # check_bucket_exists("s3://some_bucket_name")
    # This catches a Boto Error #403
    True
    > UserWarning: Insufficient credentials to access existing bucket.

    # check_bucket_exists("some_bucket_name")
    # This catches a Boto Error #404
    False

    Returns:
      A bool value representing whether the bucket exists or not.

    """

    # Standardize bucket name passed.
    if bucket.startswith("s3://"):
        bucket = bucket[5:]  # remove the s3://
    if bucket.endswith("/"):
        bucket = bucket[:-1]  # remove the trailing slash

    try:
        S3_CLIENT.head_bucket(Bucket=bucket)
        return True
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response["Error"]["Code"])
        if error_code == 403:
            print("Insufficient credentials to access existing bucket.")
            return True
        elif error_code == 404:
            return False
        else:
            return False


def copy_to_s3(src_dir: str, src_file: str, bucket: str) -> None:
    """Uploads a file from src_dir into an S3 bucket.

    NOTE: For larger files, will need to implement a chunking method.
    The FileChunkIO library is useful for the above use case.
    """

    # Standardize bucket name passed.
    if bucket.startswith("s3://"):
        bucket = bucket[5:]  # remove the s3://
    if bucket.endswith("/"):
        bucket = bucket[:-1]  # remove the trailing slash

    # Make sure the S3 bucket exists before uploading a file.
    if not check_bucket_exists(bucket):
        raise Exception("S3 bucket not found.")

    # Create the file path and upload to S3 bucket.
    file_path = f"{src_dir}/{src_file}"
    try:
        S3_CLIENT.upload_file(file_path, bucket, src_file)
        S3_RESOURCE.Object(bucket, src_file).wait_until_exists()
    except Exception as e:
        print(f"Could not upload file to S3 bucket - {e}")

    print("S3 Transfer Complete!")


def copy_to_redshift(
    src_dir: str, src_file: str, table_sch: str, dest_db: str, dest_table: str, bucket: str
) -> None:
    """Copies S3 bucket contents into a Redshift table."""

    # Make sure the S3 bucket exists before copying to Redshift.
    if not check_bucket_exists(bucket):
        raise Exception("S3 bucket not found.")

    # Standardize bucket name passed.
    if bucket[:5] != "s3://":
        bucket = "s3://" + bucket
    if bucket[-1] == "/":
        bucket = bucket[:-1]

    try:
        dest_schema, dest_tablename = dest_table.split(".")
    except ValueError:
        raise ValueError("Destination tables have the format: schema.table")

    # Make sure that Redshift schema exists before creating the dest_table.
    if not redshift.check_schema_exists(schema_name=dest_schema, db=dest_db):
        raise ValueError(f"copy_to_redshift() expected {dest_schema} to exist, but it doesn't.")

    # Generate the table schema to prep for upload.
    create_query = redshift.get_schema(
        source_dir=src_dir, source_file=table_sch, dest_table=dest_table
    )

    # Create empty table in Redshift schema.
    print(f"Creating table {dest_table} based on CSV schema found...")
    rconn.execute(sql=create_query, db=dest_db)

    # Confirm that Redshift table exists in the schema.
    if not redshift.check_table_exists(
        schema_name=dest_schema, table_name=dest_tablename, db=dest_db
    ):
        raise ValueError(f"Import table {dest_table} does not exist.")
    print("New Table Created!")

    transfer_query = f"""
    BEGIN;
    COPY {dest_table}
    FROM '{bucket}/{src_file}'
    ACCESS_KEY_ID '{S3_CREDENTIALS['AWS_ACCESS_KEY_ID']}'
    SECRET_ACCESS_KEY '{S3_CREDENTIALS['AWS_SECRET_ACCESS_ID']}'
    IGNOREHEADER 1
    BLANKSASNULL
    EMPTYASNULL
    CSV;
    COMMIT;
    """

    print("Transfering data from S3 to Redshift now...")
    rconn.execute(sql=transfer_query, quiet=True, db=dest_db)
    print("Redshift Transfer Complete!")


def run(
    source_dir: str,
    source_file: str,
    table_schema: str,
    dest_db: str,
    dest_table: str,
    bucket: str = S3_BUCKET,
) -> None:
    """Main method for triggering the job to upload a file to S3 then to Redshift."""

    print(f"Copying /{source_dir}/{source_file} to S3 bucket {bucket}...")
    copy_to_s3(src_dir=source_dir, src_file=source_file, bucket=bucket)

    print(f"Copying {source_file} from S3 bucket {bucket} to Redshift destination {dest_table}...")
    copy_to_redshift(
        src_dir=source_dir,
        src_file=source_file,
        table_sch=table_schema,
        dest_db=dest_db,
        dest_table=dest_table,
        bucket=bucket,
    )
