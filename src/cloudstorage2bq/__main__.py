import argparse
import logging
import os
import re
from typing import List

from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage

logger = logging.getLogger(__name__)


def setup_logging():
    """Configure python logging to emit to stdout."""

    logging.basicConfig(
        level=logging.INFO,
        # display isoformat time, log level, module, line number, message
        format="%(asctime)s - %(levelname)s - %(name)s:%(lineno)s - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )


def list_files(bucket_name: str, prefix: str, delimeter=None):
    """
    list all files in a google cloud storage uri
    """

    # create a client
    client = storage.Client()

    # create a bucket object
    bucket = client.get_bucket(bucket_name)

    # list all blobs in the bucket
    blobs: List[storage.Blob] = bucket.list_blobs(prefix=prefix, delimiter=delimeter)

    # return a list of all the blob names
    return [f"gs://{bucket_name}/{blob.name}" for blob in blobs]


def load_prefix_to_dataset(
    bucket_name: str, bucket_prefix: str, dataset_name: str, table_name_prefix: str = ""
):
    """
    load all data in google cloud storage at bucket_name/prefix into a bigquery dataset.

    dataset_name is the name of the destination dataset
    table_name_prefix is prepended to the table name for each file in the bucket_name
    """

    # create a client
    client = bigquery.Client()

    # create a dataset reference
    dataset_ref = client.dataset(dataset_name)

    # create a dataset if it doesn't exist
    try:
        dataset = client.get_dataset(dataset_ref)
        logger.info(f"got dataset {dataset}")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset = client.create_dataset(dataset)

    # list all tables in dataset in one request so the lookup is faster
    all_tables_in_dataset = [
        table.table_id for table in client.list_tables(dataset_ref)
    ]

    # list all files in the gs_uri
    files = list_files(bucket_name, bucket_prefix)

    # create a load job for each file
    jobs = []
    for file in files:
        logger.info(f"processing file {file}")
        # remove file extensions
        table_name = os.path.splitext(os.path.basename(file))[0]
        # table_name must contain only letters (a-z, A-Z), numbers (0-9), or underscores (_), and must start with a letter
        table_name = re.sub(r"[^a-zA-Z0-9_]", "_", table_name)
        logger.info(table_name)
        assert table_name[0].isalpha(), "table_name must start with a letter"
        table_name = f"{table_name_prefix}{table_name}"

        # check if table exists first
        if table_name in all_tables_in_dataset:
            logger.info(f"table {table_name} already exists, skipping load")
            continue

        # if not, load table
        load_job = client.load_table_from_uri(
            file,
            dataset_ref.table(table_name),
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET
            ),
        )
        jobs.append(load_job)
        logger.info(f"Started load job for {file} into {dataset_name}.{table_name}")

    done = False
    while not done:
        for load_job in jobs:
            if not load_job.done():
                continue

            # wait for the load jobs to complete
            load_job.result()

            # log the load job
            if load_job.error_result:
                logger.error(
                    f"Error loading files {load_job.source_uris} into {load_job.destination}"
                )
                logger.error(load_job.error_result)
            else:
                logger.info(
                    f"Completed job {load_job.job_id} to load files {load_job.source_uris} into {load_job.destination}"
                )

            jobs.remove(load_job)

        if len(jobs) == 0:
            done = True


def load_all_datasets(bucket_name: str, bucket_prefix: str, table_name_prefix: str):
    """Lists all prefixes in a bucket, and calls load_prefix_to_dataset for each one"""
    # create a client
    client = storage.Client()

    # create a bucket object
    bucket = client.get_bucket(bucket_name)

    blobs = bucket.list_blobs(prefix=bucket_prefix, delimiter="/")
    # due to an oddity in google cloud storage, we need to iterate over all the blobs to find the prefixes
    # this seems like there should be a better way...
    # https://github.com/googleapis/python-storage/issues/294
    for _ in blobs:
        pass

    for prefix in blobs.prefixes:
        # remove trailing slashes
        prefix = prefix.rstrip("/")
        #  Dataset IDs must be alphanumeric (plus underscores) and must be at most 1024 characters long.
        dataset_name = re.sub(r"[^a-zA-Z0-9_]", "_", prefix)
        load_prefix_to_dataset(
            bucket_name,
            bucket_prefix=prefix,
            dataset_name=dataset_name,
            table_name_prefix=table_name_prefix,
        )


def main():
    setup_logging()
    # argparse should accept bucket_name, bucket_prefix, dataset_name, table_name_prefix as arguments
    parser = argparse.ArgumentParser(
        description="Load data from google cloud storage into bigquery"
    )
    parser.add_argument(
        "--bucket_name",
        help="The name of the google cloud storage bucket",
        required=True,
    )
    parser.add_argument(
        "--bucket_prefix", help="The prefix of the files to load", default=""
    )
    parser.add_argument(
        "--table_name_prefix",
        help="The prefix to prepend to the table name for each file in the bucket",
        default="",
    )

    args = parser.parse_args()

    load_all_datasets(
        bucket_name=args.bucket_name,
        bucket_prefix=args.bucket_prefix,
        table_name_prefix=args.table_name_prefix,
    )


if __name__ == "__main__":
    main()
