from pyspark.sql import DataFrame
from google.cloud import storage
import os
import pendulum
import logging

logging.basicConfig(level= logging.INFO)


def create_month_folder(year: str, month: str, topic: str, bucket: str) -> bool:
    """Creates the folder for each month"""
    try:
        client = storage.Client()
        bucket = client.bucket(bucket)
        blob = bucket.blob(f'{topic}/{year}/{month}/')
        blob.upload_from_string("")
        return True
    except:
        logging.warning(f'Cant create directory {topic}/{year}/{month} for bucket: {bucket}')
        return False


def save_to_railscope_historical_data(
    df: DataFrame,
    topic: str,
    file_format: str = 'csv',
    output_mode: str = 'append',
    checkpoint_location: str = 'gs://railscope_historical_data/checkpoint',
) -> None:
    """
    Save a streaming PySpark DataFrame to a Google Cloud Storage bucket.

    :param df: The streaming PySpark DataFrame to save.
    :param file_format: The file format to save the DataFrame, e.g., 'parquet', 'csv', 'json'.
    :param output_mode: The output mode for writing the streaming DataFrame, e.g., 'append', 'complete', or 'update'.
    :param checkpoint_location: The path to store checkpoint information, required for fault-tolerance.
                                Format: 'gs://your-bucket-name/path/to/checkpoint/location'
    """
    year: str = str(pendulum.now().year)
    month: str = str(pendulum.now().month)
    gcs_bucket_path: str = f'gs://railscope_historical_data/{topic}/{year}/{month}/'

    if not checkpoint_location:
        raise ValueError("A Checkpoint location is required for fault-tolerance")
    

    if create_month_folder(year, month, topic, 'railscope_historical_data'):
        # Write the streaming DataFrame to the specified GCS bucket path
        
        query = (
            df.coalesce(1).writeStream.format(file_format)
            .option("path", gcs_bucket_path)
            .option("checkpointLocation", checkpoint_location)
            .outputMode(output_mode)
            .start()
        )
        return query
    else:
        logging.warning('An error ocurred while uploading the file into the Bucket')
