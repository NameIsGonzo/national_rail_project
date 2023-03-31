from pyspark.sql import DataFrame
from google.cloud import storage
import pendulum
import logging

logging.basicConfig(level=logging.INFO)


def create_checkpoint_location(topic: str, bucket: str) -> str:
    """Creates the checkpoint location for each topic query"""
    try:
        client = storage.Client()
        bucket = client.bucket(bucket)
        checkpoint_location: str = f"checkpoints/checkpoint_{topic}/"
        blob = bucket.get_blob(checkpoint_location)
        if blob is None:
            # Folder doesn't exist, create it
            blob = bucket.blob(checkpoint_location)
            blob.upload_from_string("")
            logging.info(f"Created folder {checkpoint_location}")
            return checkpoint_location
        else:
            logging.info(f"Folder {checkpoint_location} already exists")
            return checkpoint_location
    except Exception as e:
        logging.warning(f"Cant create checkpoint directory for {topic}")
        logging.warning(str(e))
        return False


def save_to_railscope_historical_data(
    df: DataFrame,
    topic: str,
    bucket: str = "railscope_historical_data",
    file_format: str = "parquet",
    output_mode: str = "append",
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
    gcs_bucket_path: str = f"gs://railscope_historical_data/{topic}/{year}/"

    checkpoint_location = create_checkpoint_location(topic, bucket)
    try:
        # Write the streaming DataFrame to the specified GCS bucket path
        query = (
            df.writeStream
            .format(file_format)
            .option("path", gcs_bucket_path)
            .option("checkpointLocation", checkpoint_location)
            .option("queryName", f"streaming_query_{topic}")
            .outputMode(output_mode)
            .trigger(processingTime='10 minutes')
            .start()
        )
        return query
    except Exception as e:
        logging.warning("An error ocurred while uploading the file into the Bucket")
        logging.info(str(e))