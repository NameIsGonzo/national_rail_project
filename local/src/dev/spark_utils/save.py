from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamWriter
from google.cloud import storage
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
        return


def save_historical_to_bq(
    df: DataFrame,
    topic: str,
    project_id: str = "railscope-381421",
    bq_dataset: str = "historical_data_railscope",
    bucket: str = "railscope_historical_data",
) -> DataStreamWriter:
    """ """
    checkpoint_location = create_checkpoint_location(topic, bucket)
    table_name: str = topic[10:]

    try:
        query: DataStreamWriter = (
            df.writeStream.format("bigquery")
            .option("table", f"{project_id}.{bq_dataset}.{table_name}")
            .option("checkpointLocation", checkpoint_location)
            .option("queryName", f"streaming_query_{topic}")
            .option("temporaryGcsBucket", bucket)
            .trigger(processingTime="5 minutes")
            .start()
        )
        logging.info(f"Succesfully loaded dataframe from topic: {topic}")
        return query
    except Exception as e:
        logging.warning(f"An error ocurred while uploading {topic} into the BigQuery")
        logging.info(str(e))


def save_to_realtime_data(
    df: DataFrame,
    topic: str,
    aggregation,
    output_mode: str = "Append",
    project_id: str = "railscope-381421",
    bq_dataset: str = "railscope_",
    bucket: str = "railscope_realtime_data",
) -> DataStreamWriter:
    """ """
    checkpoint_location = create_checkpoint_location(topic, bucket)
    table_name: str = f"{topic[10:]}_{aggregation}"

    try:
        query: DataStreamWriter = (
            df.writeStream.format("bigquery")
            .outputMode(output_mode)
            .option("table", f"{project_id}.{bq_dataset}{aggregation}.{table_name}")
            .option("checkpointLocation", f"{checkpoint_location}_{aggregation}")
            .option("queryName", f"streaming_query_{table_name}")
            .option("temporaryGcsBucket", bucket)
            .trigger(processingTime="15 minutes")
            .start()
        )
        logging.info(f"Succesfully loaded dataframe from topic: {topic}")
        return query
    except Exception as e:
        logging.warning(f"An error ocurred while uploading {topic} into the BigQuery")
        logging.info(str(e))
