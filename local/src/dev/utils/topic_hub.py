from kafka import KafkaProducer
from utils import json_parser as parser
import json
import logging

logging.basicConfig(level=logging.DEBUG)


def send_to_topic(topic: str, message: dict):

    try:
        # Kafka Producer set-up
        producer = KafkaProducer(
            value_serializer=lambda m: json.dumps(m).encode("utf-8"),
            bootstrap_servers=["localhost:9092"],
        )

        # Send message to kafka topic
        producer.send(topic, message)

        # Flush the producer to ensure all messages are written to Kafka
        producer.flush()

        logging.info(f"Successfully send {message} to {topic}")

        # Close the producer
        producer.close()
    except Exception as e:
        logging.warning(e)


def main_hub(message: dict):

    # Retrieve nationalppm
    try:
        logging.info("Retrieven national ppm information from dict")
        national_ppm = parser.flatten_national_page_ppm(
            message["RTPPMData"]["NationalPage"]["NationalPPM"]
        )
        send_to_topic("rtppmdata.nationalpage.nationalppm", national_ppm)
    except Exception as e:
        logging.warning(e)
