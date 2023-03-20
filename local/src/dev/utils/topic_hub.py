from kafka import KafkaProducer
from utils import json_parser as parser
import json
import logging
import concurrent.futures
from datetime import datetime

logging.basicConfig(level=logging.DEBUG)


def send_to_broker_9092(results: list, topics: str) -> None:

    try:
        # Kafka Producer set-up
        producer = KafkaProducer(
            value_serializer=lambda m: json.dumps(m).encode("utf-8"),
            bootstrap_servers=["localhost:9092"],
        )

        for result, topic in zip(results, topics):
            # Some result are in the following struct list[dict]
            if isinstance(result, list):
                for record in result:
                    try:
                        producer.send(topic, record)
                        logging.info(f"Successfully send {record} to {topic}")
                    except Exception as e:
                        logging.error(e)
            else:
                try:
                    # Send message to kafka topic
                    producer.send(topic, result)
                    logging.info(f"Successfully send {result} to {topic}")
                except Exception as e:
                    logging.error(e)

        # Flush any remaining message
        producer.flush()
        # Close the producer
        producer.close()
    except Exception as e:
        logging.warning(e)


def send_to_broker_9093(results: list, topics: str) -> None:

    try:
        # Kafka Producer set-up
        producer = KafkaProducer(
            value_serializer=lambda m: json.dumps(m).encode("utf-8"),
            bootstrap_servers=["localhost:9093"],
        )

        for result, topic in zip(results, topics):
            # Some result are in the following struct list[dict]
            if isinstance(result, list):
                for record in result:
                    try:
                        producer.send(topic, record)
                        logging.info(f"Successfully send {record} to {topic}")
                    except Exception as e:
                        logging.error(e)
            else:
                try:
                    # Send message to kafka topic
                    producer.send(topic, result)
                    logging.info(f"Successfully send {result} to {topic}")
                except Exception as e:
                    logging.error(e)

        # Flush any remaining message
        producer.flush()
        # Close the producer
        producer.close()
    except Exception as e:
        logging.warning(e)


def main_hub(message: dict):

    futures: list = []
    timestamp: str = message["timestamp"]
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        # Submit each parsing function to the executor
        futures.append(
            executor.submit(
                parser.flatten_national_page_ppm,
                message["RTPPMData"]["NationalPage"]["NationalPPM"],
            )
        )

        futures.append(
            executor.submit(
                parser.flatten_national_page_sector,
                message["RTPPMData"]["NationalPage"]["Sector"],
                timestamp,
            )
        )
        futures.append(
            executor.submit(
                parser.flatten_national_page_operators,
                message["RTPPMData"]["NationalPage"]["Operator"],
                timestamp,
            )
        )

        futures.append(
            executor.submit(
                parser.flatten_out_of_course_page,
                message["RTPPMData"]["OOCPage"]["Operator"],
                timestamp,
            )
        )
        futures.append(
            executor.submit(
                parser.flatten_fooc_page_ppm,
                message["RTPPMData"]["FOCPage"]["NationalPPM"],
            )
        )
        futures.append(
            executor.submit(
                parser.flatten_fooc_page_operators,
                message["RTPPMData"]["FOCPage"]["Operator"],
                timestamp,
            )
        )
        futures.append(
            executor.submit(
                parser.flatten_operators_page,
                message["RTPPMData"]["OperatorPage"],
                timestamp,
            )
        )
        futures.append(
            executor.submit(
                parser.flatten_operators_page_groups,
                message["RTPPMData"]["OperatorPage"],
                timestamp,
            )
        )
    # Get the results of each parsing function
    results: list = [result.result() for result in futures]
    results[0]["timestamp"] = timestamp
    results[4]["timestamp"] = timestamp
    try:
        results_9092: list = results[0:4]
        results_9093: list = results[4:]
        topics_9092: list = [
            "rtppmdata.nationalpage.nationalppm",
            "rtppmdata.nationalpage.sector",
            "rtppmdata.nationalpage.operator",
            "rtppmdata.oocpage.operator",
        ]
        topics_9093: list = [
            "rtppmdata.focpage.nationalppm",
            "rtppmdata.focpage.operator",
            "rtppmdata.operatorpage.operators",
            "rtppmdata.operatorpage.servicegroups",
        ]
        send_to_broker_9092(results_9092, topics_9092)
        send_to_broker_9093(results_9093, topics_9093)

    except Exception as e:
        logging.warning(e)
