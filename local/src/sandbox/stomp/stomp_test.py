import stomp
import logging
import socket
import time
import json
import os
from kafka import KafkaProducer
import sys
import fastavro
import io

# Set the path to the root of the project
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from kafka_lab import json_parser as parser


logging.basicConfig(level=logging.INFO)

# Kafka Producer set-up
producer = KafkaProducer(bootstrap_servers=["localhost:9092"])


username: str = os.environ.get("STOMP_USERNAME")
password: str = os.environ.get("STOMP_PASSWORD")
hostname: str = "publicdatafeeds.networkrail.co.uk"
port: int = 61618
topic: str = "/topic/RTPPM_ALL"
client_id: str = socket.getfqdn()
heartbeat_interval_ms: int = 15_000
reconnect_delay_sec: int = 15

if not username:
    logging.error(
        "Username not set - configure your username and password in stomp_test"
    )


def connect_and_subscribe(connection):

    if stomp.__version__[0] < 5:
        # If STOMP version is lower than the 5 version start the connection
        connection.start()

    conn_header = {"client-id": f"{username}-{client_id}"}
    sub_header = {"activemq.subscriptionName": client_id}

    connection.connect(
        username=username, passcode=password, wait=True, headers=conn_header
    )

    connection.subscribe(destination=topic, id="1", ack="auto", headers=sub_header)


class StompClient(stomp.ConnectionListener):
    def get_schema(self, key: int):
        """Returns the desired schema from the schema registry"""

        if key > 8 or key < 0:
            logging.error("Schema registry only contains 8 schemas.")
            return

        with open(
            "src/sandbox/kafka_lab/schema-registry-data/schema_registry.avsc", "r"
        ) as f:
            schema_str = f.read()

        schema_json = json.loads(schema_str)
        schema = schema_json[key]

        return schema

    def on_heartbeat(self) -> None:
        logging.info("Received heartbeat")

    def on_heartbeat_timeout(self) -> None:
        logging.error("Hearbet timeout")

    def on_error(self, headers, message) -> None:
        logging.error(message)

    def on_disconnected(self) -> None:
        logging.warning(
            f"Disconnected - waiting {reconnect_delay_sec} seconds before exiting"
        )
        time.sleep(reconnect_delay_sec)
        exit(-1)

    def on_connecting(self, host_and_port) -> None:
        return logging.info(f"Connection to {host_and_port[0]}")

    def on_message(self, frame) -> None:

        try:
            bytes_io = io.BytesIO()
            # Extract the RTPPMData from the Frame object
            body_str = frame.body.decode("utf-8")
            json_response = json.loads(body_str)

            # national ppm
            national_ppm: dict = parser.flatten_national_page_ppm(
                json_response["RTPPMDataMsgV1"]["RTPPMData"]["NationalPage"][
                    "NationalPPM"
                ]
            )
            schema = self.get_schema(0)
            fastavro.schemaless_writer(bytes_io, json.dumps(schema), national_ppm)
            message_national_ppm = bytes_io.getvalue()
            logging.info(schema)
            logging.info(national_ppm)  
            producer.send(
                "rtppmdata.nationalpage.nationalppm", value=message_national_ppm
            )

        except Exception as e:
            logging.error(e)


conn = stomp.Connection12(
    [(hostname, port)],
    auto_decode=False,
    heartbeats=(heartbeat_interval_ms, heartbeat_interval_ms),
)

conn.set_listener("", StompClient())
connect_and_subscribe(conn)

while True:
    pass
