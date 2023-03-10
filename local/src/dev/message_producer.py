import stomp
import logging
import time
import json
from utils.parse_command_line_args import parse_command_line_args
from utils.topic_hub import main_hub

logging.basicConfig(level=logging.INFO)


class StompClient(stomp.ConnectionListener):

    def __init__(self, args):
        self.args = args

    def connect_and_subscribe(self, connection):

        if not self.args.stomp_username:
            logging.error("Username not set")

        if stomp.__version__[0] < 5:
            # If STOMP version is lower than the 5 version start the connection
            connection.start()

        conn_header = {"client-id": f"{self.args.stomp_username}-{self.args.stomp_client_id}"}
        sub_header = {"activemq.subscriptionName": self.args.stomp_client_id}

        connection.connect(
            username=self.args.stomp_username, passcode=self.args.stomp_password, wait=True, headers=conn_header
        )

        connection.subscribe(destination=self.args.stomp_topic, id="1", ack="auto", headers=sub_header)


    def on_heartbeat(self) -> None:
        logging.info("Received heartbeat")


    def on_heartbeat_timeout(self) -> None:
        logging.error("Hearbet timeout")


    def on_error(self, headers, message) -> None:
        logging.error(message)


    def on_disconnected(self) -> None:
        logging.warning(
            f"Disconnected - waiting {self.args.stomp_reconnect_delay_sec} seconds before exiting"
        )
        time.sleep(self.args.stomp_reconnect_delay_sec)
        exit(-1)


    def on_connecting(self, host_and_port) -> None:
        return logging.info(f"Connection to {host_and_port[0]}")


    def on_message(self, frame) -> None:

        # Extract the RTPPMData from the Frame object
        body_str = frame.body.decode("utf-8")
        json_response = json.loads(body_str)['RTPPMDataMsgV1']
        logging.info('Received response from Network Rail System')
        try:
            # Send the message to the main hub for processing and distribution among topics
             main_hub(json_response)
        except Exception as e:
            logging.warning('Something went wrong at sending the message to Kafka Producer')
            logging.warning(e)


if __name__ == "__main__":

    args = parse_command_line_args()

    conn = stomp.Connection12(
        [(args.stomp_hostname, args.stomp_port)],
        auto_decode=False,
        heartbeats=(args.stomp_heartbeat_interval_ms, args.stomp_heartbeat_interval_ms),
    )

    conn.set_listener("", StompClient(args))
    StompClient(args).connect_and_subscribe(conn)

    while True:
        pass
