import stomp
import logging
import socket
import time
import io
import zlib
import ppv16.PPv16 as PPv16
import json

logging.basicConfig(level=logging.INFO)

username: str = "gonzaloalcala0304@gmail.com"
password: str = "000dataProject000!"
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

        filepath = r'/Users/gonzo/Desktop/RailScope/national_rail_project/src/sandbox/stomp/rtppm/rtppm_data.json'
        logging.info(frame)
        logging.info('-'*250)
        
        try:
            # Extract the RTPPMData from the Frame object
            body_str = frame.body.decode('utf-8')
            body_dict = json.loads(body_str)
            rtppm_data = body_dict['RTPPMDataMsgV1']

            with open(filepath, 'w') as f:
                json.dump(rtppm_data, f)

        except Exception as e:
            logging.error(str(e))


conn = stomp.Connection12(
    [(hostname, port)],
    auto_decode=False,
    heartbeats=(heartbeat_interval_ms, heartbeat_interval_ms),
)

conn.set_listener('', StompClient())
connect_and_subscribe(conn)

while True:
    pass

conn.disconect()
