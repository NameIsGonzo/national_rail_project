import os
import socket
from argparse import ArgumentParser


def parse_command_line_args():

    #Define command-line arguments
    parser = ArgumentParser()
    parser.add_argument(
        "--stomp_username",
        type=str,
        default=os.environ.get("STOMP_USERNAME"),
        help="STOMP username",
    )
    parser.add_argument(
        "--stomp_password",
        type=str,
        default=os.environ.get("STOMP_PASSWORD"),
        help="STOMP password",
    )
    parser.add_argument(
        "--stomp_hostname",
        type=str,
        default="publicdatafeeds.networkrail.co.uk",
        help="STOMP server hostname",
    )
    parser.add_argument("--stomp_port", type=int, default=61618, help="STOMP server port")
    parser.add_argument(
        "--stomp_topic",
        type=str,
        default="/topic/RTPPM_ALL",
        help="STOMP topic to subscribe to",
    )
    parser.add_argument(
        "--stomp_client_id", type=str, default=socket.gethostname(), help="STOMP client ID"
    )
    parser.add_argument(
        "--stomp_heartbeat_interval_ms",
        type=int,
        default=15_000,
        help="STOMP heartbeat interval in milliseconds",
    )
    parser.add_argument(
        "--stomp_reconnect_delay_sec",
        type=int,
        default=30,
        help="STOMP reconnect delay in seconds",
    )
    return parser.parse_args()