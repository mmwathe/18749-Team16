import socket
import time
import argparse
import threading
import os
from communication_utils import *

# Global Configurations
COMPONENT_ID = os.environ.get("MY_LFD_ID")
LFD_IP = '127.0.0.1'
LFD_PORT = 54321
GFD_IP = os.environ.get("GFD_IP")
GFD_PORT = 12345
heartbeat_interval = 4
timeout_threshold = 10  # Time in seconds to wait for a response before marking server as "dead"

SERVER_ID = None
gfd_socket = None
server_socket = None

def handle_server_registration():
    message = receive(server_socket, COMPONENT_ID)
    if message and message.get('message') == 'register':
        SERVER_ID = message.get('component_id', 'Unknown Server')
        printG(f"Server {SERVER_ID} registered with LFD.")
        response = create_message(COMPONENT_ID, "add replica", message_data=SERVER_ID)
        send(gfd_socket, response, "GFD")

def handle_server_communication():
    global server_socket
    heartbeat_message = create_message(COMPONENT_ID, "heartbeat")
    while True:
        send(server_socket, heartbeat_message, SERVER_ID)
        response = receive(server_socket, COMPONENT_ID)
        if not response:
            printR(f"Server {SERVER_ID} is unresponsive. Marking as dead and notifying GFD.")
            message = create_message(COMPONENT_ID, "remove replica", message_data=SERVER_ID)
            send(gfd_socket, message, "GFD")
            server_socket.close()
            break
        time.sleep(heartbeat_interval)

def wait_for_server():
    global server_socket
    server_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_listener.bind((LFD_IP, LFD_PORT))
    server_listener.listen(1)
    printY(f"LFD listening for server connections on {LFD_IP}:{LFD_PORT}...")

    while True:
        try:
            server_socket, server_address = server_listener.accept()
            printG(f"Server connected from {server_address}")
            handle_server_registration()
            handle_server_communication()
        except Exception as e:
            printR(f"Error handling server connection: {e}")
            break

def connect_to_gfd():
    global gfd_socket
    try:
        gfd_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        gfd_socket.connect((GFD_IP, GFD_PORT))
        printG(f"Connected to GFD at {GFD_IP}:{GFD_PORT}")
        registration_message = create_message(COMPONENT_ID, "register")
        send(gfd_socket, registration_message, "GFD")
    except Exception as e:
        printR(f"Failed to connect to GFD: {e}")

def receive_heartbeat_from_gfd():
        try:
            while True:
                if gfd_socket:
                    message = receive(gfd_socket, COMPONENT_ID)
                    if message and message.get("message") == "heartbeat":
                        heartbeat_acknowledgement = create_message(COMPONENT_ID, "heartbeat acknowledgment")
                        send(gfd_socket, heartbeat_acknowledgement, "GFD")
        except Exception as e:
            printR(f"Error heartbeating with GFD: {e}")

def main():
    global heartbeat_interval
    parser = argparse.ArgumentParser(description="Local Fault Detector (LFD) for monitoring server health.")
    parser.add_argument('--heartbeat_freq', type=int, default=4, help="Heartbeat frequency in seconds.")
    args = parser.parse_args()
    heartbeat_interval = args.heartbeat_freq

    connect_to_gfd()

    server_thread = threading.Thread(target=receive_heartbeat_from_gfd, daemon=True)
    server_thread.start()

    try:
        while True:
            wait_for_server()
            time.sleep(0.5)
    except KeyboardInterrupt:
        printY("LFD interrupted by user.")
    finally:
        if server_socket:
            message = create_message(COMPONENT_ID, "remove replica", message_data=SERVER_ID)
            send(gfd_socket, message, "GFD")
            server_socket.close()
        if gfd_socket:
            gfd_socket.close()
        printR("LFD shutdown.")

if __name__ == '__main__':
    main()