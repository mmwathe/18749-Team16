import socket
import time
import json
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
gfd_socket = None
server_socket = None
server_id = None

def register_with_gfd():
    """Registers LFD with GFD."""
    if gfd_socket:
        message = create_message(COMPONENT_ID, "register")
        send(gfd_socket, message, COMPONENT_ID, "GFD")

def handle_server_registration():
    """Processes the server registration message."""
    global server_id
    message = receive(server_socket, "Server", COMPONENT_ID)
    if message and message.get('message') == 'register':
        server_id = message.get('component_id', 'Unknown Server')
        printG(f"Server {server_id} registered with LFD.")
        notify_gfd("add replica", {"server_id": server_id})

def handle_server_communication():
    """Handles ongoing server communication, including heartbeats."""
    global server_socket
    while True:
        send(server_socket, create_message(COMPONENT_ID, "heartbeat"), COMPONENT_ID, server_id)
        response = receive(server_socket, server_id, COMPONENT_ID)
        if not response:
            printR(f"Server {server_id} is unresponsive. Marking as dead and notifying GFD.")
            notify_gfd("remove replica", {"server_id": server_id})
            server_socket.close()
            break
        time.sleep(heartbeat_interval)

def connect_to_gfd():
    """Establishes a persistent connection to the GFD and sends registration."""
    global gfd_socket
    gfd_socket = connect_to_socket(GFD_IP, GFD_PORT)
    if gfd_socket:
        printG(f"Connected to GFD at {GFD_IP}:{GFD_PORT}")
        register_with_gfd()

def notify_gfd(event_type, event_data):
    """Sends a notification message to GFD."""
    if gfd_socket:
        message = create_message(COMPONENT_ID, event_type, message_data=event_data)
        send(gfd_socket, message, COMPONENT_ID, "GFD")

    while True:
        try:
            server_socket, server_address = server_socket.accept()
            printG(f"Server connected from {server_address}")
            handle_server_registration()
            handle_server_communication()
        except Exception as e:
            printR(f"Error handling server connection: {e}")
            break

def heartbeat_thread():
    """Continuously sends heartbeats to GFD."""
    while True:
        receive_heartbeat_from_gfd()
        time.sleep(0.5)

def receive_heartbeat_from_gfd():
    """Handles heartbeat messages from GFD."""
    if gfd_socket:
        message = receive(gfd_socket, "GFD", COMPONENT_ID)
        if message and message.get("message") == "heartbeat":
            acknowledgment = create_message(COMPONENT_ID, "heartbeat acknowledgment")
            send(gfd_socket, acknowledgment, COMPONENT_ID, "GFD")

def main():
    global heartbeat_interval
    parser = argparse.ArgumentParser(description="Local Fault Detector (LFD) for monitoring server health.")
    parser.add_argument('--heartbeat_freq', type=int, default=4, help="Heartbeat frequency in seconds.")
    args = parser.parse_args()
    heartbeat_interval = args.heartbeat_freq

    server_socket = initialize_component(COMPONENT_ID, "Local Fault Detector", LFD_IP, LFD_PORT, 1)

    connect_to_gfd()

    heartbeat = threading.Thread(target=heartbeat_thread, daemon=True)
    heartbeat.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        printY("LFD interrupted by user.")
    finally:
        if server_socket:
            notify_gfd("remove replica", {"server_id": server_id, "reason": "LFD shutting down"})
            server_socket.close()
        if gfd_socket:
            gfd_socket.close()
        printR("LFD shutdown.")

if __name__ == '__main__':
    main()
