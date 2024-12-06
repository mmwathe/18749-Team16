import socket
import time
import argparse
import threading
import os
import subprocess
from communication_utils import *
from dotenv import load_dotenv

load_dotenv()

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
CHECKPOINT_INTERVAL = 10

def handle_server_registration():
    global SERVER_ID
    message = receive(server_socket, COMPONENT_ID)
    if message and message.get('message') == 'register':
        checkpoint_interval = message.get('checkpoint', CHECKPOINT_INTERVAL)
        CHECKPOINT_INTERVAL = checkpoint_interval
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

def begin_automated_recovery():
    global server_socket, SERVER_ID
    time.sleep(10)
    server_script_path = "/Users/hartman/Documents/College/Senior_Fall/18-749/18749-Team16/.venv/bin/python /Users/hartman/Documents/College/Senior_Fall/18-749/18749-Team16/passive_replication/server.py --checkpoint_interval {CHECKPOINT_INTERVAL}"
    try:
        # Use osascript to open a new Terminal window and run the command
        subprocess.Popen([
            "osascript", "-e",
            f'tell application "Terminal" to do script "{server_script_path}"'
        ])
    except Exception as e:
        printR(f"Failed to relaunch server {SERVER_ID}: {e}")

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

def receive_message_from_gfd():
    """Listen for messages from GFD, including heartbeats and recovery commands."""
    try:
        while True:
            if gfd_socket:
                message = receive(gfd_socket, COMPONENT_ID)
                if message:
                    action = message.get("message")
                    if action == "heartbeat":
                        # Acknowledge GFD heartbeat
                        heartbeat_acknowledgement = create_message(COMPONENT_ID, "heartbeat acknowledgment")
                        send(gfd_socket, heartbeat_acknowledgement, "GFD")
                    elif action == "recover_server":
                        # Handle recovery message from GFD
                        server_id = message.get("server_id", None)
                        if server_id:
                            printY(f"Received recovery request from GFD for server {server_id}")
                            begin_automated_recovery()  # Call the recovery function
                        else:
                            printR("Received recover_server message without server_id.")
                    elif action == "new_primary":
                        election_message = create_message(COMPONENT_ID, "new_primary")
                        send(server_socket, election_message, SERVER_ID)
                    else:
                        printY(f"Unknown message received from GFD: {message}")
    except Exception as e:
        printR(f"Error handling messages from GFD: {e}")

def main():
    global heartbeat_interval
    parser = argparse.ArgumentParser(description="Local Fault Detector (LFD) for monitoring server health.")
    parser.add_argument('--heartbeat_freq', type=int, default=4, help="Heartbeat frequency in seconds.")
    args = parser.parse_args()
    heartbeat_interval = args.heartbeat_freq

    connect_to_gfd()

    server_thread = threading.Thread(target=receive_message_from_gfd, daemon=True)
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
