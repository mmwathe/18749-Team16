import socket
import json
import time
from queue import Queue, Empty
import threading
import os
import errno
from communication_utils import create_message, send, receive, print_log, connect_to_socket, initialize_component

# Define color functions for printing with enhanced formatting
def print_registration(skk): print(f"\033[92m{skk}\033[00m")  # Green for registrations and accepted connections
def print_disconnection(skk): print(f"\033[91m{skk}\033[00m")  # Red for disconnections or errors
def print_warning(skk): print(f"\033[93m{skk}\033[00m")  # Yellow for warnings or waiting
def print_sent(skk): print(f"\033[96m{skk}\033[00m")  # Cyan for sent messages
def print_received(skk): print(f"\033[95m{skk}\033[00m")  # Purple for received messages

# Global Configurations
COMPONENT_ID = os.environ.get("MY_SERVER_ID")
SERVER_IP = '0.0.0.0'
SERVER_PORT = 12346
LFD_IP = '127.0.0.1'
LFD_PORT = 54321

RELIABLE_SERVER_IP = os.environ.get("S1")
RELIABLE_SERVER_PORT = 12351

state = 0
lfd_socket = None
clients = {}
message_queue = Queue()

def connect_to_lfd():
    """Establishes a connection to the LFD and sends a registration message."""
    global lfd_socket
    lfd_socket = connect_to_socket(LFD_IP, LFD_PORT)
    if lfd_socket:
        print_registration(f"Connected to LFD at {LFD_IP}:{LFD_PORT}")
        send(lfd_socket, create_message(COMPONENT_ID, "register"), COMPONENT_ID, "LFD")

def handle_heartbeat():
    """Handles heartbeat messages from the LFD."""
    while True:
        if lfd_socket:
            message = receive(lfd_socket, "LFD", COMPONENT_ID)
            if message and message.get("message") == "heartbeat":
                send(lfd_socket, create_message(COMPONENT_ID, "heartbeat acknowledgment"), COMPONENT_ID, "LFD")
        time.sleep(1)

def handle_request_state(client_socket):
    """Handles a request_state message from another server."""
    global state
    response = create_message(COMPONENT_ID, "state_response", state=state)
    send(client_socket, response, COMPONENT_ID, "Requesting Server")

def accept_new_connections(server_socket):
    """Accepts new client connections if available and adds them to the clients dictionary."""
    try:
        client_socket, client_address = server_socket.accept()
        print_registration(f"Client connected: {client_address}")
        clients[client_socket] = client_address
    except Exception as e:
        print_disconnection(f"Error accepting client connection: {e}")

def process_client_messages():
    """Processes messages from connected clients and handles responses."""
    global state
    for client_socket in list(clients.keys()):
        try:
            message = receive(client_socket, f"Client@{clients[client_socket]}", COMPONENT_ID)
            if not message:
                continue
            message_type = message.get("message", "unknown")
            if message_type == "ping":
                response = create_message(COMPONENT_ID, "pong")
            elif message_type == "update":
                state += 1
                response = create_message(COMPONENT_ID, "state updated", state=state)
            else:
                response = create_message(COMPONENT_ID, "unknown command")
            message_queue.put((client_socket, response))
        except Exception as e:
            print_disconnection(f"Error processing client message: {e}")
            disconnect_client(client_socket)

def synchronize_state():
    """Synchronizes the state with the reliable server if available."""
    global state
    try:
        reliable_socket = connect_to_socket(RELIABLE_SERVER_IP, RELIABLE_SERVER_PORT)
        if reliable_socket:
            print_registration(f"Connected to reliable server at {RELIABLE_SERVER_IP}:{RELIABLE_SERVER_PORT}")
            request_message = create_message(COMPONENT_ID, "request_state")
            send(reliable_socket, request_message, COMPONENT_ID, "Reliable Server")
            response = receive(reliable_socket, "Reliable Server", COMPONENT_ID)
            if response and response.get("message") == "state_response":
                state = response.get("state", state)
                print_registration(f"State synchronized with reliable server. New state: {state}")
            else:
                print_warning("No valid state response received from reliable server.")
    except Exception as e:
        print_disconnection(f"Error during state synchronization: {e}")

def flush_message_queue():
    """Sends all responses in the message queue."""
    while not message_queue.empty():
        try:
            client_socket, response = message_queue.get_nowait()
            send(client_socket, response, COMPONENT_ID, f"Client@{clients.get(client_socket, 'Unknown')}")
        except Exception as e:
            print_disconnection(f"Error sending message from queue: {e}")

def disconnect_client(client_socket):
    """Disconnects a client and removes it from the active clients list."""
    client_address = clients.pop(client_socket, None)
    if client_address:
        print_disconnection(f"Client disconnected: {client_address}")
    client_socket.close()

def main():
    global state

    isReliableServer = COMPONENT_ID == "S1"
    connect_to_lfd()
    if not isReliableServer:
        synchronize_state()

    server_socket = initialize_component(COMPONENT_ID, "Server", SERVER_IP, SERVER_PORT, 5)

    if isReliableServer:
        reliable_server_socket = initialize_component(COMPONENT_ID, "Reliable Server", RELIABLE_SERVER_IP, RELIABLE_SERVER_PORT, 5)

    threading.Thread(target=handle_heartbeat, daemon=True).start()

    try:
        while True:
            accept_new_connections(server_socket)
            process_client_messages()
            flush_message_queue()
            if isReliableServer:
                accept_new_connections(reliable_server_socket)
            time.sleep(0.1)
    except KeyboardInterrupt:
        print_warning("Server shutting down.")
    finally:
        if lfd_socket:
            lfd_socket.close()
        for client in list(clients.keys()):
            disconnect_client(client)
        server_socket.close()
        if isReliableServer:
            reliable_server_socket.close()
        print_disconnection("Server terminated.")

if __name__ == '__main__':
    main()
