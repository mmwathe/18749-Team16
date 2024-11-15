import socket
import threading
import time
from queue import Queue
import os
from communication_utils import *
from dotenv import load_dotenv

load_dotenv()

# Configuration
COMPONENT_ID = os.environ.get("MY_SERVER_ID")  # Unique ID for each server (e.g., 'S1', 'S2', ...)
SERVER_IP = '0.0.0.0'
SERVER_PORT = 12346
CHECKPOINT_PORT = 12347
PRIMARY_SERVER_ID = 'S1'  # S1 starts as the primary

SERVER_IDS = ['S1', 'S2', 'S3']  # List of all server IDs
SERVER_IPS = {
    'S1': os.environ.get("S1"),  # Replace with actual IP addresses
    'S2': os.environ.get("S2"),
    'S3': os.environ.get("S3"),
}
CHECKPOINT_INTERVAL = 10  # Checkpoint interval for primary server
LFD_IP = '127.0.0.1'
LFD_PORT = 54321

state = 0
role = 'backup'  # Initially a backup; primary role must be explicitly assigned
clients = {}
client_lock = threading.Lock()
lfd_socket = None
message_queue = Queue()

def connect_to_lfd():
    """Establishes a connection to the LFD and sends a registration message."""
    global lfd_socket
    while not lfd_socket:
        try:
            lfd_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            lfd_socket.connect((LFD_IP, LFD_PORT))
            printG(f"Connected to LFD at {LFD_IP}:{LFD_PORT}")
            registration_message = create_message(COMPONENT_ID, "register")
            send(lfd_socket, registration_message, "LFD")
        except Exception as e:
            printR(f"Failed to connect to LFD: {e}. Retrying in 5 seconds...")
            lfd_socket = None
            time.sleep(5)

def handle_heartbeat():
    """Handles heartbeat messages from the LFD."""
    while True:
        if lfd_socket:
            message = receive(lfd_socket, COMPONENT_ID)
            if message and message.get("message") == "heartbeat":
                heartbeat_message = create_message(COMPONENT_ID, "heartbeat acknowledgment")
                send(lfd_socket, heartbeat_message, "LFD")
        time.sleep(1)

# Role determination
def determine_role():
    global role
    role = 'primary' if COMPONENT_ID == PRIMARY_SERVER_ID else 'backup'
    printG(f"Server {COMPONENT_ID} starting as {role}.")

# Client communication
def accept_client_connections(server_socket):
    """Accept client connections."""
    while True:
        try:
            client_socket, client_address = server_socket.accept()
            printG(f"Client connected: {client_address}")
            with client_lock:
                clients[client_socket] = client_address
            if role == 'primary':
                threading.Thread(target=handle_client_requests, args=(client_socket,), daemon=True).start()
        except Exception as e:
            printR(f"Error accepting client connections: {e}")

def handle_client_requests(client_socket):
    """Handle client requests (only if the server is primary)."""
    global state
    try:
        while True:
            message = receive(client_socket, COMPONENT_ID)
            if not message:
                printY(f"Client disconnected.")
                break

            message_type = message.get("message")
            if message_type == "update":
                state += 1
                printG(f"State updated to {state} by client request.")
                response = create_message(COMPONENT_ID, "state updated", state=state)
                send(client_socket, response, "Client")
            elif message_type == "ping":
                response = create_message(COMPONENT_ID, "pong")
                send(client_socket, response, "Client")
            else:
                printY(f"Unknown message type: {message_type}")
    except Exception as e:
        printR(f"Error handling client request: {e}")
    finally:
        with client_lock:
            clients.pop(client_socket, None)
        client_socket.close()

# Checkpointing
def send_checkpoint():
    """Send checkpoints to backup servers (only if primary)."""
    global state
    while role == 'primary':
        time.sleep(CHECKPOINT_INTERVAL)
        for server_id, server_ip in SERVER_IPS.items():
            if server_id == COMPONENT_ID:
                continue  # Skip self
            try:
                checkpoint_socket = connect_to_socket(server_ip, CHECKPOINT_PORT)
                if checkpoint_socket:
                    checkpoint_message = create_message(COMPONENT_ID, "checkpoint", state=state)
                    send(checkpoint_socket, checkpoint_message, f"Backup {server_id}")
                    ack = receive(checkpoint_socket, f"Backup {server_id}")
                    if ack and ack.get("message") == "checkpoint_acknowledgment":
                        printG(f"Checkpoint acknowledged by {server_id}.")
                    checkpoint_socket.close()
            except Exception as e:
                printR(f"Failed to send checkpoint to {server_id}: {e}")

# Backup synchronization
def accept_checkpoint_connections(checkpoint_socket):
    """Accept checkpoints from the primary server."""
    global state
    while True:
        try:
            conn, addr = checkpoint_socket.accept()
            message = receive(conn, "Primary")
            if message and message.get("message") == "checkpoint":
                state = message.get("state", state)
                printG(f"State synchronized to {state} via checkpoint.")
                acknowledgment = create_message(COMPONENT_ID, "checkpoint_acknowledgment")
                send(conn, acknowledgment, "Primary")
            conn.close()
        except Exception as e:
            printR(f"Error accepting checkpoint: {e}")

def synchronize_with_primary():
    """Synchronize state with the primary server."""
    global state
    primary_ip, checkpoint_port = SERVER_IPS[PRIMARY_SERVER_ID], CHECKPOINT_PORT
    try:
        checkpoint_socket = connect_to_socket(primary_ip, checkpoint_port)
        if checkpoint_socket:
            sync_request = create_message(COMPONENT_ID, "request_state")
            send(checkpoint_socket, sync_request, "Primary")
            response = receive(checkpoint_socket, "Primary")
            if response and response.get("message") == "state_response":
                state = response.get("state", state)
                printG(f"State synchronized with primary: {state}")
            checkpoint_socket.close()
    except Exception as e:
        printR(f"Failed to synchronize with primary: {e}")

# Main server logic
def main():
    determine_role()

    # LFD Connection
    connect_to_lfd()

    # Sockets for client and checkpoint communication
    client_socket = initialize_component(COMPONENT_ID, "Client Handler", SERVER_IP, SERVER_PORT, 5)
    checkpoint_socket = initialize_component(COMPONENT_ID, "Checkpoint Handler", SERVER_IP, CHECKPOINT_PORT, 5)

    # Start accepting client and checkpoint connections
    threading.Thread(target=accept_client_connections, args=(client_socket,), daemon=True).start()
    threading.Thread(target=accept_checkpoint_connections, args=(checkpoint_socket,), daemon=True).start()

    # If backup, synchronize with primary
    if role == 'backup':
        synchronize_with_primary()

    # If primary, start checkpointing
    if role == 'primary':
        threading.Thread(target=send_checkpoint, daemon=True).start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        printY("Server shutting down.")
    finally:
        with client_lock:
            for conn in clients.keys():
                conn.close()
        client_socket.close()
        checkpoint_socket.close()
        printR("Server terminated.")

if __name__ == "__main__":
    main()
