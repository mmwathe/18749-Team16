import socket
import argparse
import time
import threading
import os
import sys
from dotenv import load_dotenv
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))
from communication_utils import *

load_dotenv()

# Configuration
COMPONENT_ID = os.environ.get("MY_SERVER_ID")  # Unique ID for each server (e.g., 'S1', 'S2', ...)
SERVER_IP = '0.0.0.0'
SERVER_PORT = 12346
CHECKPOINT_PORT = 12347
PRIMARY_SERVER_ID = 'S1'  # Primary server starts as S1

SERVER_IDS = ['S1', 'S2', 'S3']
SERVER_IPS = {
    'S1': '172.26.122.219',
    'S2': '172.26.99.196',
    'S3': '172.26.20.148',
}

CHECKPOINT_INTERVAL = 10
LFD_IP = '127.0.0.1'
LFD_PORT = 54321

state = 0
role = 'backup'
clients = {}
client_lock = threading.Lock()
lfd_socket = None


def connect_to_lfd():
    global lfd_socket
    while not lfd_socket:
        try:
            lfd_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            lfd_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            lfd_socket.connect((LFD_IP, LFD_PORT))
            printG(f"Connected to LFD at {LFD_IP}:{LFD_PORT}")
            registration_message = create_message(COMPONENT_ID, "register")
            send(lfd_socket, registration_message, "LFD")
        except Exception as e:
            printR(f"Failed to connect to LFD: {e}. Retrying in 5 seconds...")
            lfd_socket = None
            time.sleep(5)


def handle_heartbeat():
    global role, PRIMARY_SERVER_ID
    while True:
        try:
            if lfd_socket:
                message = receive(lfd_socket, COMPONENT_ID)
                if message:
                    action = message.get("message")
                    if action == "heartbeat":
                        # Acknowledge heartbeat
                        heartbeat_message = create_message(COMPONENT_ID, "heartbeat acknowledgment")
                        send(lfd_socket, heartbeat_message, "LFD")
                    elif action == "new_primary":
                        # Promote self to primary
                        role = 'primary'
                        PRIMARY_SERVER_ID = COMPONENT_ID
                        printG(f"Server {COMPONENT_ID} promoted to primary.")
                        threading.Thread(target=send_checkpoint, daemon=True).start()
                    else:
                        printY(f"Unknown message received from LFD: {message}")
        except Exception as e:
            printR(f"Error handling heartbeat or role change: {e}")
        time.sleep(1)


def accept_client_connections(server_socket):
    while True:
        try:
            client_socket, client_address = server_socket.accept()
            printG(f"Client connected: {client_address}")
            with client_lock:
                clients[client_socket] = client_address
            threading.Thread(target=handle_client_requests, args=(client_socket,), daemon=True).start()
        except Exception as e:
            printR(f"Error accepting client connections: {e}")


def handle_client_requests(client_socket):
    global state
    try:
        while True:
            if role != 'primary':  # Only primary handles client requests
                continue
            message = receive(client_socket, COMPONENT_ID)
            if not message:
                printY("Client disconnected.")
                break

            message_type = message.get("message")
            request_number = message.get("request_number", "unknown")
            component_id = message.get("component_id", "unknown")

            if message_type == "increase":
                state += 1
                response = create_message(COMPONENT_ID, "state increased", state=state, request_number=request_number)
                send(client_socket, response, component_id)
            elif message_type == "decrease":
                state -= 1
                response = create_message(COMPONENT_ID, "state decreased", state=state, request_number=request_number)
                send(client_socket, response, component_id)
            else:
                printY(f"Unknown message type: {message_type}")
    except Exception as e:
        printR(f"Error handling client request: {e}")
    finally:
        with client_lock:
            clients.pop(client_socket, None)
        client_socket.close()


def send_checkpoint():
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


def accept_checkpoint_connections(checkpoint_socket):
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
    global state
    primary_ip, checkpoint_port = SERVER_IPS[PRIMARY_SERVER_ID], CHECKPOINT_PORT
    try:
        checkpoint_socket = connect_to_socket(primary_ip, checkpoint_port, timeout=5)
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


def main():
    global CHECKPOINT_INTERVAL
    parser = argparse.ArgumentParser(description="Server for passive replication.")
    parser.add_argument('--checkpoint_interval', type=int, default=4, help="Checkpoint interval in seconds.")
    args = parser.parse_args()
    CHECKPOINT_INTERVAL = args.checkpoint_interval

    connect_to_lfd()
    threading.Thread(target=handle_heartbeat, daemon=True).start()

    client_socket = initialize_component(COMPONENT_ID, "Client Handler", SERVER_IP, SERVER_PORT, 5)
    checkpoint_socket = initialize_component(COMPONENT_ID, "Checkpoint Handler", SERVER_IP, CHECKPOINT_PORT, 5)

    threading.Thread(target=accept_client_connections, args=(client_socket,), daemon=True).start()
    threading.Thread(target=accept_checkpoint_connections, args=(checkpoint_socket,), daemon=True).start()

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
