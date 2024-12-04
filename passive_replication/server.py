import socket
import time
import threading
import os, sys
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
    'S1': os.environ.get("S1"),
    'S2': os.environ.get("S2"),
    'S3': os.environ.get("S3"),
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
    """Connect to the LFD and register."""
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
    """Respond to heartbeats from the LFD."""
    while True:
        try:
            if lfd_socket:
                message = receive(lfd_socket, COMPONENT_ID)
                if message and message.get("message") == "heartbeat":
                    heartbeat_message = create_message(COMPONENT_ID, "heartbeat acknowledgment")
                    send(lfd_socket, heartbeat_message, "LFD")
        except Exception as e:
            printR(f"Heartbeat handling error: {e}")
        time.sleep(1)


def determine_role():
    """Determine whether the server is primary or backup."""
    global role, PRIMARY_SERVER_ID
    for server_id, server_ip in SERVER_IPS.items():
        if server_id != COMPONENT_ID:
            try:
                sock = connect_to_socket(server_ip, CHECKPOINT_PORT, timeout=2)
                if sock:
                    # A primary is active
                    PRIMARY_SERVER_ID = server_id
                    role = 'backup'
                    printG(f"Server {COMPONENT_ID} starting as backup. Detected primary: {PRIMARY_SERVER_ID}")
                    sock.close()
                    return
            except:
                continue
    # No active primary detected; promote self
    role = 'primary'
    PRIMARY_SERVER_ID = COMPONENT_ID
    printG(f"Server {COMPONENT_ID} starting as primary.")


def monitor_primary():
    """Monitor the health of the primary server and promote if necessary."""
    global role, PRIMARY_SERVER_ID
    while role == 'backup':
        primary_ip = SERVER_IPS[PRIMARY_SERVER_ID]
        try:
            sock = connect_to_socket(primary_ip, CHECKPOINT_PORT, timeout=2)
            if sock:
                # Successfully connected; primary is alive
                sock.close()
                time.sleep(2)  # Check periodically
            else:
                raise Exception("Primary unresponsive")
        except:
            # Promote self if primary is down
            with threading.Lock():
                if role == 'backup':  # Ensure only one backup promotes
                    role = 'primary'
                    PRIMARY_SERVER_ID = COMPONENT_ID
                    printG(f"Server {COMPONENT_ID} promoted to primary.")
                    threading.Thread(target=send_checkpoint, daemon=True).start()
                    break


def accept_client_connections(server_socket):
    """Accept client connections and handle them if primary."""
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
    """Handle client requests if primary."""
    global state
    try:
        while True:
            message = receive(client_socket, COMPONENT_ID)
            if not message:
                printY("Client disconnected.")
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


def send_checkpoint():
    """Send checkpoints to backup servers if primary."""
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
    determine_role()
    connect_to_lfd()
    threading.Thread(target=handle_heartbeat, daemon=True).start()

    client_socket = initialize_component(COMPONENT_ID, "Client Handler", SERVER_IP, SERVER_PORT, 5)
    checkpoint_socket = initialize_component(COMPONENT_ID, "Checkpoint Handler", SERVER_IP, CHECKPOINT_PORT, 5)

    threading.Thread(target=accept_client_connections, args=(client_socket,), daemon=True).start()
    threading.Thread(target=accept_checkpoint_connections, args=(checkpoint_socket,), daemon=True).start()
    if role == 'backup':
        synchronize_with_primary()
        threading.Thread(target=monitor_primary, daemon=True).start()

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


