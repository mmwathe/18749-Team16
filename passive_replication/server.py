import socket
import threading
import time
import os
from communication_utils import create_message, send, receive, printG, printR, printY, printP

# Global Configurations
COMPONENT_ID = os.environ.get("MY_SERVER_ID")  # 'S1', 'S2', 'S3', etc.
SERVER_IP = '0.0.0.0'
CLIENT_PORT = 12346  # Port for client connections
CHECKPOINT_PORT = 12347  # Port for checkpoint communication
LFD_IP = '127.0.0.1'
LFD_PORT = 54321

PRIMARY_ID = 'S1'  # Primary server starts as S1
SERVER_IPS = {
    'S1': os.environ.get("S1"),
    'S2': os.environ.get("S2"),
    'S3': os.environ.get("S3"),
}

BACKUP_SERVERS = {sid: (ip, CHECKPOINT_PORT) for sid, ip in SERVER_IPS.items() if sid != COMPONENT_ID}

# Global State
state = 0
role = None  # 'primary' or 'backup'
clients = {}
lfd_socket = None  # Connection to LFD

# Timers
heartbeat_interval = 2
checkpoint_interval = 10


def determine_role():
    """Determine the server's role based on its ID."""
    global role
    role = 'primary' if COMPONENT_ID == PRIMARY_ID else 'backup'
    printG(f"Server {COMPONENT_ID} starting as {role}.")


def connect_to_lfd():
    """Establish a connection to the LFD and register."""
    global lfd_socket
    while not lfd_socket:
        try:
            lfd_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            lfd_socket.connect((LFD_IP, LFD_PORT))
            printG(f"Connected to LFD at {LFD_IP}:{LFD_PORT}")
            send(lfd_socket, create_message(COMPONENT_ID, "register"), "LFD")
        except Exception as e:
            printR(f"Failed to connect to LFD: {e}. Retrying in 5 seconds...")
            lfd_socket = None
            time.sleep(5)


def respond_to_heartbeat():
    """Respond to heartbeat messages from the LFD."""
    while True:
        try:
            if lfd_socket:
                message = receive(lfd_socket, "LFD")
                if message and message.get("message") == "heartbeat":
                    send(lfd_socket, create_message(COMPONENT_ID, "heartbeat acknowledgment"), "LFD")
        except Exception as e:
            printR(f"Error responding to LFD heartbeat: {e}")
        time.sleep(heartbeat_interval)


def handle_client_connections(sock):
    """Accept and process client connections."""
    sock.setblocking(False)
    while True:
        try:
            client_socket, client_address = sock.accept()
            printG(f"Client connected: {client_address}")
            threading.Thread(target=process_client_messages, args=(client_socket,), daemon=True).start()
        except BlockingIOError:
            time.sleep(0.1)


def process_client_messages(client_socket):
    """Process messages from a connected client."""
    global state, role

    while True:
        try:
            message = receive(client_socket, f"Client@{client_socket.getpeername()}")
            if not message:
                break

            if message["message"] == "update" and role == "primary":
                state += 1
                send(client_socket, create_message(COMPONENT_ID, "state updated", state=state), f"Client@{client_socket.getpeername()}")
                send_checkpoint_to_backups()
            elif message["message"] == "ping":
                send(client_socket, create_message(COMPONENT_ID, "pong"), f"Client@{client_socket.getpeername()}")
            else:
                send(client_socket, create_message(COMPONENT_ID, "unknown command"), f"Client@{client_socket.getpeername()}")
        except Exception as e:
            printR(f"Error processing message from client: {e}")
            break

    printY(f"Client disconnected: {client_socket.getpeername()}")
    client_socket.close()


def send_checkpoint_to_backups():
    """Send the current state to backup servers."""
    global state
    for backup_id, (backup_ip, port) in BACKUP_SERVERS.items():
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((backup_ip, port))
            checkpoint_message = create_message(COMPONENT_ID, "checkpoint", state=state)
            send(sock, checkpoint_message, f"Backup Server@{backup_ip}")
            response = receive(sock, f"Backup Server@{backup_ip}")
            if response and response["message"] == "checkpoint_acknowledgment":
                printG(f"Checkpoint acknowledged by Backup Server {backup_id}")
            sock.close()
        except Exception as e:
            printR(f"Failed to send checkpoint to Backup Server {backup_id}: {e}")


def handle_checkpoint_requests(sock):
    """Accept and process checkpoint requests from primary servers."""
    sock.setblocking(False)
    while True:
        try:
            conn, addr = sock.accept()
            message = receive(conn, f"Primary Server@{addr}")
            if message and message["message"] == "checkpoint":
                global state
                state = message["state"]
                printP(f"Checkpoint received. Updated state: {state}")
                send(conn, create_message(COMPONENT_ID, "checkpoint_acknowledgment"), f"Primary Server@{addr}")
            elif message and message["message"] == "request_state":
                send(conn, create_message(COMPONENT_ID, "state_response", state=state), f"Primary Server@{addr}")
            conn.close()
        except BlockingIOError:
            time.sleep(0.1)


def main():
    global role
    determine_role()

    connect_to_lfd()
    threading.Thread(target=respond_to_heartbeat, daemon=True).start()

    # Client connection socket
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    client_socket.bind((SERVER_IP, CLIENT_PORT))
    client_socket.listen(5)
    printG(f"Listening for clients on {SERVER_IP}:{CLIENT_PORT}")

    # Checkpoint socket
    checkpoint_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    checkpoint_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    checkpoint_socket.bind((SERVER_IP, CHECKPOINT_PORT))
    checkpoint_socket.listen(5)
    printG(f"Listening for checkpoints on {SERVER_IP}:{CHECKPOINT_PORT}")

    threading.Thread(target=handle_client_connections, args=(client_socket,), daemon=True).start()
    threading.Thread(target=handle_checkpoint_requests, args=(checkpoint_socket,), daemon=True).start()

    try:
        while True:
            time.sleep(1)  # Keep the main thread alive
    except KeyboardInterrupt:
        printY("Server shutting down...")
    finally:
        client_socket.close()
        checkpoint_socket.close()
        if lfd_socket:
            lfd_socket.close()
        printR("Server terminated.")

if __name__ == "__main__":
    main()
