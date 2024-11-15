import socket
import time
from queue import Queue, Empty
import threading
import os
import errno
from communication_utils import create_message, send, receive, print_log, connect_to_socket

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
LFD_ID = os.environ.get("MY_LFD_ID")
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
    try:
        lfd_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lfd_socket.connect((LFD_IP, LFD_PORT))
        print_registration(f"Connected to LFD at {LFD_IP}:{LFD_PORT}")
        registration_message = create_message(COMPONENT_ID, "register")
        send(lfd_socket, registration_message, LFD_ID)
    except Exception as e:
        print_disconnection(f"Failed to connect to LFD: {e}")
        lfd_socket = None

def handle_heartbeat():
    """Handles heartbeat messages from the LFD."""
    while True:
        if lfd_socket:
            message = receive(lfd_socket, COMPONENT_ID)
            if message and message.get("message") == "heartbeat":
                heartbeat_message = create_message(COMPONENT_ID, "heartbeat acknowledgment")
                send(lfd_socket, heartbeat_message, LFD_ID)
        time.sleep(1)

def handle_request_state(client_socket):
    """Handles a request_state message from another server."""
    global state
    response = create_message(COMPONENT_ID, "state_response", state=state)
    send(client_socket, response, "Requesting Server")

def accept_new_connections_reliable(server_socket):
    """Accepts new client or server connections."""
    # Non-blocking mode
    try:
        client_socket, client_address = server_socket.accept()
        print_registration(f"Connection established: {client_address}")
        # Handle request_state message if received
        message = receive(client_socket, COMPONENT_ID)
        if message and message.get("message") == "request_state":
            handle_request_state(client_socket)
        else:
            clients[client_socket] = client_address
    except BlockingIOError:
        pass
    except Exception as e:
        print_disconnection(f"Error accepting client connection: {e}")
    server_socket.setblocking(False)

def accept_new_connections(server_socket):
    """Accepts new client connections if available and adds them to the clients dictionary."""
    server_socket.setblocking(False)  # Set the socket to non-blocking mode
    try:
        client_socket, client_address = server_socket.accept()
        print_registration(f"Client connected: {client_address}")
        clients[client_socket] = client_address
    except BlockingIOError:
        # No new connections, move on
        pass
    except Exception as e:
        print_disconnection(f"Error accepting client connection: {e}")

def process_client_messages():
    """Processes messages from connected clients and handles responses."""
    global state
    for client_socket in list(clients.keys()):
        try:
            client_socket.setblocking(False)  # Allow non-blocking mode for receiving
            message = receive(client_socket, COMPONENT_ID)
            if not message:  # If no message is received, skip further processing
                continue
            message_type = message.get("message", "unknown")
            if message_type == "ping":
                response = create_message(COMPONENT_ID, "pong")
            elif message_type == "update":
                state += 1
                response = create_message(COMPONENT_ID, "state updated", state=state)
            else:
                response = create_message(COMPONENT_ID, "unknown command")
            # Add the response to the queue
            message_queue.put((client_socket, response))
        except BlockingIOError:
            # No data available for now; skip processing this socket
            continue
        except Exception as e:
            print_disconnection(f"Error processing client message: {e}")
            disconnect_client(client_socket)

def synchronize_state():
    """Synchronizes the state with the reliable server if available."""
    global state
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)  # Set a strict timeout for the connection
        sock.connect((RELIABLE_SERVER_IP, RELIABLE_SERVER_PORT))
        print_registration(f"Connected to reliable server at {RELIABLE_SERVER_IP}:{RELIABLE_SERVER_PORT}")

        # Send request_state message
        request_message = create_message(COMPONENT_ID, "request_state")
        send(sock, request_message, "Reliable Server")

        # Receive the state from the reliable server with a timeout
        sock.settimeout(2)  # Timeout for receiving the state response
        response = receive(sock, COMPONENT_ID)
        if response and response.get("message") == "state_response":
            state = response.get("state", state)
            print_registration(f"State synchronized with reliable server. New state: {state}")
        else:
            print_warning("No valid state response received from reliable server.")
    except socket.timeout:
        print_warning("Synchronization timed out while waiting for the reliable server.")
    except socket.error as e:
        if e.errno in (errno.ECONNREFUSED, errno.ETIMEDOUT):
            print_warning("Reliable server unavailable. Skipping synchronization.")
        else:
            print_disconnection(f"Socket error during synchronization: {e}")
    except Exception as e:
        print_disconnection(f"Failed to synchronize with reliable server: {e}")
    finally:
        sock.close() 

def flush_message_queue():
    """Sends all responses in the message queue."""
    while not message_queue.empty():
        try:
            client_socket, response = message_queue.get_nowait()
            send(client_socket, response, f"Client@{clients[client_socket]}")
        except KeyError:
            print_disconnection("Attempted to send message to a disconnected client.")
        except Exception as e:
            print_disconnection(f"Error sending message from queue: {e}")

def disconnect_client(client_socket):
    """Disconnects a client and removes it from the active clients list."""
    client_address = clients.pop(client_socket, None)
    if client_address:
        print_disconnection(f"Client disconnected: {client_address}")
    client_socket.close()

def main():
    isReliableServer = COMPONENT_ID == "S1"
    
    connect_to_lfd()
    if (not isReliableServer): synchronize_state()

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((SERVER_IP, SERVER_PORT))
    server_socket.listen(5)
    print_registration(f"Server listening on {SERVER_IP}:{SERVER_PORT}")

    if (isReliableServer):
        server_socket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket2.bind((RELIABLE_SERVER_IP, RELIABLE_SERVER_PORT))
        server_socket2.listen(5)

    # Start the heartbeat thread
    threading.Thread(target=handle_heartbeat, daemon=True).start()

    try:
        while True:
            accept_new_connections(server_socket)  # Non-blocking, checks for connections
            process_client_messages()  # Process any client messages
            if (isReliableServer): accept_new_connections_reliable(server_socket2)
            flush_message_queue()  # Send responses to clients
            time.sleep(0.1)  # Prevent high CPU usage
    except KeyboardInterrupt:
        print_warning("Server shutting down.")
    finally:
        if lfd_socket:
            lfd_socket.close()
        for client in list(clients.keys()):
            disconnect_client(client)
        server_socket.close()
        print_disconnection("Server terminated.")
if __name__ == '__main__':
    main()