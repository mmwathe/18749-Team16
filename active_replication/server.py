import socket
import time
from queue import Queue
import threading
import os, sys
import errno
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))
from communication_utils import *
from dotenv import load_dotenv

load_dotenv()

# Global Configurations
COMPONENT_ID = os.environ.get("MY_SERVER_ID")
SERVER_IP = '0.0.0.0'
SERVER_PORT = 12346
LFD_ID = os.environ.get("MY_LFD_ID")
LFD_IP = '127.0.0.1'
LFD_PORT = 54321

# NOTE: Might have to hardcode the reliable server IP
RELIABLE_SERVER_IP = None
RELIABLE_SERVER_PORT = 12351
MY_IP = os.environ.get(COMPONENT_ID)
SERVER_IPS = [
    os.environ.get("S1"),
    os.environ.get("S2"),
    os.environ.get("S3")  # Adjust to actual server IPs
]
state = 0
lfd_socket = None
clients = {}
message_queue = Queue()

def connect_to_lfd():
    global lfd_socket
    try:
        lfd_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lfd_socket.connect((LFD_IP, LFD_PORT))
        printG(f"Connected to LFD at {LFD_IP}:{LFD_PORT}")
        registration_message = create_message(COMPONENT_ID, "register")
        send(lfd_socket, registration_message, LFD_ID)
    except Exception as e:
        printR(f"Failed to connect to LFD: {e}")
        lfd_socket = None

def handle_heartbeat():
    global RELIABLE_SERVER_IP
    while True:
        if lfd_socket:
            message = receive(lfd_socket, COMPONENT_ID)
            if message and message.get("message") == "heartbeat":
                heartbeat_message = create_message(COMPONENT_ID, "heartbeat acknowledgment")
                send(lfd_socket, heartbeat_message, LFD_ID)
            elif message and message.get("message") == "new_reliable":
                if(message.get("server_id") != None):
                    RELIABLE_SERVER_IP = SERVER_IPS[int(message.get("server_id")[-1])-1]
                else:
                    print("message was none")
                
        time.sleep(1)

def handle_request_state(client_socket):
    global state
    response = create_message(COMPONENT_ID, "state_response", state=state)
    send(client_socket, response, "Requesting Server")

def accept_new_connections_reliable(server_socket):
    # Non-blocking mode
    try:
        client_socket, client_address = server_socket.accept()
        printG(f"Connection established: {client_address}")
        # Handle request_state message if received
        message = receive(client_socket, COMPONENT_ID)
        if message and message.get("message") == "request_state":
            handle_request_state(client_socket)
        else:
            clients[client_socket] = client_address
    except BlockingIOError:
        pass
    except Exception as e:
        printR(f"Error accepting client connection: {e}")
    server_socket.setblocking(False)

def accept_new_connections(server_socket):
    server_socket.setblocking(False)  # Set the socket to non-blocking mode
    try:
        client_socket, client_address = server_socket.accept()
        printG(f"Client connected: {client_address}")
        clients[client_socket] = client_address
    except BlockingIOError:
        # No new connections, move on
        pass
    except Exception as e:
        printR(f"Error accepting client connection: {e}")

def process_client_messages():
    global state
    for client_socket in list(clients.keys()):
        try:
            client_socket.setblocking(False)  # Allow non-blocking mode for receiving
            message = receive(client_socket, COMPONENT_ID)
            if not message:  # If no message is received, skip further processing
                continue

            message_type = message.get("message", "unknown")
            request_number = message.get("request_number", "unknown")
            component_id = message.get("component_id", "unknown")

            if message_type == "increase":
                state += 1
                response = create_message(COMPONENT_ID, "state increased", state=state, request_number=request_number)
            elif message_type == "decrease":
                state -= 1
                response = create_message(COMPONENT_ID, "state decreased", state=state, request_number=request_number)
            else:
                printY(f"Unknown message type: {message_type}")
            
            message_queue.put((client_socket, response))
        except BlockingIOError:
            # No data available for now; skip processing this socket
            continue
        except Exception as e:
            printR(f"Error processing client message: {e}")
            disconnect_client(client_socket)

def synchronize_state():
    global state
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)  # Set a strict timeout for the connection
        sock.connect((RELIABLE_SERVER_IP, RELIABLE_SERVER_PORT))
        printG(f"Connected to reliable server at {RELIABLE_SERVER_IP}:{RELIABLE_SERVER_PORT}")

        # Send request_state message
        request_message = create_message(COMPONENT_ID, "request_state")
        send(sock, request_message, "Reliable Server")

        # Receive the state from the reliable server with a timeout
        sock.settimeout(2)  # Timeout for receiving the state response
        response = receive(sock, COMPONENT_ID)
        if response and response.get("message") == "state_response":
            state = response.get("state", state)
            printG(f"State synchronized with reliable server. New state: {state}")
        else:
            printY("No valid state response received from reliable server.")
    except socket.timeout:
        printY(f"Synchronization timed out while waiting for the reliable server at {RELIABLE_SERVER_IP}:{RELIABLE_SERVER_PORT}")
    except socket.error as e:
        if e.errno in (errno.ECONNREFUSED, errno.ETIMEDOUT):
            printY("Reliable server unavailable. Skipping synchronization.")
        else:
            printR(f"Socket error during synchronization: {e}")
    except Exception as e:
        printR(f"Failed to synchronize with reliable server: {e}")
    finally:
        sock.close() 

def flush_message_queue():
    while not message_queue.empty():
        try:
            client_socket, response = message_queue.get_nowait()
            send(client_socket, response, f"Client@{clients[client_socket]}")
        except KeyError:
            printR("Attempted to send message to a disconnected client.")
        except Exception as e:
            printR(f"Error sending message from queue: {e}")

def disconnect_client(client_socket):
    client_address = clients.pop(client_socket, None)
    if client_address:
        printR(f"Client disconnected: {client_address}")
    client_socket.close()

def main():
    isReliableServer = COMPONENT_ID == "S1"
    
    connect_to_lfd()
    threading.Thread(target=handle_heartbeat, daemon=True).start()
    time.sleep(2)
    if (RELIABLE_SERVER_IP != None and RELIABLE_SERVER_IP != MY_IP): 
        print(RELIABLE_SERVER_IP)
        print("hi")
        synchronize_state()

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((SERVER_IP, SERVER_PORT))
    server_socket.listen(5)
    printG(f"Server listening on {SERVER_IP}:{SERVER_PORT}")

    
    server_socket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket2.bind((MY_IP, RELIABLE_SERVER_PORT))
    server_socket2.listen(5)

    # Start the heartbeat thread

    try:
        while True:
            accept_new_connections(server_socket)  # Non-blocking, checks for connections
            process_client_messages()  # Process any client messages
            accept_new_connections_reliable(server_socket2)
            flush_message_queue()  # Send responses to clients
            time.sleep(0.1)  # Prevent high CPU usage
    except KeyboardInterrupt:
        printY("Server shutting down.")
    finally:
        if lfd_socket:
            lfd_socket.close()
        for client in list(clients.keys()):
            disconnect_client(client)
        server_socket.close()
        printR("Server terminated.")
if __name__ == '__main__':
    main()