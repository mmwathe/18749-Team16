import socket
import json
import time
from queue import Queue, Empty

# Define color functions for printing with enhanced formatting
def printG(skk): print(f"\033[92m{skk}\033[00m")         # Green
def printR(skk): print(f"\033[91m{skk}\033[00m")         # Red
def printY(skk): print(f"\033[93m{skk}\033[00m")         # Yellow
def printLP(skk): print(f"\033[94m{skk}\033[00m")        # Light Purple
def printP(skk): print(f"\033[95m{skk}\033[00m")         # Purple
def printC(skk): print(f"\033[96m{skk}\033[00m")         # Cyan

COMPONENT_ID = "S1"
SERVER_IP = '0.0.0.0'
SERVER_PORT = 12345
LFD_IP = '127.0.0.1'
LFD_PORT = 54321
state = 0
server_socket = None
lfd_socket = None
clients = {}
message_queue = Queue()

def create_message(message_type, **kwargs):
    """Creates a standard message with component_id and timestamp."""
    message = {
        "component_id": COMPONENT_ID,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        "message": message_type
    }
    message.update(kwargs)
    return message

def start_server():
    global server_socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((SERVER_IP, SERVER_PORT))
    server_socket.listen(3)
    printG(f"Server listening on {SERVER_IP}:{SERVER_PORT}")
    connect_to_lfd()

def connect_to_lfd():
    global lfd_socket
    try:
        lfd_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lfd_socket.connect((LFD_IP, LFD_PORT))
        printG(f"Connected to LFD at {LFD_IP}:{LFD_PORT}")
    except Exception as e:
        printR(f"Failed to connect to LFD: {e}")
        lfd_socket = None

def accept_new_connection():
    global server_socket, clients
    try:
        client_socket, client_address = server_socket.accept()
        client_socket.setblocking(False)
        clients[client_socket] = client_address
        printC(f"New connection established with {client_address}")
    except BlockingIOError:
        pass

def receive_messages():
    global clients, message_queue
    for client_socket in list(clients):
        try:
            data = client_socket.recv(1024).decode()
            if data:
                message_queue.put((client_socket, data))
            else:
                disconnect_client(client_socket)
        except BlockingIOError:
            pass

def process_messages():
    global message_queue
    try:
        while True:
            client_socket, data = message_queue.get_nowait()
            process_message(client_socket, data)
    except Empty:
        pass

def process_message(client_socket, data):
    global state
    try:
        message = json.loads(data)
        timestamp = message.get('timestamp', 'Unknown')
        client_id = message.get('client_id', 'Unknown')
        content = message.get('message', 'Unknown')
        message_id = message.get('message_id', 'Unknown')

        printP("=" * 80)
        if content.lower() == 'heartbeat':
            printR(f"{timestamp:<20} Received heartbeat from: {client_id}")
            response = create_message("heartbeat response", server_id=COMPONENT_ID, state=state)
            client_socket.sendall(json.dumps(response).encode())
        else:
            printY(f"{timestamp:<20} C{client_id} -> {COMPONENT_ID}")
            printLP(f"{'':<20} {'Message ID:':<15} {message_id}")
            printLP(f"{'':<20} {'Message:':<15} {content}")

            response = create_message("", server_id=COMPONENT_ID)
            if content.lower() == 'ping':
                response["message"] = "pong"
                printG(f"{'':<20} Sending 'pong' response...")
            elif content.lower() == 'update':
                state += 1
                response["message"] = "state updated"
                response["state_after"] = state
                printG(f"{'':<20} State updated to: {state}")
            else:
                response["message"] = "unknown command"
                printR(f"{'':<20} Received unknown message: {content}")

            client_socket.sendall(json.dumps(response).encode())
    except json.JSONDecodeError:
        printR("Received malformed message.")

def receive_messages_from_lfd():
    global lfd_socket
    if lfd_socket:
        try:
            data = lfd_socket.recv(1024).decode()
            if data:
                message = json.loads(data)
                printC(f"Received message from LFD: {message}")
                if message.get("message") == "heartbeat":
                    printG("Heartbeat received from LFD.")
                    acknowledgment = create_message("heartbeat acknowledgment")
                    lfd_socket.sendall(json.dumps(acknowledgment).encode())
        except (socket.error, json.JSONDecodeError):
            printR("Failed to receive or parse message from LFD.")

def disconnect_client(client_socket):
    global clients
    client_address = clients.get(client_socket, 'Unknown client')
    printR(f"Client {client_address} disconnected.")
    client_socket.close()
    del clients[client_socket]

def close_server():
    global server_socket, lfd_socket, clients
    for client_socket in list(clients):
        disconnect_client(client_socket)
    if server_socket:
        server_socket.close()
    if lfd_socket:
        lfd_socket.close()
    printR("Server shutdown.")

def main():
    start_server()

    try:
        while True:
            accept_new_connection()
            receive_messages()
            process_messages()
            receive_messages_from_lfd()
            time.sleep(2)  # Adjust as needed
    except KeyboardInterrupt:
        printY("Server is shutting down...")
    finally:
        close_server()

if __name__ == '__main__':
    main()