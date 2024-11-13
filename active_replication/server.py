import socket
import json
import time
import threading

# Define color functions for printing with enhanced formatting
def print_registration(skk): print(f"\033[92m{skk}\033[00m")  # Green for registrations and accepted connections
def print_disconnection(skk): print(f"\033[91m{skk}\033[00m")  # Red for disconnections or errors
def print_warning(skk): print(f"\033[93m{skk}\033[00m")  # Yellow for warnings or waiting
def print_sent(skk): print(f"\033[96m{skk}\033[00m")  # Cyan for sent messages
def print_received(skk): print(f"\033[95m{skk}\033[00m")  # Purple for received messages

# Global Configurations
COMPONENT_ID = "S2"
SERVER_IP = '0.0.0.0'
SERVER_PORT = 12346
LFD_IP = '127.0.0.1'
LFD_PORT = 54321
state = 0
lfd_socket = None
clients = {}

def create_message(message_type, **kwargs):
    """Creates a standard message with component_id and timestamp."""
    return {
        "component_id": COMPONENT_ID,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        "message": message_type,
        **kwargs
    }

def format_message_log(message, sender, receiver, sent=True):
    """Formats the message log for structured printing."""
    message_type = message.get("message", "Unknown")
    timestamp = message.get("timestamp", "Unknown")
    details = {k: v for k, v in message.items() if k not in ["component_id", "timestamp", "message"]}
    color_fn = print_sent if sent else print_received
    color_fn("====================================================================")
    color_fn(f"{sender} → {receiver} ({message_type}) at {timestamp}")
    if details:
        for key, value in details.items():
            color_fn(f"  {key}: {value}")
    color_fn("====================================================================")

def send_message(sock, message, receiver):
    """Sends a message through the provided socket."""
    try:
        sock.sendall(json.dumps(message).encode())
        format_message_log(message, COMPONENT_ID, receiver, sent=True)
    except socket.error as e:
        print_disconnection(f"Failed to send message to {receiver}: {e}")

def receive_message(sock, sender):
    """Receives a message from the provided socket."""
    try:
        data = sock.recv(1024).decode()
        message = json.loads(data)
        format_message_log(message, sender, COMPONENT_ID, sent=False)
        return message
    except (socket.error, json.JSONDecodeError) as e:
        print_disconnection(f"Failed to receive or decode message from {sender}: {e}")
        return None

def connect_to_lfd():
    """Establishes a connection to the LFD and sends a registration message."""
    global lfd_socket
    try:
        lfd_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lfd_socket.connect((LFD_IP, LFD_PORT))
        print_registration(f"Connected to LFD at {LFD_IP}:{LFD_PORT}")
        send_message(lfd_socket, create_message("register"), "LFD")
    except Exception as e:
        print_disconnection(f"Failed to connect to LFD: {e}")
        lfd_socket = None

def handle_heartbeat():
    """Handles heartbeat messages from the LFD."""
    while True:
        if lfd_socket:
            message = receive_message(lfd_socket, "LFD")
            if message and message.get("message") == "heartbeat":
                send_message(lfd_socket, create_message("heartbeat acknowledgment", state=state), "LFD")
        time.sleep(1)

def start_client_listener():
    """Starts listening for client connections."""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((SERVER_IP, SERVER_PORT))
    server_socket.listen(3)
    print_registration(f"Server listening on {SERVER_IP}:{SERVER_PORT}")

    while True:
        try:
            client_socket, client_address = server_socket.accept()
            print_registration(f"Client connected: {client_address}")
            clients[client_socket] = client_address
            threading.Thread(target=handle_client_communication, args=(client_socket,), daemon=True).start()
        except Exception as e:
            print_disconnection(f"Error accepting client connection: {e}")

def handle_client_communication(client_socket):
    """Handles communication with a connected client."""
    global state
    while True:
        try:
            message = receive_message(client_socket, "Client")
            if not message:
                disconnect_client(client_socket)
                break

            message_type = message.get("message", "unknown")
            if message_type == "ping":
                response = create_message("pong")
            elif message_type == "update":
                state += 1
                response = create_message("state updated", state=state)
            else:
                response = create_message("unknown command")
            send_message(client_socket, response, "Client")
        except Exception as e:
            print_disconnection(f"Error communicating with client: {e}")
            disconnect_client(client_socket)
            break

def disconnect_client(client_socket):
    """Disconnects a client and removes it from the active clients list."""
    client_address = clients.pop(client_socket, None)
    if client_address:
        print_disconnection(f"Client disconnected: {client_address}")
    client_socket.close()

def main():
    """Main server function."""
    connect_to_lfd()

    threading.Thread(target=start_client_listener, daemon=True).start()
    threading.Thread(target=handle_heartbeat, daemon=True).start()

    try:
        while True:
            time.sleep(1)  # Keep the main thread alive
    except KeyboardInterrupt:
        print_warning("Server shutting down.")
    finally:
        if lfd_socket:
            lfd_socket.close()
        for client in list(clients.keys()):
            disconnect_client(client)
        print_disconnection("Server terminated.")

if __name__ == '__main__':
    main()
