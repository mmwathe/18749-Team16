import socket
import time
import json
import argparse
import threading
import os

# Define color functions for printing with enhanced formatting
def print_sent(skk): print(f"\033[96m{skk}\033[00m")  # Cyan for sent messages
def print_received(skk): print(f"\033[95m{skk}\033[00m")  # Green for received messages
def printR(skk): print(f"\033[91m{skk}\033[00m")  # Red for errors
def printY(skk): print(f"\033[93m{skk}\033[00m")  # Yellow for warnings
def printG(skk): print(f"\033[92m{skk}\033[00m")         # Green


# Global Configurations
COMPONENT_ID = os.environ.get("MY_LFD_ID")
LFD_IP = '127.0.0.1'
LFD_PORT = 54321
GFD_IP = os.environ.get("GFD_IP")
GFD_PORT = 12345
heartbeat_interval = 4
timeout_threshold = 10  # Time in seconds to wait for a response before marking server as "dead"
gfd_socket = None
server_socket = None
server_id = None

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
        printR(f"Failed to send message to {receiver}: {e}")

def receive_message(sock, sender):
    """Receives a message from the provided socket."""
    try:
        sock.settimeout(timeout_threshold)  # Set a timeout for receiving messages
        data = sock.recv(1024).decode()
        sock.settimeout(None)  # Reset the timeout
        message = json.loads(data)
        format_message_log(message, sender, COMPONENT_ID, sent=False)
        return message
    except socket.timeout:
        printR(f"Timed out waiting for response from {sender}.")
        return None
    except (socket.error, json.JSONDecodeError) as e:
        printR(f"Failed to receive or decode message from {sender}: {e}")
        return None

def register_with_gfd():
    """Registers LFD with GFD."""
    if gfd_socket:
        send_message(gfd_socket, create_message("register"), "GFD")

def handle_server_registration():
    """Processes the server registration message."""
    global server_id
    message = receive_message(server_socket, "Server")
    if message and message.get('message') == 'register':
        server_id = message.get('component_id', 'Unknown Server')
        printG(f"Server {server_id} registered with LFD.")
        notify_gfd("add replica", {"server_id": server_id})

def handle_server_communication():
    """Handles ongoing server communication, including heartbeats."""
    global server_socket
    while True:
        send_message(server_socket, create_message("heartbeat"), server_id)
        response = receive_message(server_socket, server_id)
        if not response:
            printR(f"Server {server_id} is unresponsive. Marking as dead and notifying GFD.")
            notify_gfd("remove replica", {"server_id": server_id})
            server_socket.close()
            break
        time.sleep(heartbeat_interval)

def connect_to_gfd():
    """Establishes a persistent connection to the GFD and sends registration."""
    global gfd_socket
    try:
        gfd_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        gfd_socket.connect((GFD_IP, GFD_PORT))
        printG(f"Connected to GFD at {GFD_IP}:{GFD_PORT}")
        register_with_gfd()
    except Exception as e:
        printR(f"Failed to connect to GFD: {e}")

def notify_gfd(event_type, event_data):
    """Sends a notification message to GFD."""
    if gfd_socket:
        send_message(gfd_socket, create_message(event_type, message_data=event_data), "GFD")

def wait_for_server():
    """Waits for a server connection."""
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

def receive_heartbeat_from_gfd():
    """Handles heartbeat messages from GFD."""
    if gfd_socket:
        message = receive_message(gfd_socket, "GFD")
        if message and message.get("message") == "heartbeat":
            send_message(gfd_socket, create_message("heartbeat acknowledgment"), "GFD")

def main():
    global heartbeat_interval
    parser = argparse.ArgumentParser(description="Local Fault Detector (LFD) for monitoring server health.")
    parser.add_argument('--heartbeat_freq', type=int, default=4, help="Heartbeat frequency in seconds.")
    args = parser.parse_args()
    heartbeat_interval = args.heartbeat_freq

    connect_to_gfd()

    server_thread = threading.Thread(target=wait_for_server, daemon=True)
    server_thread.start()

    try:
        while True:
            receive_heartbeat_from_gfd()
            time.sleep(0.5)
    except KeyboardInterrupt:
        printY("LFD interrupted by user.")
    finally:
        if server_socket:
            notify_gfd("remove replica", {"server_id": server_id, "reason": "LFD shutting down"})
            server_socket.close()
        if gfd_socket:
            gfd_socket.close()
        printR("LFD shutdown.")

if __name__ == '__main__':
    main()