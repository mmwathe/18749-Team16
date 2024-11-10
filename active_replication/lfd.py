import socket
import time
import json
import argparse
import os
from dotenv import load_dotenv

# Define color functions for printing with enhanced formatting
def printG(skk): print(f"\033[92m{skk}\033[00m")         # Green
def printR(skk): print(f"\033[91m{skk}\033[00m")         # Red
def printY(skk): print(f"\033[93m{skk}\033[00m")         # Yellow
def printLP(skk): print(f"\033[94m{skk}\033[00m")        # Light Purple
def printP(skk): print(f"\033[95m{skk}\033[00m")         # Purple
def printC(skk): print(f"\033[96m{skk}\033[00m")         # Cyan

COMPONENT_ID = "LFD1"
LFD_IP = '0.0.0.0'
LFD_PORT = 54321
GFD_IP = '172.26.66.176'
GFD_PORT = 12345
heartbeat_interval = 4
gfd_socket = None
server_socket = None
server_connected = False
server_id = None

def create_message(message_type, **kwargs):
    """Creates a standard message with component_id and timestamp."""
    message = {
        "component_id": COMPONENT_ID,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        "message": message_type
    }
    message.update(kwargs)
    return message

def wait_for_server():
    """Waits for a server connection and sends a registration message to GFD."""
    global server_socket, server_connected, server_id
    lfd_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    lfd_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    lfd_socket.bind((LFD_IP, LFD_PORT))
    lfd_socket.listen(1)

    printY(f"LFD waiting for server connection on {LFD_IP}:{LFD_PORT}...")
    try:
        server_socket, server_address = lfd_socket.accept()
        printG(f"Server connected from {server_address}")
        server_connected = True
        server_id = f"{server_address}"  # Default to address until we get an actual server ID
        notify_gfd("add replica", {"server_id": "S1"})
    except Exception as e:
        printR(f"Failed to accept server connection: {e}")

def connect_to_gfd():
    """Establishes a persistent connection to the GFD."""
    global gfd_socket
    try:
        gfd_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        gfd_socket.connect((GFD_IP, GFD_PORT))
        printG(f"Connected to GFD at {GFD_IP}:{GFD_PORT}")
        return True
    except Exception as e:
        printR(f"Failed to connect to GFD: {e}")
        return False

def notify_gfd(event_type, event_data):
    """Sends a notification message to GFD with event details."""
    global gfd_socket
    if not gfd_socket:
        printR("GFD is not connected. Cannot send notification.")
        return

    message = create_message(event_type, message_data=event_data)
    try:
        gfd_socket.sendall(json.dumps(message).encode())
        printC(f"Sent '{event_type}' notification to GFD: {message}")
    except socket.error as e:
        printR(f"Failed to send notification to GFD: {e}")

def send_heartbeat_to_server():
    """Sends heartbeat messages to the server at regular intervals."""
    global server_socket
    message = create_message("heartbeat")
    try:
        printY(f"Sending heartbeat to server: {message}")
        server_socket.sendall(json.dumps(message).encode())
    except socket.error as e:
        printR(f"Failed to send heartbeat to server: {e}")

def receive_heartbeat_from_gfd():
    """Receives heartbeat messages from the GFD and sends acknowledgment."""
    global gfd_socket
    try:
        data = gfd_socket.recv(1024).decode()
        message = json.loads(data)
        if message.get('message') == 'heartbeat':
            printC(f"Received heartbeat from GFD: {message}")
            acknowledgment = create_message("heartbeat acknowledgment")
            gfd_socket.sendall(json.dumps(acknowledgment).encode())
            printG("Sent heartbeat acknowledgment to GFD.")
    except (socket.error, json.JSONDecodeError) as e:
        printR(f"Failed to receive or respond to heartbeat from GFD: {e}")

def monitor_server():
    """Monitors the server connection, sends heartbeats, and receives responses."""
    global server_connected, server_socket
    if server_connected:
        send_heartbeat_to_server()
        response = receive_response_from_server()

        if response is None:
            printR("Server did not respond to the heartbeat.")
            notify_gfd("remove replica", {"server_id": server_id})
            server_socket.close()
            server_connected = False
    else:
        wait_for_server()

def receive_response_from_server():
    """Receives and processes the server's response to heartbeat messages."""
    global server_socket, server_id
    try:
        response = server_socket.recv(1024).decode()
        response_data = json.loads(response)
        server_id = response_data.get('server_id', server_id)
        timestamp = response_data.get('timestamp', "Unknown")
        message = response_data.get('message', "Unknown")
        state = response_data.get('state', "Unknown")

        printP("=" * 80)
        printY(f"{timestamp:<20} {server_id} -> {COMPONENT_ID}")
        printLP(f"{'':<20} {'Message:':<15} {message}")
        printLP(f"{'':<20} {'State:':<15} {state}")

        return response
    except (socket.error, json.JSONDecodeError):
        printR("No response received from server. Server might be down.")
        return None

def main():
    global heartbeat_interval

    parser = argparse.ArgumentParser(description="Local Fault Detector (LFD) for monitoring server health.")
    parser.add_argument('--heartbeat_freq', type=int, default=4,
                        help="Frequency of heartbeat messages in seconds (default: 4 seconds).")
    args = parser.parse_args()

    CLIENT_ID = os.getenv('LFDID')

    LFD_IP = '0.0.0.0'  # LFD listens on all interfaces
    LFD_PORT = 54321  # LFD listens on this port

    GFD_IP = os.getenv('GFDIP')
    GFD_PORT = 12345

    # Create an instance of LFD with the specified heartbeat frequency
    lfd = LFD(LFD_IP, LFD_PORT, GFD_IP, GFD_PORT, CLIENT_ID, heartbeat_interval=args.heartbeat_freq)

    # Connect to the GFD
    if not lfd.connect_to_gfd():
        return

    try:
        while True:
            receive_heartbeat_from_gfd()
            monitor_server()
            time.sleep(0.5)  # Prevents high CPU usage
    except KeyboardInterrupt:
        printY("LFD interrupted by user.")
    finally:
        close_connections()

def close_connections():
    """Closes all active connections and shuts down the LFD."""
    global server_socket, gfd_socket
    if server_socket:
        notify_gfd("remove replica", {"server_id": server_id, "reason": "LFD shutting down"})
        server_socket.close()
    if gfd_socket:
        gfd_socket.close()
    printR("LFD shutdown.")

if __name__ == '__main__':
    main()