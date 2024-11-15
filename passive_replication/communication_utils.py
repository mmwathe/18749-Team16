import socket
import json
import time

# Define color functions for printing
def printG(skk): print(f"\033[92m{skk}\033[00m")         # Green
def printR(skk): print(f"\033[91m{skk}\033[00m")         # Red
def printY(skk): print(f"\033[93m{skk}\033[00m")         # Yellow
def printLP(skk): print(f"\033[94m{skk}\033[00m")        # Light Purple
def printP(skk): print(f"\033[95m{skk}\033[00m")         # Purple
def printC(skk): print(f"\033[96m{skk}\033[00m")         # Cyan

def create_message(component_id, message_type, **kwargs):
    """Creates a standard message with component_id and timestamp."""
    return {
        "component_id": component_id,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        "message": message_type,
        **kwargs
    }

def send(sock, message, receiver, print_message=True):
    """Sends a message through the provided socket."""
    try:
        sock.sendall(json.dumps(message).encode())
        if print_message:
            print_log(message, receiver, sent=True)
    except socket.error as e:
        print(f"\033[91mFailed to send message to {receiver}: {e}\033[00m")
        raise

def receive(sock, receiver, print_message=True):
    try:
        data = sock.recv(1024).decode()
        if not data:
            return None
        message = json.loads(data)
        if print_message:
            print_log(message, receiver, sent=False)
        return message
    except (socket.error, json.JSONDecodeError) as e:
        return None

def print_log(message, receiver, sent=True):
    """Formats and prints log messages consistently."""
    message_type = message.get("message", "Unknown")
    timestamp = message.get("timestamp", "Unknown")
    sender = message.get("component_id", "Unknown")
    details = {k: v for k, v in message.items() if k not in ["component_id", "timestamp", "message"]}
    color = "\033[96m" if sent else "\033[95m"  # Cyan for sent, Purple for received
    reset = "\033[00m"
    
    print(f"{color}{sender} \u2192 {receiver} ({message_type}) at {timestamp}{reset}")
    if details:
        for key, value in details.items():
            print(f"{color}  {key}: {value}{reset}")

def connect_to_socket(ip, port, timeout=5):
    """Attempts to connect to a socket and returns the socket object."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((ip, port))
        sock.settimeout(None)  # Disable timeout after connection
        return sock
    except socket.error as e:
        print(f"\033[91mFailed to connect to {ip}:{port}: {e}\033[00m")  # Red for errors
        return None  # Red for errors
    

def initialize_component(component_id, component_name, ip, port, max_connections):
    """
    Initializes a component by setting up its socket and printing startup details.

    Args:
        component_id (str): Identifier for the component (e.g., "RM", "GFD").
        ip (str): IP address the component will bind to.
        port (int): Port the component will bind to.
        max_connections (int): Maximum number of connections the component will allow (default is 1).
    
    Returns:
        socket.socket: The initialized and bound socket.
    """
    print("=====================================================")
    print(f"      {component_name} ({component_id})       ")
    print(f"{component_id} active on {ip}:{port}")
    print("-----------------------------------------------------")

    # Create and configure the socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((ip, port))
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.listen(max_connections)

    return sock

