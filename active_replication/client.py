import socket
import json
import time
from collections import defaultdict

# Define color functions for printing
def print_sent(skk): print("\033[96m{}\033[00m".format(skk))  # Cyan for sent messages
def print_received(skk): print("\033[95m{}\033[00m".format(skk))  # Purple for received messages
def printR(skk): print("\033[91m{}\033[00m".format(skk))  # Red for errors
def printY(skk): print("\033[93m{}\033[00m".format(skk))  # Yellow for warnings
def printG(skk): print("\033[92m{}\033[00m".format(skk))  # Green for registrations

# List of server IPs
SERVER_IPS = ['127.0.0.1', '127.0.0.2', '127.0.0.3']  # Replace with actual server IPs

class Client:
    def __init__(self, server_port, client_id):
        self.server_ips = SERVER_IPS
        self.server_port = server_port
        self.client_id = client_id
        self.sockets = {}
        self.request_number = 0
        self.server_responses = defaultdict(list)

    def create_message(self, message_type, **kwargs):
        """Creates a standard message with client_id and timestamp."""
        return {
            "client_id": self.client_id,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "message": message_type,
            **kwargs
        }

    def format_message_log(self, message, sender, receiver, sent=True):
        """Formats the message log for structured printing."""
        message_type = message.get("message", "Unknown")
        timestamp = message.get("timestamp", "Unknown")
        details = {k: v for k, v in message.items() if k not in ["client_id", "timestamp", "message"]}
        color_fn = print_sent if sent else print_received
        color_fn("====================================================================")
        color_fn(f"{sender} → {receiver} ({message_type}) at {timestamp}")
        if details:
            for key, value in details.items():
                color_fn(f"  {key}: {value}")
        color_fn("====================================================================")

    def send_message(self, sock, message, receiver):
        """Sends a message through the provided socket."""
        try:
            sock.sendall(json.dumps(message).encode())
            self.format_message_log(message, self.client_id, receiver, sent=True)
        except socket.error as e:
            printR(f"Failed to send message to {receiver}: {e}")

    def receive_message(self, sock, sender):
        """Receives a message from the provided socket."""
        try:
            data = sock.recv(1024).decode()
            message = json.loads(data)
            self.format_message_log(message, sender, self.client_id, sent=False)
            return message
        except (socket.error, json.JSONDecodeError) as e:
            printR(f"Failed to receive or decode message from {sender}: {e}")
            return None

    def connect(self):
        """Establish connections to all servers."""
        for ip in self.server_ips:
            self.attempt_connection(ip)

    def attempt_connection(self, ip):
        """Attempt to connect to a specific server IP."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((ip, self.server_port))
            self.sockets[ip] = sock
            printG(f"Connected to server at {ip}:{self.server_port}")
        except Exception as e:
            printR(f"Failed to connect to server {ip}: {e}")
            if ip in self.sockets:
                self.sockets.pop(ip, None)

    def reconnect(self):
        """Attempt to reconnect to servers that are not connected."""
        for ip in self.server_ips:
            if ip not in self.sockets:
                printY(f"Attempting to reconnect to server {ip}...")
                self.attempt_connection(ip)

    def send_to_all_servers(self, message_content):
        """Send a message to all connected servers."""
        message = self.create_message(message_content)
        for ip, sock in self.sockets.items():
            self.send_message(sock, message, f"Server@{ip}")

    def receive_from_all_servers(self):
        """Receive responses from all servers."""
        for ip, sock in self.sockets.items():
            self.receive_message(sock, f"Server@{ip}")

    def close_connections(self):
        """Close all connections."""
        for ip, sock in self.sockets.items():
            sock.close()
            printY(f"Connection to server {ip} closed.")
