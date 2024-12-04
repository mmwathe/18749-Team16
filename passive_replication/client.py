import socket
import time
import os
from dotenv import load_dotenv
from communication_utils import *

load_dotenv()

# List of server IPs in order of preference
SERVER_IPS = [
    os.environ.get("S1"),
    os.environ.get("S2"),
    os.environ.get("S3")  # Adjust to actual server IPs
]

class Client:
    def __init__(self, server_port, client_id):
        self.server_ips = SERVER_IPS
        self.server_port = server_port
        self.client_id = client_id
        self.socket = None
        self.connected_server = None

    def connect_to_server(self):
        """Connect to a server from the list of IPs."""
        for ip in self.server_ips:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((ip, self.server_port))
                self.socket = sock
                self.connected_server = ip
                printG(f"Connected to server at {ip}:{self.server_port}")
                return True
            except Exception as e:
                printR(f"Failed to connect to server {ip}:{self.server_port}: {e}")
        return False

    def send_and_receive(self):
        """Send messages to the server and receive responses."""
        while self.socket:
            try:
                # Example: Send an 'update' message
                message = create_message(self.client_id, "update")
                send(self.socket, message, f"Server@{self.connected_server}")
                
                response = receive(self.socket, f"Server@{self.connected_server}")
                if not response:
                    printR("Server disconnected. Reconnecting...")
                    self.socket.close()
                    self.socket = None
                    break  # Break loop to reconnect
                time.sleep(2)  # Simulate client request frequency
            except Exception as e:
                printR(f"Error during communication: {e}")
                self.socket.close()
                self.socket = None
                break

    def run(self):
        """Run the client."""
        while True:
            if not self.socket:
                printY("Attempting to connect to a server...")
                if not self.connect_to_server():
                    printR("All servers are unreachable. Retrying in 5 seconds...")
                    time.sleep(5)
                    continue

            self.send_and_receive()
