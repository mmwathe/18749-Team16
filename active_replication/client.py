from collections import defaultdict
import os
from communication_utils import *

S1 = os.environ.get("S1")
S2 = os.environ.get("S2")
S3 = os.environ.get("S3")

# List of server IPs
SERVER_IPS = [S1, S2, S3]

class Client:
    def __init__(self, server_port, client_id):
        self.server_ips = SERVER_IPS
        self.server_port = server_port
        self.client_id = client_id
        self.sockets = {}
        self.request_number = 0
        self.server_responses = defaultdict(list)

    def connect(self):
        """Establish connections to all servers."""
        for ip in self.server_ips:
            self.attempt_connection(ip)

    def attempt_connection(self, ip):
        """Attempt to connect to a specific server IP."""
        sock = connect_to_socket(ip, self.server_port)
        if sock:
            self.sockets[ip] = sock
            printG(f"Connected to server at {ip}:{self.server_port}")
        else:
            printR(f"Failed to connect to server {ip}:{self.server_port}")

    def reconnect(self):
        """Attempt to reconnect to servers that are not connected."""
        for ip in self.server_ips:
            if ip not in self.sockets:
                printY(f"Attempting to reconnect to server {ip}...")
                self.attempt_connection(ip)

    def send_to_all_servers(self, message_type, **kwargs):
        """Send a message to all connected servers."""
        message = create_message(self.client_id, message_type, **kwargs)
        for ip, sock in list(self.sockets.items()):  # Use list to avoid runtime dict changes
            try:
                send(sock, message, self.client_id, f"Server@{ip}")
            except Exception as e:
                printR(f"Error sending to server {ip}: {e}")
                self.sockets.pop(ip, None)

    def receive_from_all_servers(self):
        """Receive responses from all servers and detect duplicate states."""
        responses = []
        for ip, sock in list(self.sockets.items()):
            try:
                response = receive(sock, f"Server@{ip}", self.client_id)
                if response:
                    state = response.get("state")
                    server_id = response.get("component_id")

                    if state in self.seen_states:
                        printY(f"request_num {state}: Discarded duplicate reply from {server_id}.")
                    else:
                        self.seen_states.add(state)
                        responses.append((ip, response))
            except Exception as e:
                printR(f"Error receiving from server {ip}: {e}")
                self.sockets.pop(ip, None)
        return responses


    def close_connections(self):
        """Close all connections."""
        for ip, sock in list(self.sockets.items()):
            sock.close()
            printY(f"Connection to server {ip} closed.")
