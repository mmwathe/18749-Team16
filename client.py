import socket
import json
from message import Message
from collections import defaultdict

# Define color functions for printing
def prGreen(skk): print("\033[92m{}\033[00m".format(skk))
def prRed(skk): print("\033[91m{}\033[00m".format(skk))
def prYellow(skk): print("\033[93m{}\033[00m".format(skk))
def prLightPurple(skk): print("\033[94m{}\033[00m".format(skk))
def prPurple(skk): print("\033[95m{}\033[00m".format(skk))
def prCyan(skk): print("\033[96m{}\033[00m".format(skk))

SERVERS_IPS = ['172.26.105.167', '172.0.0.1', '172.26.77.220']
# SERVERS_IPS = ['172.26.117.200']


class Client:
    def __init__(self, server_port, client_id):
        self.server_port = server_port  # Client specifies the port
        self.client_id = client_id
        self.sockets = []
        self.request_number = 0
        self.server_responses = defaultdict(list)
        
        for ip in SERVERS_IPS:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sockets.append((ip, sock))

    def getServerIDfromIP(self):
        return {ip: f"{index+1}" for index, ip in enumerate(SERVERS_IPS)}

    def connect(self):
        for ip, sock in self.sockets:
            try:
                sock.connect((ip, self.server_port))  # Use client-specified port for all servers
                print(f"Connected to server at {ip}:{self.server_port}")
            except Exception as e:
                print(f"Failed to connect to server {ip}: {e}")

    def send_message(self, message_content):
        message = Message(self.client_id, message_content)
        message_json = json.dumps({
            "timestamp": message.timestamp,
            "client_id": message.client_id,
            "message": message.message,
            "message_id": message.message_id,
            "request_number": self.request_number
        })
        
        for ip, sock in self.sockets:
            try:
                prPurple("=" * 80)
                prYellow(f"{message.timestamp:<20} C{self.client_id} -> S{self.getServerIDfromIP()[ip]}")
                prLightPurple(f"{'':<20} {'Message:':<15} {message.message}")
                prGreen(f"{'':<20} {'Request number:':<15} {self.request_number}")
                self.request_number += 1
                sock.sendall(message_json.encode())
            except Exception as e:
                prRed(f"Failed to send message to server {ip}: {e}")

    def receive_response(self):
        for ip, sock in self.sockets:
            try:
                response = sock.recv(1024).decode()
                if response: 
                    message = json.loads(response)
                    server_id = message.get("server_id", "Unknown")
                    timestamp = message.get('timestamp', 'Unknown')
                    content = message.get('message', 'Unknown')
                    request_number = message.get("request_number", "Unknown")
                    state_before = message.get("state_before", "Unknown")
                    state_after = message.get("state_after", "Unknown")

                    prPurple("=" * 80)
                    prYellow(f"{timestamp:<20} {server_id} -> C{self.client_id}")

                    if request_number not in self.server_responses:
                        prLightPurple(f"{'':<20} {'Message:':<15} {content}")
                        prLightPurple(f"{'':<20} {'State updated:':<15} {state_before} -> {state_after}")
                        prGreen(f"{'':<20} {'Request number:':<15} {request_number}")
                    else:
                        prRed(f"request_num {request_number}: Discarded duplicate reply from {server_id}")
                    self.server_responses[request_number].append(response)

            except Exception as e:
                print(f"Failed to receive response from server {ip}: {e}")

    def close_connections(self):
        for ip, sock in self.sockets:
            sock.close()
            print(f"Connection to server {ip} closed.")