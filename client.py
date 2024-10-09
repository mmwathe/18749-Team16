import socket
import json
from message import Message
from collections import defaultdict

SERVER_1_IP = '172.26.86.86'
SERVER_2_IP = '172.26.117.200'
SERVER_3_IP = '172.27.240.1'

class Client:
    def __init__(self, server_port, client_id):
        self.server_port = server_port  # Client specifies the port
        self.client_id = client_id
        self.sockets = []
        self.request_number = 0
        self.server_responses = defaultdict(list)
        
        for ip in [SERVER_1_IP, SERVER_2_IP, SERVER_3_IP]:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sockets.append((ip, sock))

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
                print(f"Sending message to server {ip}: {message_json}")
                self.request_number += 1
                sock.sendall(message_json.encode())
            except Exception as e:
                print(f"Failed to send message to server {ip}: {e}")

    def receive_response(self):
        for ip, sock in self.sockets:
            try:
                response = sock.recv(1024).decode()
                message = json.loads(response)
                server_id = message.get("server_id", "Unknown")
                request_number = message.get("request_number", "Unknown")
                if request_number not in self.server_responses:
                    print(f"Received from server {ip}: {response}")
                else:
                    print(f"request_num {request_number}: Discarded duplicate reply from {server_id}")
                self.server_responses[request_number].append(response)
            except Exception as e:
                print(f"Failed to receive response from server {ip}: {e}")

    def close_connections(self):
        for ip, sock in self.sockets:
            sock.close()
            print(f"Connection to server {ip} closed.")