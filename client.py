import socket
import json
from message import Message

class Client:
    def __init__(self, server_ip, server_port, client_id):
        self.server_ip = server_ip
        self.server_port = server_port
        self.client_id = client_id
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def connect(self):
        try:
            self.socket.connect((self.server_ip, self.server_port))
            print(f"Connected to server at {self.server_ip}:{self.server_port}")
        except Exception as e:
            print(f"Failed to connect to server: {e}")
            return False
        return True

    def send_message(self, message_content):
        message = Message(self.client_id, message_content)
        message_json = json.dumps({
            "timestamp": message.timestamp,
            "client_id": message.client_id,
            "message": message.message,
            "message_id": message.message_id
        })
        print("=========================================================\n" + 
              f"Sending message to server: {message_json}" + "\n")
        self.socket.sendall(message_json.encode())

    def receive_response(self):
        try:
            response = self.socket.recv(1024).decode()
            print("=========================================================\n" + 
                  f"Received from server: {response}" + "\n")
            return response
        except Exception as e:
            print(f"Failed to receive response: {e}")
            return None

    def close_connection(self):
        self.socket.close()
        print("Client shutdown.")

