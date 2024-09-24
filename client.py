import socket
import time
from datetime import datetime
import uuid
import json
from dotenv import load_dotenv
import os

class Message:
    def __init__(self, client_id, message):
        self.timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.client_id = client_id
        self.message = message
        self.message_id = str(uuid.uuid4())  # Generate a unique message ID

    def __str__(self):
        return f"[{self.timestamp}] Client {self.client_id} (ID: {self.message_id}): {self.message}"


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
        print(f"Sending message to server: {message_json}")
        self.socket.sendall(message_json.encode())

    def receive_response(self):
        try:
            response = self.socket.recv(1024).decode()
            print(f"Received from server: {response}")
            return response
        except Exception as e:
            print(f"Failed to receive response: {e}")
            return None

    def close_connection(self):
        self.socket.close()
        print("Client shutdown.")

def main():
    SERVER_IP = os.getenv('SERVER_IP')
    SERVER_PORT = 12345
    CLIENT_ID = '1'  # Hardcoded client ID for now

    client = Client(SERVER_IP, SERVER_PORT, CLIENT_ID)

    if not client.connect():
        return

    try:
        while True:
            client.send_message("update")
            client.receive_response()
            time.sleep(2)
    except KeyboardInterrupt:
        print("Client exiting...")
    finally:
        client.send_message("exit")  # Send exit message to server
        client.close_connection()

if __name__ == '__main__':
    load_dotenv() 
    main()