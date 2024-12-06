import socket
import time
import os, sys
import threading
from dotenv import load_dotenv
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))
from communication_utils import *

load_dotenv()

# List of server IPs in order of preference
SERVER_MAP = {
    '172.26.122.219': 'S1',
    '172.26.99.196': 'S2'
    #'172.26.20.148': 'S3'
}
SERVER_IPS = list(SERVER_MAP.keys())

RM_IP = 'localhost'
RM_PORT = 13579


class Client:
    def __init__(self, server_port, client_id):
        self.server_ips = SERVER_IPS
        self.server_port = server_port
        self.client_id = client_id
        self.socket = None
        self.connected_server = None
        self.request_number = 0
        self.rmsocket = None
        self.primary = -1

    def listen_to_rm(self):
        while self.rmsocket:
            try:
                message = receive(self.rmsocket, "RM")
                print(message)
                self.primary = int(message.get("primary_server")[-1])
            except Exception as e:
                printR(f"Error during communication with RM: {e}")


    def connect_to_rm(self):
        try:
             sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
             sock.connect((RM_IP, RM_PORT))
             self.rmsocket = sock
             printG("Connected to RM")
             threading.Thread(target=self.listen_to_rm, daemon=True).start()
             return True
        except Exception as e:
             printR(f"Failed to connect to RM: {e} ")
        return False

    def connect_to_server(self):
        """Connect to a server from the list of IPs."""
        ip = self.server_ips[self.primary - 1]
        try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((ip, self.server_port))
                self.socket = sock
                self.connected_server = ip
                server_id = SERVER_MAP.get(ip, "Unknown")
                printG(f"Connected to server {server_id} ({ip}:{self.server_port})")
                return True
        except Exception as e:
                printR(f"Failed to connect to server {ip}:{self.server_port}: {e}")
        return False

    def send_and_receive(self):
        """Send messages to the server and receive responses."""
        server_id = SERVER_MAP.get(self.connected_server, "Unknown")
        while self.socket:
            try:
                message = create_message(self.client_id, "increase", request_number=self.request_number)
                send(self.socket, message, server_id)
                self.request_number += 1

                response = receive(self.socket, server_id)
                if not response:
                    printR("Server disconnected. Reconnecting...")
                    self.socket.close()
                    self.socket = None
                    break
                time.sleep(2)  # Simulate client request frequency
            except Exception as e:
                printR(f"Error during communication: {e}")
                self.socket.close()
                self.socket = None
                break


    def run(self):
        self.connect_to_rm()
        """Run the client."""
        while(self.primary == -1):
            time.sleep(1)
        while True:
            if not self.socket:
                printY("Attempting to connect to a server...")
                if not self.connect_to_server():
                    printR("All servers are unreachable. Retrying in 5 seconds...")
                    time.sleep(5)
                    continue

            self.send_and_receive()
