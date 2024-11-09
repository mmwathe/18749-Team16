import socket
import json
import time
from queue import Queue, Empty
import os
from dotenv import load_dotenv

# Define color functions for printing
def prGreen(skk): print("\033[92m{}\033[00m".format(skk))
def prRed(skk): print("\033[91m{}\033[00m".format(skk))
def prYellow(skk): print("\033[93m{}\033[00m".format(skk))
def prLightPurple(skk): print("\033[94m{}\033[00m".format(skk))
def prPurple(skk): print("\033[95m{}\033[00m".format(skk))
def prCyan(skk): print("\033[96m{}\033[00m".format(skk))

class Server:
    def __init__(self, server_ip, server_port, server_id, lfd_ip, lfd_port):
        self.server_ip = server_ip
        self.server_port = server_port
        self.server_id = server_id
        self.lfd_ip = lfd_ip
        self.lfd_port = lfd_port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setblocking(False)  # Non-blocking mode
        self.message_queue = Queue()
        self.clients = {}
        self.state = 0  # Initial state
        self.lfd_socket = None

    def start(self):
        try:
            self.server_socket.bind((self.server_ip, self.server_port))
            self.server_socket.listen(3)  # Listen for up to 3 simultaneous connections
            prGreen(f"Server listening on {self.server_ip}:{self.server_port}")
            self.connect_to_lfd()  # Try to connect to LFD on start
        except Exception as e:
            prRed(f"Failed to start server: {e}")
            return False
        return True

    def connect_to_lfd(self):
        try:
            self.lfd_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.lfd_socket.connect((self.lfd_ip, self.lfd_port))
            prGreen(f"Connected to LFD at {self.lfd_ip}:{self.lfd_port}")
        except Exception as e:
            prRed(f"Failed to connect to LFD: {e}")
            self.lfd_socket = None

    def receive_messages_from_lfd(self):
        if self.lfd_socket:
            try:
                response = self.lfd_socket.recv(1024).decode()
                if response:
                    message = json.loads(response)
                    prCyan(f"Received message from LFD: {message}")
                    timestamp = message.get('timestamp', 'Unknown')
                    content = message.get('message', 'Unknown')
                    prYellow(f"LFD -> Server: {content} at {timestamp}")

                    # Process heartbeat or other messages here
                    if content.lower() == 'heartbeat':
                        prGreen("Heartbeat received from LFD.")
                        # Optionally, send an acknowledgment back
                        response = {
                            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                            "server_id": self.server_id,
                            "message": "heartbeat acknowledgment"
                        }
                        self.lfd_socket.sendall(json.dumps(response).encode())
                        prGreen("Sent heartbeat acknowledgment to LFD.")

            except (socket.error, json.JSONDecodeError):
                prRed("Failed to receive or parse message from LFD.")

    def accept_new_connection(self):
        try:
            client_socket, client_address = self.server_socket.accept()
            client_socket.setblocking(False)  # Non-blocking mode for client
            self.clients[client_socket] = client_address
            prCyan(f"New connection established with {client_address}")
        except BlockingIOError:
            # No new connections, continue
            pass

    def receive_messages(self):
        for client_socket in list(self.clients):
            try:
                data = client_socket.recv(1024).decode()
                if data:
                    self.message_queue.put((client_socket, data))
                else:
                    self.disconnect_client(client_socket)
            except BlockingIOError:
                # No data to receive, continue
                pass

    def process_messages(self):
        try:
            while True:
                client_socket, data = self.message_queue.get_nowait()
                self.process_message(client_socket, data)
        except Empty:
            # No messages to process, continue
            pass

    def process_message(self, client_socket, data):
        try:
            # Attempt to parse the data as JSON
            message = json.loads(data)
            timestamp = message.get('timestamp', 'Unknown')
            client_id = message.get('client_id', 'Unknown')
            content = message.get('message', 'Unknown')
            message_id = message.get('message_id', 'Unknown')
            request_number = message.get("request_number", "Unknown")

            prPurple("=" * 80)

            if content.lower() == 'heartbeat':
                prRed(f"{timestamp:<20} {'Received heartbeat from:':<20} {client_id}")
                prRed(f"{'':<20} {'Sending heartbeat...'}")
                response = {
                    "message": "heartbeat response",
                    "server_id": self.server_id,
                    "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                    "state": self.state
                }

            else:
                prYellow(f"{timestamp:<20} C{client_id} -> {self.server_id}")
                prLightPurple(f"{'':<20} {'Message ID:':<15} {message_id}")
                prLightPurple(f"{'':<20} {'Message:':<15} {content}")
                prLightPurple(f"{'':<20} {'Request Number:':<15} {request_number}")

                # Handle the message content and update state if necessary
                state_before = self.state
                response = {
                    "message": "",
                    "server_id": self.server_id,
                    "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                    "state_before": state_before,
                    "state_after": self.state,
                    "request_number": request_number
                }

                if content.lower() == 'ping':
                    response["message"] = "pong"
                    prGreen(f"{'':<20} Sending 'pong' response...")
                elif content.lower() == 'update':
                    self.state += 1
                    response["message"] = "state updated"
                    response["state_after"] = self.state
                    prGreen(f"{'':<20} {'State updated:':<15} {state_before} -> {self.state}")
                else:
                    response["message"] = "unknown command"
                    prRed(f"{'':<20} {'Received unknown message:':<15} {content}")

            client_socket.sendall(json.dumps(response).encode())
        except json.JSONDecodeError:
            prRed(f"{'':<20} Received malformed message: {data}")

    def disconnect_client(self, client_socket):
        client_address = self.clients.get(client_socket, 'Unknown client')
        prRed(f"Client {client_address} disconnected.")
        client_socket.close()
        del self.clients[client_socket]

    def close_server(self):
        for client_socket in list(self.clients):
            self.disconnect_client(client_socket)
        self.server_socket.close()
        if self.lfd_socket:
            self.lfd_socket.close()
        prRed("Server shutdown.")

def main():
    SERVER_IP = '0.0.0.0'
    SERVER_PORT = 12345
    SERVER_ID = os.getenv('SERVERID')
    LFD_IP = '0.0.0.0'  # Replace with LFD IP (same machine, hence localhost)
    LFD_PORT = 54321  # Replace with LFD listening port

    server = Server(SERVER_IP, SERVER_PORT, SERVER_ID, LFD_IP, LFD_PORT)

    if not server.start():
        return

    prCyan(f"Server ID: {SERVER_ID}")

    try:
        while True:
            # Accept new connections
            server.accept_new_connection()
            
            # Receive messages from clients
            server.receive_messages()
            
            # Process all messages in the queue
            server.process_messages()

            # Receive messages from LFD
            server.receive_messages_from_lfd()

            # Sleep briefly to avoid high CPU usage
            time.sleep(2)  # Adjust the sleep time to avoid excessive CPU usage
    except KeyboardInterrupt:
        prYellow("Server is shutting down...")
    finally:
        server.close_server()

if __name__ == '__main__':
    main()