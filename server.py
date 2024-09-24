import socket
import json
import time
import uuid
import threading
from queue import Queue, Empty

# Define color functions for printing
def prGreen(skk): print("\033[92m{}\033[00m".format(skk))
def prRed(skk): print("\033[91m{}\033[00m".format(skk))
def prYellow(skk): print("\033[93m{}\033[00m".format(skk))
def prLightPurple(skk): print("\033[94m{}\033[00m".format(skk))
def prPurple(skk): print("\033[95m{}\033[00m".format(skk))
def prCyan(skk): print("\033[96m{}\033[00m".format(skk))

class Server:
    def __init__(self, server_ip, server_port, server_id, lfd_port):
        self.server_ip = server_ip
        self.server_port = server_port
        self.lfd_port = lfd_port
        self.server_id = server_id
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setblocking(False)  # Non-blocking mode for server socket
        self.lfd_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.lfd_socket.setblocking(False)  # Non-blocking mode for LFD socket
        self.message_queue = Queue()
        self.clients = {}
        self.state = 0

    def start(self):
        try:
            # Start the LFD socket
            self.lfd_socket.bind((self.server_ip, self.lfd_port))
            self.lfd_socket.listen(1)  # Listen for LFD connection
            prGreen(f"LFD listener active on {self.server_ip}:{self.lfd_port}")

            # Start the main server socket
            self.server_socket.bind((self.server_ip, self.server_port))
            self.server_socket.listen(3)  # Listen for up to 3 simultaneous connections
            prGreen(f"Server listening on {self.server_ip}:{self.server_port}")
            
        except Exception as e:
            prRed(f"Failed to start server or LFD listener: {e}")
            return False
        return True

    def accept_new_connection(self):
        try:
            client_socket, client_address = self.server_socket.accept()
            client_socket.setblocking(False)  # Non-blocking mode for each client socket
            self.clients[client_socket] = client_address
            prGreen(f"New connection established with {client_address}")
        except BlockingIOError:
            pass  # No new connections available, continue

    def receive_messages(self):
        for client_socket in list(self.clients):
            try:
                data = client_socket.recv(1024).decode()
                if data:
                    self.message_queue.put((client_socket, data))
                else:
                    self.disconnect_client(client_socket)
            except BlockingIOError:
                pass  # No data available, continue

    def process_messages(self):
        try:
            while True:
                client_socket, data = self.message_queue.get_nowait()
                self.process_client_message(client_socket, data)
        except Empty:
            pass  # No messages in queue, continue

    def process_client_message(self, client_socket, data):
        try:
            message = json.loads(data)
            timestamp = message.get('timestamp', 'Unknown')
            client_id = message.get('client_id', 'Unknown')
            message_content = message.get('message', 'Unknown')
            message_id = message.get('message_id', 'Unknown')
            
            prPurple("=========================================================")
            prYellow(f"Received message from Client {client_id} at {timestamp}:")
            prLightPurple(f"Message: {message_content}")
            prLightPurple(f"Message ID: {message_id}")

            state_before = self.state
            response = {
                "message": "",
                "server_id": self.server_id,
                "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                "state_before": state_before,
                "state_after": self.state
            }

            # Handling messages from clients
            if message_content.lower() == 'update':
                self.state += 1
                response["message"] = "state updated"
                response["state_after"] = self.state
                prGreen(f"State updated: {state_before} -> {self.state}")
            else:
                response["message"] = "unknown command"
                prRed(f"Received unknown message: {message_content}")

            client_socket.sendall(json.dumps(response).encode())
        except json.JSONDecodeError:
            prRed(f"Received malformed message: {data}")

    def disconnect_client(self, client_socket):
        client_address = self.clients.get(client_socket, 'Unknown client')
        prRed(f"Client {client_address} disconnected.")
        client_socket.close()
        del self.clients[client_socket]

    def close_server(self):
        for client_socket in list(self.clients):
            self.disconnect_client(client_socket)
        self.server_socket.close()
        self.lfd_socket.close()
        prRed("Server shutdown.")

    def handle_client_connections(self):
        while True:
            self.accept_new_connection()
            self.receive_messages()
            self.process_messages()
            time.sleep(0.1)  # Sleep to avoid high CPU usage

    def handle_lfd(self):
        while True:
            try:
                lfd_socket, lfd_address = self.lfd_socket.accept()
                prCyan(f"Connected to LFD at {lfd_address}")
                while True:
                    try:
                        data = lfd_socket.recv(1024).decode()
                        if data:
                            message = json.loads(data)
                            if message.get('message') == 'heartbeat':
                                prRed(f"Heartbeat request from LFD1, sending heartbeat...")
                                response = {
                                    "message": "ack",
                                    "server_id": self.server_id,
                                    "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                                    "state": self.state
                                }
                                lfd_socket.sendall(json.dumps(response).encode())
                        else:
                            break
                    except json.JSONDecodeError:
                        prRed(f"Received malformed message from LFD.")
                    except socket.error:
                        prRed(f"Connection error with LFD.")
                        break
            except BlockingIOError:
                time.sleep(0.1)
            except Exception as e:
                prRed(f"Unexpected error in LFD handling: {e}")
                break

def main():
    SERVER_IP = '0.0.0.0'  # Listen on all available interfaces
    SERVER_PORT = 12345
    LFD_PORT = 49663  # Port for LFD connection
    SERVER_ID = str(uuid.uuid4())  # Unique Server ID

    server = Server(SERVER_IP, SERVER_PORT, SERVER_ID, LFD_PORT)

    if not server.start():
        return

    prCyan(f"Server ID: {SERVER_ID}")

    # Start a thread for client connections
    client_thread = threading.Thread(target=server.handle_client_connections)
    client_thread.start()

    # Start a thread for LFD handling
    lfd_thread = threading.Thread(target=server.handle_lfd)
    lfd_thread.start()

    # Wait for both threads to complete
    client_thread.join()
    lfd_thread.join()

    server.close_server()

if __name__ == '__main__':
    main()