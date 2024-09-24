import socket
import json
import time
import uuid
from queue import Queue, Empty

def prGreen(skk): print("\033[92m{}\033[00m".format(skk))
def prRed(skk): print("\033[91m{}\033[00m".format(skk))
def prGreen(skk): print("\033[92m{}\033[00m".format(skk))
def prYellow(skk): print("\033[93m{}\033[00m".format(skk))
def prLightPurple(skk): print("\033[94m{}\033[00m".format(skk))
def prPurple(skk): print("\033[95m{}\033[00m".format(skk))
def prCyan(skk): print("\033[96m{}\033[00m".format(skk))

class Server:
    def __init__(self, server_ip, server_port, server_id):
        self.server_ip = server_ip
        self.server_port = server_port
        self.server_id = server_id
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setblocking(False)
        self.message_queue = Queue()
        self.clients = {}
        self.state = 0

    def start(self):
        try:
            self.server_socket.bind((self.server_ip, self.server_port))
            self.server_socket.listen(3)  # Listen for up to 3 simultaneous connections (this can be changed in the future)
            print(f"Server listening on {self.server_ip}:{self.server_port}")
        except Exception as e:
            print(f"Failed to start server: {e}")
            return False
        return True

    def accept_new_connection(self):
        try:
            client_socket, client_address = self.server_socket.accept()
            client_socket.setblocking(False)  # Non-blocking mode for client
            self.clients[client_socket] = client_address
            prGreen(f"New connection established with {client_address}")
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
            message_content = message.get('message', 'Unknown')
            message_id = message.get('message_id', 'Unknown')
            
            prPurple("=========================================================")
            prYellow(f"Received message from Client {client_id} at {timestamp}:")
            prLightPurple(f"Message: {message_content}")
            prLightPurple(f"Message ID: {message_id}")

            # Handle the message content and update state if necessary
            state_before = self.state
            response = {
                "message": "",
                "server_id": self.server_id,
                "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                "state_before": state_before,
                "state_after": self.state
            }

            if message_content.lower() == 'ping':
                response["message"] = "pong"
                prGreen("Sending 'pong' response...")
            elif message_content.lower() == 'update':
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
        print(f"Client {client_address} disconnected.")
        client_socket.close()
        del self.clients[client_socket]

    def close_server(self):
        for client_socket in list(self.clients):
            self.disconnect_client(client_socket)
        self.server_socket.close()
        print("Server shutdown.")

def main():
    SERVER_IP = '0.0.0.0'  # Listen on all available interfaces
    SERVER_PORT = 12345
    SERVER_ID = str(uuid.uuid4())  # Unique Server ID

    server = Server(SERVER_IP, SERVER_PORT, SERVER_ID)

    if not server.start():
        return

    print(f"Server ID: {SERVER_ID}")

    try:
        while True:
            # Accept new connections
            server.accept_new_connection()
            
            # Receive messages from clients
            server.receive_messages()
            
            # Process all messages in the queue
            server.process_messages()
            
            # Sleep briefly to avoid high CPU usage
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("Server is shutting down...")
    finally:
        server.close_server()

if __name__ == '__main__':
    main()