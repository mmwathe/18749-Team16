import socket
import json

class Server:
    def __init__(self, server_ip, server_port):
        self.server_ip = server_ip
        self.server_port = server_port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket = None
        self.client_address = None

    def start(self):
        try:
            self.server_socket.bind((self.server_ip, self.server_port))
            self.server_socket.listen(1)
            print(f"Server listening on {self.server_ip}:{self.server_port}")
        except Exception as e:
            print(f"Failed to start server: {e}")
            return False
        return True

    def wait_for_client(self):
        try:
            self.client_socket, self.client_address = self.server_socket.accept()
            print(f"Connection established with {self.client_address}")
        except Exception as e:
            print(f"Failed to accept client connection: {e}")
            return False
        return True

    def handle_client(self):
        try:
            while True:
                data = self.client_socket.recv(1024).decode()
                if not data:
                    break
                self.process_message(data)
                if data.lower() == 'exit':
                    break
        except Exception as e:
            print(f"Error handling client: {e}")
        finally:
            self.close_connection()

    def process_message(self, data):
        try:
            # Attempt to parse the data as JSON
            message = json.loads(data)
            timestamp = message.get('timestamp', 'Unknown')
            client_id = message.get('client_id', 'Unknown')
            message_content = message.get('message', 'Unknown')
            message_id = message.get('message_id', 'Unknown')
            
            print(f"Received message from Client {client_id} at {timestamp}:")
            print(f"Message ID: {message_id}")
            print(f"Message: {message_content}")

            # Respond to 'ping' messages
            if message_content.lower() == 'ping':
                print("Sending 'pong' response...")
                self.client_socket.sendall("pong".encode())
        except json.JSONDecodeError:
            print(f"Received unknown message: {data}")

    def close_connection(self):
        if self.client_socket:
            self.client_socket.close()
            print("Client connection closed.")
        self.server_socket.close()
        print("Server shutdown.")

def main():
    SERVER_IP = '0.0.0.0'  # Listen on all available interfaces
    SERVER_PORT = 12345

    server = Server(SERVER_IP, SERVER_PORT)

    if not server.start():
        return

    if not server.wait_for_client():
        return

    server.handle_client()

if __name__ == '__main__':
    main()