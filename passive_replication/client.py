import socket
import json
import time

# Define color functions for printing
def print_sent(skk): print("\033[96m{}\033[00m".format(skk))  # Cyan for sent messages
def print_received(skk): print("\033[95m{}\033[00m".format(skk))  # Purple for received messages
def printR(skk): print("\033[91m{}\033[00m".format(skk))  # Red for errors
def printY(skk): print("\033[93m{}\033[00m".format(skk))  # Yellow for warnings
def printG(skk): print("\033[92m{}\033[00m".format(skk))  # Green for registrations

# List of server IPs in order of preference
SERVER_IPS = [
    '172.26.45.165',  # Primary server IP (S1)
    '172.26.115.84'   # Backup server IP (S2)
    # Add more servers if available
]

class Client:
    def __init__(self, server_port, client_id):
        self.server_ips = SERVER_IPS
        self.server_port = server_port
        self.client_id = client_id

    def create_message(self, message_type, **kwargs):
        """Creates a standard message with client_id and timestamp."""
        return {
            "client_id": self.client_id,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "message": message_type,
            **kwargs
        }

    def format_message_log(self, message, sender, receiver, sent=True):
        """Formats the message log for structured printing."""
        message_type = message.get("message", "Unknown")
        timestamp = message.get("timestamp", "Unknown")
        details = {k: v for k, v in message.items() if k not in ["client_id", "timestamp", "message"]}
        color_fn = print_sent if sent else print_received
        color_fn("====================================================================")
        color_fn(f"{sender} → {receiver} ({message_type}) at {timestamp}")
        if details:
            for key, value in details.items():
                color_fn(f"  {key}: {value}")
        color_fn("====================================================================")

    def attempt_connection(self):
        """Attempt to connect to servers in order."""
        for ip in self.server_ips:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((ip, self.server_port))
                printG(f"Connected to server at {ip}:{self.server_port}")
                return sock, ip
            except Exception as e:
                printR(f"Failed to connect to server {ip}: {e}")
                time.sleep(2)  # Wait before trying next server
        return None, None

    def send_message(self, sock, message, receiver):
        """Sends a message through the provided socket."""
        try:
            sock.sendall(json.dumps(message).encode())
            self.format_message_log(message, self.client_id, receiver, sent=True)
        except socket.error as e:
            printR(f"Failed to send message to {receiver}: {e}")
            sock.close()
            return False
        return True

    def receive_message(self, sock, sender):
        """Receives a message from the provided socket."""
        try:
            data = sock.recv(4096).decode()
            if not data:
                printR(f"No data received from {sender}.")
                sock.close()
                return None
            message = json.loads(data)
            self.format_message_log(message, sender, self.client_id, sent=False)
            return message
        except (socket.error, json.JSONDecodeError) as e:
            printR(f"Failed to receive or decode message from {sender}: {e}")
            sock.close()
            return None

    def run(self):
        """Main client operation."""
        while True:
            sock, server_ip = self.attempt_connection()
            if sock is None:
                printR("All servers are unreachable. Retrying in 5 seconds...")
                time.sleep(5)  # Wait before retrying all servers
                continue

            try:
                while True:
                    # Example: Send an 'update' message
                    message = self.create_message("update")
                    if not self.send_message(sock, message, f"Server@{server_ip}"):
                        break  # Connection failed, try next server

                    response = self.receive_message(sock, f"Server@{server_ip}")
                    if response is None:
                        break  # Connection failed, try next server

                    # Process response if needed
                    time.sleep(5)  # Wait before sending next message
            except Exception as e:
                printR(f"Error during communication with server {server_ip}: {e}")
            finally:
                sock.close()
                printY(f"Disconnected from server {server_ip}. Retrying other servers...")
                time.sleep(2)  # Wait before trying next server
