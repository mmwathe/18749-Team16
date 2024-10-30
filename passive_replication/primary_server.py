import socket
import json
import time
import threading
from queue import Queue, Empty

# Define color functions for printing
def prGreen(skk): print("\033[92m{}\033[00m".format(skk))
def prRed(skk): print("\033[91m{}\033[00m".format(skk))
def prYellow(skk): print("\033[93m{}\033[00m".format(skk))
def prLightPurple(skk): print("\033[94m{}\033[00m".format(skk))
def prPurple(skk): print("\033[95m{}\033[00m".format(skk))
def prCyan(skk): print("\033[96m{}\033[00m".format(skk))

class PrimaryServer:
    def __init__(self, server_ip, server_port, lfd_ip, lfd_port, checkpoint_freq=10):
        self.server_ip = server_ip
        self.server_port = server_port
        self.lfd_ip = lfd_ip
        self.lfd_port = lfd_port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setblocking(False)
        self.message_queue = Queue()
        self.clients = {}
        self.state = 0
        self.checkpoint_count = 0
        self.checkpoint_freq = checkpoint_freq
        self.lfd_socket = None
        self.backup_sockets = []  # Connections to replicas

    def start(self):
        try:
            self.server_socket.bind((self.server_ip, self.server_port))
            self.server_socket.listen(5)
            prGreen(f"Primary Server listening on {self.server_ip}:{self.server_port}")
            self.connect_to_lfd()
        except Exception as e:
            prRed(f"Failed to start primary server: {e}")
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

    def connect_to_backups(self, backups):
        """Connect to backup replicas for checkpointing."""
        for ip, port in backups:
            try:
                backup_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                backup_socket.connect((ip, port))
                self.backup_sockets.append(backup_socket)
                prGreen(f"Connected to backup replica at {ip}:{port}")
            except Exception as e:
                prRed(f"Failed to connect to backup {ip}:{port}: {e}")

    def start_checkpointing(self):
        """Periodically send checkpoints to replicas."""
        while True:
            time.sleep(self.checkpoint_freq)
            self.checkpoint_count += 1
            checkpoint_data = {
                "my_state": self.state,
                "checkpoint_count": self.checkpoint_count
            }
            prYellow(f"Primary: Sending checkpoint {checkpoint_data} to replicas.")
            for backup_socket in self.backup_sockets:
                try:
                    backup_socket.sendall(json.dumps(checkpoint_data).encode())
                except socket.error as e:
                    prRed(f"Failed to send checkpoint to replica: {e}")

    def accept_new_connection(self):
        try:
            client_socket, client_address = self.server_socket.accept()
            client_socket.setblocking(False)
            self.clients[client_socket] = client_address
            prCyan(f"New client connection from {client_address}")
        except BlockingIOError:
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
                pass

    def process_messages(self):
        try:
            while True:
                client_socket, data = self.message_queue.get_nowait()
                self.process_client_message(client_socket, data)
        except Empty:
            pass

    def process_client_message(self, client_socket, data):
        try:
            message = json.loads(data)
            timestamp = message.get('timestamp', 'Unknown')
            client_id = message.get('client_id', 'Unknown')
            content = message.get('message', 'Unknown')
            request_number = message.get("request_number", "Unknown")

            prYellow(f"Primary received: {content} from C{client_id}")

            state_before = self.state
            response = {
                "message": "",
                "server_id": "primary",
                "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                "state_before": state_before,
                "state_after": self.state,
                "request_number": request_number
            }

            if content.lower() == 'update':
                self.state += 1
                response["message"] = "state updated"
                response["state_after"] = self.state
                prGreen(f"State updated: {state_before} -> {self.state}")

            client_socket.sendall(json.dumps(response).encode())
        except json.JSONDecodeError:
            prRed("Received malformed message from client.")

    def handle_heartbeat(self, data):
        """Acknowledge heartbeat messages from LFD."""
        try:
            message = json.loads(data)
            if message.get("message") == "heartbeat":
                prGreen("Heartbeat received from LFD. Acknowledging...")
                response = {
                    "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                    "server_id": "primary",
                    "message": "heartbeat acknowledgment"
                }
                self.lfd_socket.sendall(json.dumps(response).encode())
        except json.JSONDecodeError:
            prRed("Malformed heartbeat message received from LFD.")

    def receive_messages_from_lfd(self):
        """Receive heartbeat messages from LFD."""
        if self.lfd_socket:
            try:
                data = self.lfd_socket.recv(1024).decode()
                if data:
                    self.handle_heartbeat(data)
            except BlockingIOError:
                pass

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
        prRed("Primary server shutdown.")

def main():
    SERVER_IP = '0.0.0.0'
    SERVER_PORT = 12345
    LFD_IP = '0.0.0.0'  # Replace with LFD IP address
    LFD_PORT = 54321
    CHECKPOINT_FREQ = 10  # Frequency for checkpointing
    BACKUP_REPLICAS = [('0.0.0.0', 12346), ('0.0.0.0', 12347)]  # Addresses for S2 and S3

    server = PrimaryServer(SERVER_IP, SERVER_PORT, LFD_IP, LFD_PORT, checkpoint_freq=CHECKPOINT_FREQ)
    
    if not server.start():
        return
    
    server.connect_to_backups(BACKUP_REPLICAS)
    threading.Thread(target=server.start_checkpointing, daemon=True).start()

    try:
        while True:
            server.accept_new_connection()
            server.receive_messages()
            server.process_messages()
            server.receive_messages_from_lfd()
            time.sleep(2)
    except KeyboardInterrupt:
        prYellow("Primary server is shutting down...")
    finally:
        server.close_server()

if __name__ == '__main__':
    main()