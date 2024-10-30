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

# REPLICA_ADDRESSES = [('BACKUP_IP_1', 12346), ('BACKUP_IP_2', 12347)]
REPLICA_ADDRESSES = [('172.26.88.54', 34567)]


class Server:
    def __init__(self, server_ip, server_port, lfd_ip, lfd_port, is_primary_replica, checkpoint_freq=10):
        self.server_ip = server_ip
        self.server_port = server_port
        self.lfd_ip = lfd_ip
        self.lfd_port = lfd_port
        self.is_primary_replica = is_primary_replica
        self.checkpoint_freq = checkpoint_freq
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setblocking(False)
        self.message_queue = Queue()
        self.clients = {}
        self.state = 0
        self.checkpoint_count = 0
        self.lfd_socket = None
        self.backup_sockets = []  # Connections to backups, only used by primary

        # Start checkpointing if primary
        if self.is_primary_replica:
            prYellow("This server is set as the primary replica.")
            threading.Thread(target=self.retry_connect_to_backups, daemon=True).start()
            threading.Thread(target=self.start_checkpointing, daemon=True).start()
        else:
            prCyan("This server is set as a backup replica.")
            threading.Thread(target=self.listen_for_checkpoints, daemon=True).start()  # Start checkpoint listener for replicas

    def start(self):
        try:
            self.server_socket.bind((self.server_ip, self.server_port))
            self.server_socket.listen(5)
            prGreen(f"Server listening on {self.server_ip}:{self.server_port}")
            self.connect_to_lfd()
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

    def retry_connect_to_backups(self):
        """Primary-only: continuously attempts to connect to backup replicas."""  # Replace with actual IPs and ports
        while len(self.backup_sockets) < len(REPLICA_ADDRESSES):
            for address in REPLICA_ADDRESSES:
                if address not in [sock.getpeername() for sock in self.backup_sockets]:
                    try:
                        backup_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        backup_socket.connect(address)
                        self.backup_sockets.append(backup_socket)
                        prGreen(f"Primary connected to backup at {address}")
                    except socket.error:
                        prYellow(f"Primary failed to connect to backup at {address}, retrying...")
                        time.sleep(2)  # Retry delay

    def start_checkpointing(self):
        """Primary-only: periodically send checkpoints to replicas."""
        while self.is_primary_replica:
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
                    prRed(f"Failed to send checkpoint to backup: {e}")

    def listen_for_checkpoints(self):
        """Backup-only: listens for checkpoint data from primary."""
        checkpoint_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        checkpoint_listener.bind((self.server_ip, self.server_port + 1))  # Listening port for checkpoints
        checkpoint_listener.listen(5)
        prCyan(f"Backup listening for checkpoints on {self.server_ip}:{self.server_port + 1}")
        
        while not self.is_primary_replica:
            try:
                primary_socket, address = checkpoint_listener.accept()
                with primary_socket:
                    data = primary_socket.recv(1024).decode()
                    if data:
                        self.receive_checkpoint(data)
            except socket.error:
                time.sleep(1)

    def receive_checkpoint(self, data):
        """Replica-only: update state based on checkpoint data from primary."""
        checkpoint_data = json.loads(data)
        self.state = checkpoint_data["my_state"]
        self.checkpoint_count = checkpoint_data["checkpoint_count"]
        prGreen(f"Replica: Updated state to {self.state}, checkpoint to {self.checkpoint_count}")

    def accept_new_connection(self):
        try:
            client_socket, client_address = self.server_socket.accept()
            client_socket.setblocking(False)
            self.clients[client_socket] = client_address
            prCyan(f"New client connection from {client_address}")
        except BlockingIOError:
            pass

    def receive_messages(self):
        if not self.is_primary_replica:
            return  # Replicas do not handle client messages

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
        if not self.is_primary_replica:
            return  # Replicas do not process messages

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
        """Process incoming heartbeat messages from LFD."""
        try:
            message = json.loads(data)
            if message.get("message") == "heartbeat":
                prGreen("Heartbeat received from LFD. Acknowledging...")
                response = {
                    "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                    "server_id": "primary" if self.is_primary_replica else "replica",
                    "message": "heartbeat acknowledgment"
                }
                self.lfd_socket.sendall(json.dumps(response).encode())
        except json.JSONDecodeError:
            prRed("Malformed heartbeat message received from LFD.")

    def receive_messages_from_lfd(self):
        """Receive messages from LFD, handle heartbeats."""
        if self.lfd_socket:
            try:
                data = self.lfd_socket.recv(1024).decode()
                if data:
                    self.handle_heartbeat(data)
            except BlockingIOError:
                pass  # No data received; continue

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
    LFD_IP = '0.0.0.0'  # Replace with LFD IP address
    LFD_PORT = 54321
    CHECKPOINT_FREQ = 10  # Frequency for checkpointing

    # Manually set whether this server is primary or replica
    IS_PRIMARY_REPLICA = True  # Set to True for primary, False for backups

    server = Server(SERVER_IP, SERVER_PORT, LFD_IP, LFD_PORT, is_primary_replica=IS_PRIMARY_REPLICA, checkpoint_freq=CHECKPOINT_FREQ)

    if not server.start():
        return

    try:
        while True:
            server.accept_new_connection()
            server.receive_messages()
            server.process_messages()
            server.receive_messages_from_lfd()
            time.sleep(2)
    except KeyboardInterrupt:
        prYellow("Server is shutting down...")
    finally:
        server.close_server()

if __name__ == '__main__':
    main()