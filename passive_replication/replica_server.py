import socket
import json
import time

# Define color functions for printing
def prGreen(skk): print("\033[92m{}\033[00m".format(skk))
def prRed(skk): print("\033[91m{}\033[00m".format(skk))
def prYellow(skk): print("\033[93m{}\033[00m".format(skk))
def prLightPurple(skk): print("\033[94m{}\033[00m".format(skk))
def prCyan(skk): print("\033[96m{}\033[00m".format(skk))

class ReplicaServer:
    def __init__(self, server_ip, server_port, primary_ip, primary_port, lfd_ip, lfd_port):
        self.server_ip = server_ip
        self.server_port = server_port
        self.primary_ip = primary_ip
        self.primary_port = primary_port
        self.lfd_ip = lfd_ip
        self.lfd_port = lfd_port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setblocking(False)
        self.state = 0
        self.checkpoint_count = 0
        self.lfd_socket = None
        self.primary_socket = None

    def start(self):
        try:
            self.server_socket.bind((self.server_ip, self.server_port))
            self.server_socket.listen(5)
            prGreen(f"Replica Server listening on {self.server_ip}:{self.server_port}")
            self.connect_to_lfd()
            self.connect_to_primary()
        except Exception as e:
            prRed(f"Failed to start replica server: {e}")
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

    def connect_to_primary(self):
        """Connect to primary server for receiving checkpoints."""
        try:
            self.primary_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.primary_socket.connect((self.primary_ip, self.primary_port))
            prGreen(f"Connected to Primary Server at {self.primary_ip}:{self.primary_port}")
        except Exception as e:
            prRed(f"Failed to connect to Primary Server: {e}")
            self.primary_socket = None

    def handle_heartbeat(self, data):
        """Process incoming heartbeat messages from LFD."""
        try:
            message = json.loads(data)
            if message.get("message") == "heartbeat":
                prGreen("Heartbeat received from LFD. Acknowledging...")
                response = {
                    "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                    "server_id": "replica",
                    "message": "heartbeat acknowledgment"
                }
                self.lfd_socket.sendall(json.dumps(response).encode())
        except json.JSONDecodeError:
            prRed("Malformed heartbeat message received from LFD.")

    def receive_messages_from_lfd(self):
        """Receive messages from LFD and handle heartbeats."""
        if self.lfd_socket:
            try:
                data = self.lfd_socket.recv(1024).decode()
                if data:
                    self.handle_heartbeat(data)
            except BlockingIOError:
                pass  # No data received; continue

    def receive_checkpoint_from_primary(self):
        """Receive checkpoint updates from the primary server."""
        if self.primary_socket:
            try:
                data = self.primary_socket.recv(1024).decode()
                if data:
                    checkpoint_data = json.loads(data)
                    self.state = checkpoint_data["my_state"]
                    self.checkpoint_count = checkpoint_data["checkpoint_count"]
                    prGreen(f"Replica: Updated state to {self.state}, checkpoint count to {self.checkpoint_count}")
            except (socket.error, json.JSONDecodeError) as e:
                prRed(f"Failed to receive or process checkpoint from primary: {e}")

    def close_server(self):
        self.server_socket.close()
        if self.lfd_socket:
            self.lfd_socket.close()
        if self.primary_socket:
            self.primary_socket.close()
        prRed("Replica server shutdown.")

def main():
    SERVER_IP = '0.0.0.0'
    SERVER_PORT = 12346  # Unique port for replica
    PRIMARY_IP = '172.26.98.208'  # Replace with primary server's IP
    PRIMARY_PORT = 43210  # Primary server port
    LFD_IP = '0.0.0.0'  # Replace with LFD IP address
    LFD_PORT = 54321

    server = ReplicaServer(SERVER_IP, SERVER_PORT, PRIMARY_IP, PRIMARY_PORT, LFD_IP, LFD_PORT)
    
    if not server.start():
        return

    try:
        while True:
            server.receive_messages_from_lfd()
            server.receive_checkpoint_from_primary()
            time.sleep(2)
    except KeyboardInterrupt:
        prYellow("Replica server is shutting down...")
    finally:
        server.close_server()

if __name__ == '__main__':
    main()