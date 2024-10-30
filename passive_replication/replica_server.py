import socket
import json
import time

# Define color functions for printing
def prGreen(skk): print("\033[92m{}\033[00m".format(skk))
def prRed(skk): print("\033[91m{}\033[00m".format(skk))
def prYellow(skk): print("\033[93m{}\033[00m".format(skk))
def prCyan(skk): print("\033[96m{}\033[00m".format(skk))

class ReplicaServer:
    def __init__(self, server_ip, server_port, primary_ip, primary_port, lfd_ip, lfd_port):
        self.server_ip = server_ip
        self.server_port = server_port
        self.primary_ip = primary_ip
        self.primary_port = primary_port
        self.lfd_ip = lfd_ip
        self.lfd_port = lfd_port
        self.state = 0
        self.checkpoint_count = 0
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.server_ip, self.server_port))
        self.server_socket.listen(1)  # Listen for the primary connection
        self.lfd_socket = None
        self.primary_socket = None
        self.last_heartbeat_ack_time = time.time()

    def connect_to_lfd(self):
        """Connect to the LFD for heartbeat exchange."""
        try:
            self.lfd_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.lfd_socket.connect((self.lfd_ip, self.lfd_port))
            prGreen(f"Connected to LFD at {self.lfd_ip}:{self.lfd_port}")
        except Exception as e:
            prRed(f"Failed to connect to LFD: {e}")
            self.lfd_socket = None

    def connect_to_primary(self):
        """Continuously try to connect to the primary server to receive checkpoints."""
        while True:
            try:
                prYellow(f"Attempting to connect to Primary Server at {self.primary_ip}:{self.primary_port}")
                self.primary_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.primary_socket.settimeout(5)  # Timeout to avoid long hangs
                self.primary_socket.connect((self.primary_ip, self.primary_port))
                prGreen(f"Connected to Primary Server at {self.primary_ip}:{self.primary_port}")
                break  # Exit loop if connection is successful
            except (socket.timeout, socket.error) as e:
                prRed(f"Failed to connect to Primary Server: {e}. Retrying in 5 seconds...")
                time.sleep(5)

    def handle_heartbeat_from_lfd(self, data):
        """Process heartbeat messages from LFD and send acknowledgment."""
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

    def receive_heartbeat_from_lfd(self):
        """Receive heartbeat messages from LFD."""
        if self.lfd_socket:
            try:
                data = self.lfd_socket.recv(1024).decode()
                if data:
                    self.handle_heartbeat_from_lfd(data)
            except (socket.error, json.JSONDecodeError):
                prRed("Failed to receive or respond to heartbeat from LFD.")

    def receive_checkpoint_from_primary(self):
        """Receive checkpoint messages from Primary."""
        try:
            data = self.primary_socket.recv(1024).decode()
            if data:
                checkpoint_data = json.loads(data.strip())  # Remove any whitespace or newline characters
                self.state = checkpoint_data["my_state"]
                self.checkpoint_count = checkpoint_data["checkpoint_count"]
                prYellow(f"Replica: Received checkpoint - {checkpoint_data}")
                # Send acknowledgment back to primary
                ack_message = json.dumps({
                    "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                    "server_id": "replica",
                    "message": "checkpoint_acknowledgment"
                })
                self.primary_socket.sendall(ack_message.encode())
                prCyan("Sent checkpoint acknowledgment to Primary.")
        except (BlockingIOError, socket.error):
            pass  # No data received yet, continue the loop
        except json.JSONDecodeError:
            prRed("Malformed checkpoint data received from Primary.")

    def main_loop(self):
        """Main loop to handle receiving checkpoints and heartbeats."""
        while True:
            # Attempt to connect to LFD if not connected
            if not self.lfd_socket:
                self.connect_to_lfd()
            
            # Attempt to connect to Primary if not connected
            if not self.primary_socket:
                self.connect_to_primary()

            # Handle receiving checkpoints from Primary
            if self.primary_socket:
                self.receive_checkpoint_from_primary()
            
            # Handle receiving heartbeats from LFD
            if self.lfd_socket:
                self.receive_heartbeat_from_lfd()
            
            # Brief sleep to avoid high CPU usage
            time.sleep(0.5)

    def close_server(self):
        if self.lfd_socket:
            self.lfd_socket.close()
        if self.primary_socket:
            self.primary_socket.close()
        prRed("Replica server shutdown.")

def main():
    SERVER_IP = '0.0.0.0'
    SERVER_PORT = 12346  # Unique port for replica
    PRIMARY_IP = '172.26.36.98'  # Primary server's IP
    PRIMARY_PORT = 43210  # Primary server port
    LFD_IP = '0.0.0.0'  # LFD IP address
    LFD_PORT = 54321

    server = ReplicaServer(SERVER_IP, SERVER_PORT, PRIMARY_IP, PRIMARY_PORT, LFD_IP, LFD_PORT)
    
    try:
        server.main_loop()
    except KeyboardInterrupt:
        prYellow("Replica server is shutting down...")
    finally:
        server.close_server()

if __name__ == '__main__':
    main()