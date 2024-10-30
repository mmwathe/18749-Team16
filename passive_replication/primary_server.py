import socket
import json
import time

# Define color functions for printing
def prGreen(skk): print("\033[92m{}\033[00m".format(skk))
def prRed(skk): print("\033[91m{}\033[00m".format(skk))
def prYellow(skk): print("\033[93m{}\033[00m".format(skk))
def prCyan(skk): print("\033[96m{}\033[00m".format(skk))

class PrimaryServer:
    def __init__(self, server_ip, server_port, lfd_ip, lfd_port, checkpoint_interval=10, heartbeat_interval=5):
        self.server_ip = server_ip
        self.server_port = server_port
        self.lfd_ip = lfd_ip
        self.lfd_port = lfd_port
        self.checkpoint_interval = checkpoint_interval
        self.heartbeat_interval = heartbeat_interval
        self.state = 0
        self.checkpoint_count = 0
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.server_ip, self.server_port))
        self.server_socket.listen(1)  # Listen for one connection (the replica)
        self.lfd_socket = None
        self.replica_socket = None
        self.last_checkpoint_time = time.time()
        self.last_heartbeat_time = time.time()

    def wait_for_replica(self):
        """Wait for the replica to connect to the primary server."""
        self.server_socket.settimeout(1.0)  # Set a timeout to prevent blocking
        prYellow(f"Primary server waiting for replica connection on {self.server_ip}:{self.server_port}...")
        try:
            self.replica_socket, replica_address = self.server_socket.accept()
            prGreen(f"Replica connected from {replica_address}")
        except socket.timeout:
            pass  # No connection yet, continue the loop
        except Exception as e:
            prRed(f"Failed to accept replica connection: {e}")

    def connect_to_lfd(self):
        """Connect to the LFD for heartbeat exchange."""
        try:
            self.lfd_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.lfd_socket.connect((self.lfd_ip, self.lfd_port))
            prGreen(f"Connected to LFD at {self.lfd_ip}:{self.lfd_port}")
        except Exception as e:
            prRed(f"Failed to connect to LFD: {e}")
            self.lfd_socket = None

    def send_heartbeat_to_lfd(self):
        """Send a heartbeat message to the LFD."""
        message = {
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
            "server_id": "primary",
            "message": "heartbeat"
        }

        try:
            prYellow(f"Sending heartbeat to LFD: {message}")
            self.lfd_socket.sendall(json.dumps(message).encode())
        except socket.error as e:
            prRed(f"Failed to send heartbeat to LFD. Error: {e}")

    def receive_heartbeat_from_lfd(self):
        """Receive and respond to heartbeat messages from LFD."""
        if self.lfd_socket:
            try:
                data = self.lfd_socket.recv(1024).decode()
                if data:
                    message = json.loads(data)
                    prGreen(f"Received heartbeat from LFD: {message}")
            except (socket.error, json.JSONDecodeError):
                prRed("Failed to receive or parse heartbeat from LFD.")

    def send_checkpoint_to_replica(self):
        """Send a checkpoint to the connected replica."""
        checkpoint_data = {
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
            "server_id": "primary",
            "my_state": self.state,
            "checkpoint_count": self.checkpoint_count
        }
        checkpoint_message = json.dumps(checkpoint_data)

        try:
            prYellow(f"Sending checkpoint to replica: {checkpoint_data}")
            self.replica_socket.sendall(checkpoint_message.encode())
        except socket.error as e:
            prRed(f"Failed to send checkpoint to replica. Error: {e}")
            self.replica_socket.close()
            self.replica_socket = None  # Disconnect replica

    def receive_ack_from_replica(self):
        """Receive acknowledgment from the replica after sending a checkpoint."""
        try:
            response = self.replica_socket.recv(1024).decode()
            response_data = json.loads(response)
            prCyan(f"Received acknowledgment from replica: {response_data}")
            return response_data
        except (socket.error, json.JSONDecodeError):
            prRed("Failed to receive acknowledgment from replica. Replica might be down.")
            self.replica_socket.close()
            self.replica_socket = None  # Disconnect replica
            return None

    def main_loop(self):
        """Main loop to handle replica connection, heartbeat, and checkpointing."""
        while True:
            # Attempt to connect to LFD if not connected
            if not self.lfd_socket:
                self.connect_to_lfd()

            # Attempt to connect to a replica if not connected
            if not self.replica_socket:
                self.wait_for_replica()

            # Send a heartbeat to LFD at regular intervals
            if self.lfd_socket and (time.time() - self.last_heartbeat_time >= self.heartbeat_interval):
                self.send_heartbeat_to_lfd()
                self.receive_heartbeat_from_lfd()
                self.last_heartbeat_time = time.time()

            # Send checkpoint to replica at regular intervals
            if self.replica_socket and (time.time() - self.last_checkpoint_time >= self.checkpoint_interval):
                self.checkpoint_count += 1
                self.send_checkpoint_to_replica()
                self.receive_ack_from_replica()
                self.last_checkpoint_time = time.time()

            # Brief sleep to avoid high CPU usage
            time.sleep(0.5)

    def close_server(self):
        if self.replica_socket:
            self.replica_socket.close()
        if self.server_socket:
            self.server_socket.close()
        if self.lfd_socket:
            self.lfd_socket.close()
        prRed("Primary server shutdown.")

def main():
    SERVER_IP = '0.0.0.0'  # Listening on all interfaces
    SERVER_PORT = 43210
    LFD_IP = '127.0.0.1'  # Update with actual LFD IP
    LFD_PORT = 54321  # Update with actual LFD port
    CHECKPOINT_INTERVAL = 10  # Frequency for sending checkpoints in seconds
    HEARTBEAT_INTERVAL = 5  # Frequency for sending heartbeats in seconds

    # Create an instance of the primary server
    primary_server = PrimaryServer(SERVER_IP, SERVER_PORT, LFD_IP, LFD_PORT, checkpoint_interval=CHECKPOINT_INTERVAL, heartbeat_interval=HEARTBEAT_INTERVAL)

    try:
        primary_server.main_loop()
    except KeyboardInterrupt:
        prYellow("Primary server interrupted by user.")
    finally:
        primary_server.close_server()

if __name__ == '__main__':
    main()