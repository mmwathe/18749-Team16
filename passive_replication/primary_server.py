import socket
import json
import time
import errno
from queue import Queue, Empty

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
        self.server_socket.listen(5)
        self.server_socket.settimeout(1.0)  # Non-blocking for connection attempts
        self.lfd_socket = None
        self.replicas = []
        self.replica_count = 2  # Expecting exactly 2 replicas
        self.clients = {}
        self.message_queue = Queue()
        self.last_checkpoint_time = time.time()
        self.last_heartbeat_time = time.time()

    def connect_to_lfd(self):
        """Continuously try to connect to the LFD for heartbeat exchange."""
        while not self.lfd_socket:
            try:
                prYellow(f"Attempting to connect to LFD at {self.lfd_ip}:{self.lfd_port}...")
                self.lfd_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.lfd_socket.connect((self.lfd_ip, self.lfd_port))
                prGreen(f"Connected to LFD at {self.lfd_ip}:{self.lfd_port}")
            except Exception as e:
                prRed(f"Failed to connect to LFD: {e}. Retrying in 5 seconds...")
                time.sleep(5)
                self.lfd_socket = None

    def accept_connections_in_order(self):
        """Accept connections in a specific order: 2 replicas, then clients."""
        try:
            conn, addr = self.server_socket.accept()
            conn.setblocking(False)
            if len(self.replicas) < self.replica_count:
                # Treat the connection as a replica if we're still expecting replicas
                self.replicas.append(conn)
                prGreen(f"Connected to replica from {addr}")
            else:
                # Treat the connection as a client once all replicas are connected
                self.clients[conn] = addr
                prCyan(f"Connected to client from {addr}")
        except socket.timeout:
            pass  # No connection available, continue without blocking

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
            self.lfd_socket = None  # Reset LFD socket if it fails

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
                self.lfd_socket = None  # Reset LFD socket if it fails

    def send_checkpoint_to_replicas(self):
        """Send a checkpoint to all connected replicas."""
        checkpoint_data = {
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
            "server_id": "primary",
            "my_state": self.state,
            "checkpoint_count": self.checkpoint_count
        }
        checkpoint_message = json.dumps(checkpoint_data)

        for replica in self.replicas:
            try:
                prYellow(f"Sending checkpoint to replica: {checkpoint_data}")
                replica.sendall(checkpoint_message.encode())
            except socket.error as e:
                prRed(f"Failed to send checkpoint to replica. Error: {e}")
                #replica.close()
                #self.replicas.remove(replica)  # Remove failed replica

    def receive_ack_from_replicas(self):
        """Receive acknowledgment from replicas after sending a checkpoint."""
        for replica in self.replicas:
            retries = 5
            retry_delay = .1
            for _ in range(retries):
                try:
                    response = replica.recv(1024).decode()
                    response_data = json.loads(response)
                    prCyan(f"Received acknowledgment from replica: {response_data}")
                    break
                except (socket.error, json.JSONDecodeError) as e:
                    if isinstance(e, socket.error) and (e.errno == errno.EAGAIN or e.errno == errno.EWOULDBLOCK):
                        time.sleep(retry_delay)
                    else:
                        prRed(f"Failed to receive acknowledgment from a replica {e}")
                        break
                    #replica.close()
                    #self.replicas.remove(replica)

    def receive_messages_from_clients(self):
        """Receive messages from all connected clients."""
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
        """Process messages received from clients."""
        try:
            while True:
                client_socket, data = self.message_queue.get_nowait()
                self.process_client_message(client_socket, data)
        except Empty:
            pass

    def process_client_message(self, client_socket, data):
        """Handle individual client message, updating state if needed."""
        try:
            message = json.loads(data)
            client_id = message.get("client_id", "Unknown")
            content = message.get("message", "Unknown")
            request_number = message.get("request_number", "Unknown")

            prYellow(f"Primary received: {content} from C{client_id}")
            response = {
                "message": "",
                "server_id": "primary",
                "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                "state_before": self.state,
                "state_after": self.state,
                "request_number": request_number
            }

            if content.lower() == "update":
                self.state += 1
                response["message"] = "state updated"
                response["state_after"] = self.state
                prGreen(f"State updated: {self.state}")

            client_socket.sendall(json.dumps(response).encode())
        except json.JSONDecodeError:
            prRed("Received malformed message from client.")

    def disconnect_client(self, client_socket):
        """Disconnect a client."""
        client_address = self.clients.get(client_socket, "Unknown client")
        prRed(f"Client {client_address} disconnected.")
        client_socket.close()
        del self.clients[client_socket]

    def main_loop(self):
        """Main loop to handle client connections, replica connection, heartbeat, and checkpointing."""
        # Step 1: Ensure connection to the LFD
        if not self.lfd_socket:
            self.connect_to_lfd()

        while True:
            # Send heartbeat and receive response from LFD
            if self.lfd_socket and (time.time() - self.last_heartbeat_time >= self.heartbeat_interval):
                self.send_heartbeat_to_lfd()
                self.receive_heartbeat_from_lfd()
                self.last_heartbeat_time = time.time()

            # Accept connections in order: first replicas, then clients
            self.accept_connections_in_order()

            # Send checkpoints to replicas at regular intervals
            if len(self.replicas) >= self.replica_count and (time.time() - self.last_checkpoint_time >= self.checkpoint_interval):
                self.checkpoint_count += 1
                self.send_checkpoint_to_replicas()
                self.receive_ack_from_replicas()
                self.last_checkpoint_time = time.time()

            # Handle client messages
            self.receive_messages_from_clients()
            self.process_messages()

            # Brief sleep to avoid high CPU usage
            time.sleep(0.5)

    def close_server(self):
        """Clean up connections."""
        for client_socket in list(self.clients):
            self.disconnect_client(client_socket)
        for replica in self.replicas:
            replica.close()
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