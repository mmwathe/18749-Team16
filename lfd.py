import socket
import time
import json
import argparse
from message import Message

# Define color functions for printing
def prGreen(skk): print(f"\033[92m{skk}\033[00m")
def prRed(skk): print(f"\033[91m{skk}\033[00m")
def prYellow(skk): print(f"\033[93m{skk}\033[00m")
def prLightPurple(skk): print(f"\033[94m{skk}\033[00m")
def prPurple(skk): print(f"\033[95m{skk}\033[00m")
def prCyan(skk): print(f"\033[96m{skk}\033[00m")

class LFD:
    def __init__(self, lfd_ip, lfd_port, gfd_ip, gfd_port, client_id, heartbeat_interval=2):
        self.lfd_ip = lfd_ip
        self.lfd_port = lfd_port
        self.gfd_ip = gfd_ip
        self.gfd_port = gfd_port
        self.client_id = client_id
        self.heartbeat_interval = heartbeat_interval
        self.lfd_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.lfd_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.lfd_socket.bind((self.lfd_ip, self.lfd_port))
        self.lfd_socket.listen(1)  # Listen for one connection (the server)
        self.gfd_socket = None
        

    def wait_for_server(self):
        """Wait for the server to connect to the LFD."""
        prYellow(f"LFD waiting for server to connect on {self.lfd_ip}:{self.lfd_port}...")
        try:
            self.server_socket, self.server_address = self.lfd_socket.accept()
            prGreen(f"Server connected from {self.server_address}")
            # Notify GFD about the server connection
            self.notify_gfd("add replica " + self.server_address, self.server_address)
            return True
        except Exception as e:
            prRed(f"Failed to accept server connection: {e}")
            return False

    def connect_to_gfd(self):
        """Connect to the GFD and respond to heartbeats."""
        try:
            self.gfd_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.gfd_socket.connect((self.gfd_ip, self.gfd_port))
            prGreen(f"Connected to GFD at {self.gfd_ip}:{self.gfd_port}")
            return True
        except Exception as e:
            prRed(f"Failed to connect to GFD: {e}")
            return False

    def notify_gfd(self, event_type, event_data):
        """Notify the GFD of server-related events (connection or disconnection)."""
        if not self.gfd_socket:
            prRed("GFD is not connected. Cannot send notification.")
            return

        message = {
            "message": event_type,
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
            "lfd_id": self.client_id,
            "event_data": event_data
        }

        try:
            self.gfd_socket.sendall(json.dumps(message).encode())
            prCyan(f"Sent '{event_type}' notification to GFD: {message}")
        except socket.error as e:
            prRed(f"Failed to send notification to GFD. Error: {e}")

    def send_heartbeat_to_server(self):
        """Send a heartbeat to the server."""
        message = Message(self.client_id, "heartbeat")
        message_json = json.dumps({
            "timestamp": message.timestamp,
            "client_id": message.client_id,
            "message": message.message,
            "message_id": message.message_id
        })

        try:
            prYellow(f"Sending heartbeat to server: {message}")
            self.server_socket.sendall(message_json.encode())
        except socket.error as e:
            prRed(f"Failed to send heartbeat to server. Error: {e}. Retrying...")
            time.sleep(self.heartbeat_interval)

    def receive_response_from_server(self):
        """Receive a response from the server."""
        try:
            response = self.server_socket.recv(1024).decode()
            response_data = json.loads(response)
            server_id = response_data.get('server_id', 'Unknown')
            timestamp = response_data.get('timestamp', 'Unknown')
            message = response_data.get('message', 'Unknown')
            state = response_data.get('state', 'Unknown')

            prPurple("=" * 80)
            prYellow(f"{timestamp:<20} {server_id} -> {self.client_id}")
            prLightPurple(f"{'':<20} {'Message:':<15} {message}")
            prLightPurple(f"{'':<20} {'State:':<15} {state}")

            return response
        except (socket.error, json.JSONDecodeError):
            prRed("No response received from server. Server might be down.")
            return None

    def receive_heartbeat_from_gfd(self):
        """Receive a heartbeat from the GFD and send a response."""
        try:
            data = self.gfd_socket.recv(1024).decode()
            message = json.loads(data)
            if message.get('message', '') == 'heartbeat':
                prCyan(f"Received heartbeat from GFD: {message}")
                response = {
                    "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                    "client_id": self.client_id,
                    "message": "heartbeat acknowledgment"
                }
                self.gfd_socket.sendall(json.dumps(response).encode())
                prGreen("Sent heartbeat acknowledgment to GFD.")
        except (socket.error, json.JSONDecodeError):
            prRed("Failed to receive or respond to heartbeat from GFD.")

    def monitor_server(self):
        """Monitor the server by sending heartbeats."""
        self.send_heartbeat_to_server()
        start_time = time.time()
        response = self.receive_response_from_server()

        if response is None:
            prRed("Server did not respond to the heartbeat.")
            # Notify GFD that the server has disconnected
            self.notify_gfd("remove replica " + self.server_address, "No response from server")
            # If server does not respond, terminate connection and wait for reconnection
            self.server_socket.close()
            self.wait_for_server()

        # Calculate remaining time to sleep until the next heartbeat
        elapsed_time = time.time() - start_time
        sleep_time = max(0, self.heartbeat_interval - elapsed_time)
        time.sleep(sleep_time)

    def close_connection(self):
        if self.server_socket:
            # Notify GFD that the server has disconnected
            self.notify_gfd("remove replica " + self.server_address, "LFD shutting down")
            self.server_socket.close()
        if self.lfd_socket:
            self.lfd_socket.close()
        if self.gfd_socket:
            self.gfd_socket.close()
        prRed("LFD shutdown.")

def main():
    # Set up argument parser for the heartbeat frequency
    parser = argparse.ArgumentParser(description="Local Fault Detector (LFD) for monitoring server health.")
    parser.add_argument('--heartbeat_freq', type=int, default=4,
                        help="Frequency of heartbeat messages in seconds (default: 4 seconds).")
    args = parser.parse_args()

    CLIENT_ID = 'LFD1'

    LFD_IP = '0.0.0.0'  # LFD listens on all interfaces
    LFD_PORT = 54321  # LFD listens on this port

    GFD_IP = '172.26.59.208'
    GFD_PORT = 12345

    # Create an instance of LFD with the specified heartbeat frequency
    lfd = LFD(LFD_IP, LFD_PORT, GFD_IP, GFD_PORT, CLIENT_ID, heartbeat_interval=args.heartbeat_freq)

    # Wait for the server to connect
    if not lfd.wait_for_server():
        return

    # Connect to the GFD
    if not lfd.connect_to_gfd():
        return

    try:
        while True:
            # Monitor heartbeats with the server
            lfd.monitor_server()

            # Monitor for heartbeats from the GFD
            lfd.receive_heartbeat_from_gfd()

    except KeyboardInterrupt:
        prYellow("LFD interrupted by user.")
    finally:
        lfd.close_connection()

if __name__ == '__main__':
    main()