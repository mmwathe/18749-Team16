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
    def __init__(self, lfd_ip, lfd_port, client_id, heartbeat_interval=2):
        self.lfd_ip = lfd_ip
        self.lfd_port = lfd_port
        self.client_id = client_id
        self.heartbeat_interval = heartbeat_interval
        self.lfd_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.lfd_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.lfd_socket.bind((self.lfd_ip, self.lfd_port))
        self.lfd_socket.listen(1)  # Listen for one connection (the server)

    def wait_for_server(self):
        prYellow(f"LFD waiting for server to connect on {self.lfd_ip}:{self.lfd_port}...")
        try:
            self.server_socket, server_address = self.lfd_socket.accept()
            prGreen(f"Server connected from {server_address}")
            return True
        except Exception as e:
            prRed(f"Failed to accept server connection: {e}")
            return False

    def send_heartbeat(self):
        message = Message(self.client_id, "heartbeat")
        message_json = json.dumps({
            "timestamp": message.timestamp,
            "client_id": message.client_id,
            "message": message.message,
            "message_id": message.message_id
        })

        # Attempt to send the heartbeat message with error handling
        try:
            prYellow(f"Sending heartbeat: {message}")
            self.server_socket.sendall(message_json.encode())
        except socket.error as e:
            prRed(f"Failed to send heartbeat. Error: {e}. Retrying...")
            time.sleep(self.heartbeat_interval)

    def receive_response(self):
        try:
            response = self.server_socket.recv(1024).decode()
            # Parse the response JSON
            response_data = json.loads(response)
            server_id = response_data.get('server_id', 'Unknown')
            timestamp = response_data.get('timestamp', 'Unknown')
            message = response_data.get('message', 'Unknown')
            state = response_data.get('state', 'Unknown')

            # Print the parsed response in a formatted manner
            prPurple("=" * 80)
            prYellow(f"{timestamp:<20} {server_id} -> {self.client_id}")
            prLightPurple(f"{'':<20} {'Message:':<15} {message}")
            prLightPurple(f"{'':<20} {'State:':<15} {state}")

            return response
        except (socket.error, json.JSONDecodeError):
            prRed("No response received. Server might be down or sent an invalid response.")
            return None

    def monitor_server(self):
        while True:
            self.send_heartbeat()
            start_time = time.time()
            response = self.receive_response()

            if response is None:
                prRed("Server did not respond to the heartbeat.")
                # If server does not respond, terminate connection and wait for reconnection
                self.server_socket.close()
                self.wait_for_server()

            # Calculate remaining time to sleep until the next heartbeat
            elapsed_time = time.time() - start_time
            sleep_time = max(0, self.heartbeat_interval - elapsed_time)
            time.sleep(sleep_time)

    def close_connection(self):
        self.server_socket.close()
        self.lfd_socket.close()
        prRed("LFD shutdown.")

def main():
    # Set up argument parser for the heartbeat frequency
    parser = argparse.ArgumentParser(description="Local Fault Detector (LFD) for monitoring server health.")
    parser.add_argument('--heartbeat_freq', type=int, default=4,
                        help="Frequency of heartbeat messages in seconds (default: 4 seconds).")
    args = parser.parse_args()

    LFD_IP = '0.0.0.0'  # LFD listens on all interfaces
    LFD_PORT = 54321  # LFD listens on this port
    CLIENT_ID = 'LFD1'

    # Create an instance of LFD with the specified heartbeat frequency
    lfd = LFD(LFD_IP, LFD_PORT, CLIENT_ID, heartbeat_interval=args.heartbeat_freq)

    # Wait for the server to connect
    if not lfd.wait_for_server():
        return

    try:
        lfd.monitor_server()
    except KeyboardInterrupt:
        prYellow("LFD interrupted by user.")
    finally:
        lfd.close_connection()

if __name__ == '__main__':
    main()