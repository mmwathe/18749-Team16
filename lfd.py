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
    def __init__(self, server_ip, server_port, client_id, heartbeat_interval=2):
        self.server_ip = server_ip
        self.server_port = server_port
        self.client_id = client_id
        self.heartbeat_interval = heartbeat_interval
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_alive = True

    def connect(self):
        try:
            self.socket.connect((self.server_ip, self.server_port))
            prGreen(f"Connected to server at {self.server_ip}:{self.server_port}")
            return True
        except Exception as e:
            prRed(f"Failed to connect to server: {e}")
            self.server_alive = False
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
            prYellow(str(message))  # Print the formatted heartbeat message
            self.socket.sendall(message_json.encode())
        except socket.error as e:
            prRed(f"Failed to send heartbeat. Error: {e}. Retrying...")
            # Sleep for a short time before retrying
            time.sleep(self.heartbeat_interval)

    def receive_response(self):
        try:
            response = self.socket.recv(1024).decode()
            
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
                # No response means server did not respond within the expected time frame
                prRed("Server did not respond to the heartbeat.")
                # We keep the LFD running and will continue to send heartbeats
                self.socket.close()
                time.sleep(self.heartbeat_interval)
                self.connect()

            # Calculate remaining time to sleep until the next heartbeat
            elapsed_time = time.time() - start_time
            sleep_time = max(0, self.heartbeat_interval - elapsed_time)
            time.sleep(sleep_time)

    def close_connection(self):
        self.socket.close()
        prRed("LFD shutdown.")

def main():
    # Set up argument parser for the heartbeat frequency
    parser = argparse.ArgumentParser(description="Local Fault Detector (LFD) for monitoring server health.")
    parser.add_argument('--heartbeat_freq', type=int, default=4,
                        help="Frequency of heartbeat messages in seconds (default: 4 seconds).")
    args = parser.parse_args()

    SERVER_IP = '0.0.0.0'
    SERVER_PORT = 12345
    CLIENT_ID = 'LFD1'

    # Create an instance of LFD with the specified heartbeat frequency
    lfd_client = LFD(SERVER_IP, SERVER_PORT, CLIENT_ID, heartbeat_interval=args.heartbeat_freq)

    if not lfd_client.connect():
        return

    try:
        lfd_client.monitor_server()
    except KeyboardInterrupt:
        prYellow("LFD interrupted by user.")
    finally:
        lfd_client.close_connection()

if __name__ == '__main__':
    main()