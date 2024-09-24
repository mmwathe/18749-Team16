import socket
import json
import time
import threading
import uuid

class LocalFaultDetector:
    def __init__(self, lfd_ip, lfd_port, server_ip, server_port, heartbeat_interval=2, timeout=5):
        self.lfd_ip = lfd_ip
        self.lfd_port = lfd_port
        self.server_ip = server_ip
        self.server_port = server_port
        self.heartbeat_interval = heartbeat_interval
        self.timeout = timeout
        self.server_socket = None
        self.lfd_socket = None
        self.server_alive = True

    def connect_to_server(self):
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.settimeout(self.timeout)
            self.server_socket.connect((self.server_ip, self.server_port))
            print(f"Connected to server at {self.server_ip}:{self.server_port}")
        except socket.error:
            print("Unable to connect to server.")
            self.server_alive = False

    def send_heartbeat(self):
        while self.server_alive:
            try:
                timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                message = {
                    "message": "heartbeat",
                    "client_id": "LFD1",
                    "timestamp": timestamp,
                    "message_id": str(uuid.uuid4())
                }
                # Send heartbeat to server
                self.server_socket.sendall(json.dumps(message).encode())
                print(f"Heartbeat Sent to S1 at {timestamp}")

                # Wait for acknowledgment
                response = self.server_socket.recv(1024).decode()
                response_data = json.loads(response)
                response_timestamp = response_data.get('timestamp', 'Unknown')
                print(f"Heartbeat Received for S1 at {response_timestamp}")
            except socket.error:
                print("Server is unreachable. Server might be down.")
                self.server_alive = False
            except json.JSONDecodeError:
                print("Received malformed response from server.")
            time.sleep(self.heartbeat_interval)

    def start(self):
        self.connect_to_server()
        if self.server_alive:
            heartbeat_thread = threading.Thread(target=self.send_heartbeat)
            heartbeat_thread.start()
            heartbeat_thread.join()

    def close(self):
        if self.server_socket:
            self.server_socket.close()
        print("LFD shutdown.")

def main():
    LFD_IP = '0.0.0.0'  # LFD listens on all available interfaces
    LFD_PORT = 49663    # LFD Port (not used in this version but set for completeness)
    SERVER_IP = '0.0.0.0'  # Server IP
    SERVER_PORT = 12345  # Server Port
    HEARTBEAT_INTERVAL = 2   # Interval between heartbeats
    TIMEOUT = 5              # Timeout for server response

    lfd = LocalFaultDetector(LFD_IP, LFD_PORT, SERVER_IP, SERVER_PORT, HEARTBEAT_INTERVAL, TIMEOUT)
    try:
        lfd.start()
    except KeyboardInterrupt:
        print("LFD is shutting down...")
    finally:
        lfd.close()

if __name__ == '__main__':
    main()