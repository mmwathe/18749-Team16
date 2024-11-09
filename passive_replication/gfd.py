import socket  
import json
import time
import threading
import os
from dotenv import load_dotenv
# Define color functions for printing
def prGreen(skk): print(f"\033[92m{skk}\033[00m")         # Green
def prRed(skk): print(f"\033[91m{skk}\033[00m")           # Red
def prYellow(skk): print(f"\033[93m{skk}\033[00m")        # Yellow
def prLightPurple(skk): print(f"\033[94m{skk}\033[00m")    # Light Purple
def prPurple(skk): print(f"\033[95m{skk}\033[00m")        # Purple
def prCyan(skk): print(f"\033[96m{skk}\033[00m")          # Cyan

LFD_IPS = [os.getenv('SERVER1'), os.getenv('SERVER2'), os.getenv('SERVER3')]

class GFD:
    def __init__(self, host, port, heartbeat_interval=5):
        self.host = host
        self.port = port
        self.heartbeat_interval = heartbeat_interval  # Interval between heartbeats
        self.membership = {}  # To track replicas (server_id -> timestamp)
        self.member_count = 0  # Initialize member_count to 0
        prPurple(f"GFD: {self.member_count} members")  # Print initial member count
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        prCyan(f"GFD started. Listening on {self.host}:{self.port}")
        self.lock = threading.Lock()  # To synchronize access to membership

    def handle_lfd_connection(self, conn, addr):
        prCyan(f"Connected to LFD at {addr}")
        buffer = ""
        brace_counter = 0
        in_json = False

        # Start a separate thread for sending heartbeats to avoid blocking
        heartbeat_thread = threading.Thread(target=self.send_heartbeat_continuously, args=(conn, addr), daemon=True)
        heartbeat_thread.start()

        while True:
            try:
                # Receive message from LFD
                data = conn.recv(1024).decode()
                if not data:
                    prRed(f"LFD at {addr} disconnected.")
                    break
                # Add incoming data to buffer
                buffer += data
                # Parse buffer for complete JSON objects
                while buffer:
                    if not in_json:
                        # Look for the start of a JSON object
                        start = buffer.find('{')
                        if start == -1:
                            # No JSON object start found
                            buffer = ""
                            break
                        else:
                            buffer = buffer[start:]
                            in_json = True
                            brace_counter = 1
                    else:
                        # Iterate through the buffer to find the end of the JSON object
                        for i in range(1, len(buffer)):
                            if buffer[i] == '{':
                                brace_counter += 1
                            elif buffer[i] == '}':
                                brace_counter -= 1
                                if brace_counter == 0:
                                    # Found the end of the JSON object
                                    json_str = buffer[:i+1]
                                    buffer = buffer[i+1:]
                                    in_json = False
                                    try:
                                        message = json.loads(json_str)
                                        
                                        # Check for 'add' or 'remove' action for a replica
                                        action = message.get("message", "").lower()
                                        message_data = message.get("message_data", {})
                                        
                                        if action == "add replica":
                                            server_id = message_data.get("server_id")
                                            if server_id:
                                                self.add_replica(server_id)
                                            else:
                                                prRed("Add replica message missing 'server_id'.")
                                        elif action == "remove replica":
                                            server_id = message_data.get("server_id")
                                            if server_id:
                                                self.delete_replica(server_id)
                                            else:
                                                prRed("Remove replica message missing 'server_id'.")
                                        elif action == "heartbeat acknowledgment":   
                                            prYellow(f"Heartbeat acknowledgment received from LFD{self.getLFDIDfromIP()[addr[0]]}")
                                        else:
                                            prLightPurple(f"Unknown action '{action}' from LFD{self.getLFDIDfromIP()[addr[0]]}")
                                    except json.JSONDecodeError as e:
                                        prRed(f"Error decoding JSON message: {e}")
                                    break
                        else:
                            # No complete JSON object found yet
                            break
            except socket.error as e:
                prRed(f"Error receiving message from LFD at LFD{self.getLFDIDfromIP()[addr[0]]}: {e}")
                break
        conn.close()
        
        
    def getLFDIDfromIP(self):
        return {ip: f"{index+1}" for index, ip in enumerate(LFD_IPS)}    

    def send_heartbeat_continuously(self, conn, addr):
        while True:
            try:
                # Sending heartbeat message
                message = {
                    "message": "heartbeat",
                    "gfd_id": "GFD",
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                }
                conn.sendall(json.dumps(message).encode())  # No newline delimiter
                prCyan(f"Sent heartbeat to LFD{self.getLFDIDfromIP()[addr[0]]}")
                
                time.sleep(self.heartbeat_interval)  # Wait before sending the next heartbeat
            except socket.error as e:
                prRed(f"Failed to send heartbeat to LFD at {addr}: {e}")
                conn.close()
                break

    def add_replica(self, replica_id):
        with self.lock:
            if replica_id not in self.membership:
                self.membership[replica_id] = time.time()
                self.member_count += 1  # Increment member count
                prLightPurple(f"Replica '{replica_id}' added to membership.")
                self.print_membership()
            else:
                prYellow(f"Replica '{replica_id}' already exists in membership.")

    def delete_replica(self, replica_id):
        with self.lock:
            if replica_id in self.membership:
                del self.membership[replica_id]
                self.member_count -= 1  # Decrement member count
                prRed(f"Replica '{replica_id}' deleted from membership.")
                self.print_membership()
            else:
                prYellow(f"Replica '{replica_id}' not found in membership.")

    def print_membership(self):
        prPurple("Current Membership List:")
        if self.membership:
            for replica, timestamp in self.membership.items():
                formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
                prPurple(f"  - {replica}: Last updated at {formatted_time}")
        else:
            prPurple("  [No replicas currently in membership]")

    def start(self):
        while True:
            conn, addr = self.server_socket.accept()
            threading.Thread(target=self.handle_lfd_connection, args=(conn, addr), daemon=True).start()

def main():
    GFD_IP = '0.0.0.0'
    GFD_PORT = 12345
    gfd = GFD(GFD_IP, GFD_PORT, heartbeat_interval=5)
    try:
        gfd.start()
    except KeyboardInterrupt:
        prYellow("GFD interrupted by user.")
    finally:
        gfd.server_socket.close()
        prRed("GFD shutdown.")

if __name__ == '__main__':
    main()
