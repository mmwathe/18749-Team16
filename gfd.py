import socket
import json
import time
import threading

LFD_IPS = ['172.26.77.220', '127.0.0.1', '172.26.105.167']

class GFD:
    def __init__(self, host, port, heartbeat_interval=5):
        self.host = host
        self.port = port
        self.heartbeat_interval = heartbeat_interval  # Interval between heartbeats
        self.membership = {}  # To track replicas (replica_id -> timestamp)
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        print(f"GFD started. Listening on {self.host}:{self.port}")
    def handle_lfd_connection(self, conn, addr):
        print(f"Connected to LFD at {addr}")
        buffer = ""
        brace_counter = 0
        in_json = False
        while True:
            try:
                # Send heartbeat to LFD
                self.send_heartbeat(conn, addr)
                # Receive message from LFD
                data = conn.recv(1024).decode()
                if not data:
                    print(f"LFD at {addr} disconnected.")
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
                                        print(f"Received message from LFD at {addr}: {message}")
                                        # Check for 'add' or 'delete' action for a replica
                                        action = message.get("message")
                                        replica_id = message.get("client_id")
                                        if action.startswith("add replica"):
                                            _, _, replica = action.partition("add replica ")
                                            replica = replica.strip()
                                            self.add_replica(replica)
                                        elif action.startswith("remove replica"):
                                            _, _, replica = action.partition("remove replica ")
                                            replica = replica.strip()
                                            self.delete_replica(replica)
                                        elif action == "heartbeat response":
                                            print(f"Heartbeat response received from {replica_id} at {addr}")
                                        else:
                                            print(f"Unknown action '{action}' from {addr}")
                                    except json.JSONDecodeError as e:
                                        print(f"Error decoding JSON message: {e}")
                                    break
                        else:
                            # No complete JSON object found yet
                            break
            except socket.error as e:
                print(f"Error receiving message from LFD at {addr}: {e}")
                break
        conn.close()
    
    def getLFDIDfromIP(self):
        return {ip: f"{index+1}" for index, ip in enumerate(LFD_IPS)}
    
    def send_heartbeat(self, conn, addr):
        try:
            # Sending heartbeat message
            message = {
                "message": "heartbeat",
                "gfd_id": "GFD",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            }
            conn.sendall((json.dumps(message)).encode())  # No newline delimiter
            print(f"Sent heartbeat to LFD at LFD{self.getLFDIDfromIP(addr[1])}")
            time.sleep(self.heartbeat_interval)  # Wait before sending the next heartbeat
        except socket.error as e:
            print(f"Failed to send heartbeat to LFD at {addr}: {e}")
            conn.close()
    def add_replica(self, replica_id):
        if replica_id not in self.membership:
            self.membership[replica_id] = time.time()
            print(f"Replica {replica_id} added to membership")
        else:
            print(f"Replica {replica_id} already exists in membership")
    def delete_replica(self, replica_id):
        if replica_id in self.membership:
            del self.membership[replica_id]
            print(f"Replica {replica_id} deleted from membership")
        else:
            print(f"Replica {replica_id} not found in membership")
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
        print("GFD interrupted by user.")
if __name__ == '__main__':
    main()