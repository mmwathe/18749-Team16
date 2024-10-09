import socket 
import json
import time
import threading

class GFD:
    def __init__(self, host, port, heartbeat_interval=5):
        self.host = host
        self.port = port
        self.heartbeat_interval = heartbeat_interval  # Interval between heartbeats
        self.membership = {}  # To track replicas (replica_id -> timestamp)
        self.lock = threading.Lock()  # Lock for thread-safe access to membership
        self.conn_to_replica = {}  # Map connection to replica_id

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        print(f"GFD started. Listening on {self.host}:{self.port}")
        self.print_membership()

    def handle_lfd_connection(self, conn, addr):
        print(f"Connected to LFD at {addr}")
        buffer = ""
        brace_counter = 0
        in_json = False

        # Start a separate thread for sending heartbeats
        threading.Thread(target=self.heartbeat_sender, args=(conn, addr), daemon=True).start()

        while True:
            try:
                data = conn.recv(1024).decode()
                if not data:
                    print(f"LFD at {addr} disconnected.")
                    break

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

                                        # Extract 'message' field to determine action
                                        action = message.get("message")
                                        message_data = message.get("message_data", {})

                                        if action.startswith("add replica"):
                                            # Extract replica_id from message_data
                                            replica_id = message_data.get("server_id")
                                            if replica_id:
                                                self.add_replica(replica_id)
                                                # Map connection to replica_id
                                                with self.lock:
                                                    self.conn_to_replica[conn] = replica_id
                                            else:
                                                print(f"Missing 'server_id' in message_data from {addr}: {message}")
                                        elif action.startswith("remove replica"):
                                            # Extract replica_id from message_data
                                            replica_id = message_data.get("server_id")
                                            if replica_id:
                                                self.delete_replica(replica_id)
                                                # Remove mapping
                                                with self.lock:
                                                    if conn in self.conn_to_replica:
                                                        del self.conn_to_replica[conn]
                                            else:
                                                print(f"Missing 'server_id' in message_data from {addr}: {message}")
                                        elif action == "heartbeat acknowledgment":
                                            print(f"Heartbeat acknowledgment received from LFD at {addr}")
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

        # Clean up upon disconnection
        with self.lock:
            replica_id = self.conn_to_replica.get(conn)
            if replica_id:
                self.delete_replica(replica_id)
                del self.conn_to_replica[conn]
        conn.close()
        print(f"Connection with LFD at {addr} closed.")

    def heartbeat_sender(self, conn, addr):
        while True:
            try:
                # Sending heartbeat message
                message = {
                    "message": "heartbeat",
                    "gfd_id": "GFD",
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                }
                conn.sendall(json.dumps(message).encode())  # No newline delimiter
                print(f"Sent heartbeat to LFD at {addr}")
                time.sleep(self.heartbeat_interval)  # Wait before sending the next heartbeat

            except socket.error as e:
                print(f"Failed to send heartbeat to LFD at {addr}: {e}")
                conn.close()
                break

    def add_replica(self, replica_id):
        with self.lock:
            if replica_id not in self.membership:
                self.membership[replica_id] = time.time()
                print(f"Replica {replica_id} added to membership")
                self.print_membership()
            else:
                print(f"Replica {replica_id} already exists in membership")

    def delete_replica(self, replica_id):
        with self.lock:
            if replica_id in self.membership:
                del self.membership[replica_id]
                print(f"Replica {replica_id} deleted from membership")
                self.print_membership()
            else:
                print(f"Replica {replica_id} not found in membership")

    def print_membership(self):
        with self.lock:
            member_count = len(self.membership)
            members = ", ".join(self.membership.keys())
            print(f"GFD: {member_count} member{'s' if member_count != 1 else ''}: {members}")

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
