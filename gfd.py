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
                        start = buffer.find('{')
                        if start == -1:
                            buffer = ""
                            break
                        else:
                            buffer = buffer[start:]
                            in_json = True
                            brace_counter = 1
                    else:
                        for i in range(1, len(buffer)):
                            if buffer[i] == '{':
                                brace_counter += 1
                            elif buffer[i] == '}':
                                brace_counter -= 1
                                if brace_counter == 0:
                                    json_str = buffer[:i+1]
                                    buffer = buffer[i+1:]
                                    in_json = False
                                    try:
                                        message = json.loads(json_str)
                                        print(f"Received message from LFD at {addr}")
                                        # Check for 'add' or 'delete' action for a replica
                                        action = message.get("message")
                                        replica_id = message.get("client_id")
                                        if action.startswith("add replica"):
                                            self.add_replica(replica_id)
                                        elif action.startswith("delete replica"):
                                            self.delete_replica(replica_id)
                                        elif action == "heartbeat acknowledgment":
                                            print(f"Heartbeat acknowledgment received from {replica_id} at {addr}")
                                        else:
                                            print(f"Unknown action '{action}' from {addr}")
                                    except json.JSONDecodeError as e:
                                        print(f"Error decoding JSON message: {e}")
                                    break
                        else:
                            break
            except socket.error as e:
                print(f"Error receiving message from LFD at {addr}: {e}")
                break

        conn.close()

    def send_heartbeat(self, conn, addr):
        try:
            # Sending heartbeat message
            message = {
                "message": "heartbeat",
                "gfd_id": "GFD",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            }
            conn.sendall((json.dumps(message)).encode())  # No newline delimiter
            print(f"Sent heartbeat to LFD at {addr}")
            time.sleep(self.heartbeat_interval)  # Wait before sending the next heartbeat
        except socket.error as e:
            print(f"Failed to send heartbeat to LFD at {addr}: {e}")
            conn.close()

    def add_replica(self, replica_id):
        if replica_id not in self.membership:
            self.membership[replica_id] = time.time()
            self.print_membership()
        else:
            self.print_membership()

    def delete_replica(self, replica_id):
        if replica_id in self.membership:
            del self.membership[replica_id]
            self.print_membership()
        else:
            self.print_membership()

    def print_membership(self):
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
