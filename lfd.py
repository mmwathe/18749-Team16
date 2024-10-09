import socket
import time
import json
import argparse
import uuid
from message import Message

# Define color functions for printing
def prGreen(skk): print(f"\033[92m{skk}\033[00m")
def prRed(skk): print(f"\033[91m{skk}\033[00m")
def prYellow(skk): print(f"\033[93m{skk}\033[00m")
def prLightPurple(skk): print(f"\033[94m{skk}\033[00m")
def prPurple(skk): print(f"\033[95m{skk}\033[00m")
def prCyan(skk): print(f"\033[96m{skk}\033[00m")

class LFD:
    def __init__(self, server_ip, server_port, gfd_ip, gfd_port,client_id, heartbeat_interval=2):
        self.server_ip = server_ip
        self.server_port = server_port
        self.gfd_ip = gfd_ip
        self.gfd_port = gfd_port
        self.client_id = client_id
        self.client_address = 0
        self.client_socket = 0
        self.heartbeat_interval = heartbeat_interval
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.gfdsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setblocking(False)
        self.server_alive = True

    '''def connect(self):
        try:
            self.socket.connect((self.server_ip, self.server_port))
            prGreen(f"Connected to server at {self.server_ip}:{self.server_port}")
            return True
        except Exception as e:
            prRed(f"Failed to connect to server: {e}")
            self.server_alive = False
            return False '''

    def start(self):
        try:
            self.socket.bind((self.server_ip, self.server_port))
            self.socket.listen(1)  # Listen for up to 3 simultaneous connections
            prGreen(f"Server listening on {self.server_ip}:{self.server_port}")
        except Exception as e:
            prRed(f"Failed to start server: {e}")
            return False
        return True

    
    def connectGfd(self):
        try:
            self.gfdsocket.connect((self.gfd_ip, self.gfd_port))
            prGreen(f"Connected to gfd at {self.gfd_ip}:{self.gfd_port}")
            return True
        except Exception as e:
            prRed(f"Failed to connect to gfd: {e}")
            return False
    # for the server
    def accept_new_connection(self):
        try:
            client_socket, client_address = self.socket.accept()
            client_socket.setblocking(False)  # Non-blocking mode for client
            self.client_socket = client_socket
            self.client_address = client_address
            self.send_register()
            prCyan(f"New connection established with {client_address}")

        except BlockingIOError:
            # No new connections, continue
            
            pass

    def send_register(self, option):
        if(option == 1):
            message = Message(self.client_id, "add replica S1")
            message_json = json.dumps({
                "timestamp": message.timestamp,
                "client_id": message.client_id,
                "message": message.message,
                "message_id": message.message_id
            })
            try:
                prYellow(str(message))  # Print the formatted heartbeat message
                self.gfdsocket.sendall(message_json.encode())
            except socket.error as e:
                prRed(f"Failed to send replica addition. Error: {e}. Retrying...")
        else:
            message = Message(self.client_id, "delete replica S1")
            message_json = json.dumps({
                "timestamp": message.timestamp,
                "client_id": message.client_id,
                "message": message.message,
                "message_id": message.message_id
            })
            try:
                prYellow(str(message))  # Print the formatted heartbeat message
                self.gfdsocket.sendall(message_json.encode())
            except socket.error as e:
                prRed(f"Failed to send replica deletion. Error: {e}. Retrying...")


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
            self.client_socket.sendall(message_json.encode())
        except socket.error as e:
            prRed(f"Failed to send heartbeat. Error: {e}. Retrying...")
            # Sleep for a short time before retrying
            time.sleep(self.heartbeat_interval)

   
    def send_heartbeat_response(self, data):
        try:
            # Attempt to parse the data as JSON
            message = json.loads(data)
            timestamp = message.get('timestamp', 'Unknown')
            client_id = message.get('client_id', 'Unknown')
            content = message.get('message', 'Unknown')
            message_id = message.get('message_id', 'Unknown')

            prPurple("=" * 80)

            
            prRed(f"{timestamp:<20} {'Received heartbeat from:':<20} {client_id}")
            prRed(f"{'':<20} {'Sending heartbeat...'}")
            response = {
                "message": "heartbeat response",
                "server_id": self.client_id,
                "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                
            } 
            self.gfdsocket.sendall(json.dumps(response).encode())
        except json.JSONDecodeError:
            prRed(f"{'':<20} Received malformed message: {data}")

    def receive_response(self, option):
        if(option == 1):
            try:
                response = self.gfdsocket.recv(1024).decode()    
                # Parse the response JSON
                response_data = json.loads(response)
                server_id = response_data.get('server_id', 'Unknown')
                timestamp = response_data.get('timestamp', 'Unknown')
                message = response_data.get('message', 'Unknown')
                state = response_data.get('state', 'Unknown')
                content = response_data.get('message', 'Unknown')    
                # Print the parsed response in a formatted manner
                prPurple("=" * 80)
                prYellow(f"{timestamp:<20} {server_id} -> {self.client_id}")
                prLightPurple(f"{'':<20} {'Message:':<15} {message}")
                prLightPurple(f"{'':<20} {'State:':<15} {state}")
                if content.lower() == 'heartbeat':
                    self.send_heartbeat_response(response)
                
                return response
            except (socket.error, json.JSONDecodeError):
                prRed("No response received. Server might be down or sent an invalid response.")
                return None
        else:
            try:
                response = self.socket.recv(1024).decode()    
                # Parse the response JSON
                response_data = json.loads(response)
                server_id = response_data.get('server_id', 'Unknown')
                timestamp = response_data.get('timestamp', 'Unknown')
                message = response_data.get('message', 'Unknown')
                state = response_data.get('state', 'Unknown')
                content = response_data.get('message', 'Unknown')    
                # Print the parsed response in a formatted manner
                prPurple("=" * 80)
                prYellow(f"{timestamp:<20} {server_id} -> {self.client_id}")
                prLightPurple(f"{'':<20} {'Message:':<15} {message}")
                prLightPurple(f"{'':<20} {'State:':<15} {state}")
                if content.lower() == 'heartbeat':
                    self.send_heartbeat_response(response)
                
                return response
            except (socket.error, json.JSONDecodeError):
                prRed("No response received. Server might be down or sent an invalid response.")
                return None


    

    def monitor_server(self):
        self.send_heartbeat()
        start_time = time.time()
        response = self.receive_response(0)
        if response is None:
            # No response means server did not respond within the expected time frame
            prRed("Server did not respond to the heartbeat.")
            # We keep the LFD running and will continue to send heartbeats
            #self.socket.close()
            time.sleep(self.heartbeat_interval)
            

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
    GFD_IP = '172.26.59.208'
    GFD_PORT = 12345
    CLIENT_ID = 'LFD1'

    # Create an instance of LFD with the specified heartbeat frequency
    lfd_client = LFD(SERVER_IP, SERVER_PORT, GFD_IP, GFD_PORT, CLIENT_ID, heartbeat_interval=args.heartbeat_freq)

    if not lfd_client.start():
        return
    
    if not lfd_client.connectGfd():
        return

    count = 0

    try:
        while(True):
            lfd_client.accept_new_connection()
            lfd_client.receive_response(1)

            #lfd_client.monitor_server()
            time.sleep(0.1)
            count += 1
            if(count == 3):
                lfd_client.send_register(1)
            


        
    
    except KeyboardInterrupt:
        prYellow("Server is shutting down...")
'''
    try:
        lfd_client.monitor_server()
    except KeyboardInterrupt:
        prYellow("LFD interrupted by user.")
    finally:
        lfd_client.close_connection()
'''
if __name__ == '__main__':
    main()