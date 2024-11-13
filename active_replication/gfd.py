import socket  
import json
import time
import threading
import os
from dotenv import load_dotenv

def printG(skk): print(f"\033[92m{skk}\033[00m")         # Green
def printR(skk): print(f"\033[91m{skk}\033[00m")         # Red
def printY(skk): print(f"\033[93m{skk}\033[00m")         # Yellow
def printLP(skk): print(f"\033[94m{skk}\033[00m")        # Light Purple
def printP(skk): print(f"\033[95m{skk}\033[00m")         # Purple
def printC(skk): print(f"\033[96m{skk}\033[00m")         # Cyan

COMPONENT_ID = "GFD"
membership = {}
member_count = 0
lock = threading.Lock()
rm_socket = None
heartbeat_interval = 5   

def create_message(message_type, **kwargs):
    """Creates a standard message with component_id and timestamp."""
    message = {
        "component_id": COMPONENT_ID,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        "message": message_type
    }
    message.update(kwargs)
    return message

def register_with_rm(rm_ip, rm_port):
    """Registers GFD with RM by opening a persistent connection and sending the initial member count."""
    global rm_socket, member_count
    try:
        rm_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        rm_socket.connect((rm_ip, rm_port))
        message = create_message("register", member_count=member_count)
        rm_socket.sendall(json.dumps(message).encode())
        printP(f"GFD registered with RM: {member_count} members")
    except socket.error as e:
        printR(f"Failed to register with RM: {e}")

def handle_lfd_connection(conn, addr):
    try:
        # Receive the initial registration message from the LFD
        data = conn.recv(1024).decode()
        message = json.loads(data)

        # Retrieve and print the component_id from the LFD's registration message
        component_id = message.get("component_id", "Unknown")
        printP(f"Received registration from {component_id} at {addr}")
        threading.Thread(target=send_heartbeat_continuously, args=(conn, addr, component_id), daemon=True).start()

        # Handle further messages from this LFD in a loop
        while True:
            data = conn.recv(1024).decode()
            if not data:
                printR(f"LFD at {addr} disconnected.")
                break

            # Process the incoming message from the LFD
            try:
                message = json.loads(data)
                if "LFD" in message.get("component_id"):
                    handle_lfd_message(component_id, message)
                else:
                    printY(f"Ignored message from unknown component: {message.get('component_id')}")
            except json.JSONDecodeError as e:
                printR(f"Failed to decode message from LFD: {e}")
    except socket.error as e:
        printR(f"Error receiving message from LFD at {addr}: {e}")
    finally:
        conn.close()

def handle_lfd_message(component_id, message):
    """Processes messages from LFD for add/remove replicas or heartbeat acknowledgments."""
    action = message.get("message", "").lower()
    if action == "add replica":
        server_id = message.get("message_data", {}).get("server_id")
        if server_id:
            add_replica(server_id)
        else:
            printR("Add replica message missing 'server_id'.")
    elif action == "remove replica":
        server_id = message.get("message_data", {}).get("server_id")
        if server_id:
            delete_replica(server_id)
        else:
            printR("Remove replica message missing 'server_id'.")
    elif action == "heartbeat acknowledgment":   
        printY(f"Heartbeat acknowledgment received from {component_id}")
    else:
        printLP(f"Unknown action '{action}' from LFD")

def send_heartbeat_continuously(conn, addr, component_id):
    """Sends heartbeat messages continuously to an LFD."""
    while True:
        try:
            message = create_message("heartbeat")
            conn.sendall(json.dumps(message).encode())
            printC(f"Sent heartbeat to {component_id}")
            time.sleep(heartbeat_interval)
        except socket.error as e:
            printR(f"Failed to send heartbeat to LFD at {addr}: {e}")
            conn.close()
            break

def add_replica(replica_id):
    """Adds a replica to the membership and notifies RM."""
    global member_count
    with lock:
        if replica_id not in membership:
            membership[replica_id] = time.time()
            member_count += 1
            printLP(f"Replica '{replica_id}' added to membership.")
            print_membership()
            send_update_to_rm()

def delete_replica(replica_id):
    """Removes a replica from the membership and notifies RM."""
    global member_count
    with lock:
        if replica_id in membership:
            del membership[replica_id]
            member_count -= 1
            printR(f"Replica '{replica_id}' deleted from membership.")
            print_membership()
            send_update_to_rm()

def send_update_to_rm():
    """Sends updated membership count to RM."""
    global rm_socket, member_count
    update_membership = create_message("update_membership", member_count=member_count)
    try:
        if rm_socket:
            rm_socket.sendall(json.dumps(update_membership).encode())
            printG(f"Sent updated membership count to RM: {member_count}")
        else:
            printR("No active connection to RM.")
    except socket.error as e:
        printR(f"Failed to send update to RM: {e}")

def print_membership():
    """Prints the current membership list."""
    printP("Current Membership List:")
    if membership:
        for replica, timestamp in membership.items():
            formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
            printP(f"  - {replica}: Last updated at {formatted_time}")
    else:
        printP("  [No replicas currently in membership]")

def main():
    global server_socket

    GFD_IP = '0.0.0.0'
    GFD_PORT = 12345
    RM_IP = '127.0.0.1'
    RM_PORT = 12346

    printC("====================================")
    printC("    Global Fault Detector (GFD)     ")
    printC(f"GFD active on {GFD_IP, GFD_PORT}")
    printC("====================================")

    # Initialize and bind the server socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((GFD_IP, GFD_PORT))
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.listen(5)

    # Register with RM
    register_with_rm(RM_IP, RM_PORT)

    try:
        while True:
            conn, addr = server_socket.accept()
            threading.Thread(target=handle_lfd_connection, args=(conn, addr), daemon=True).start()
    except KeyboardInterrupt:
        printY("GFD interrupted by user.")
    finally:
        server_socket.close()
        printR("GFD shutdown.")

if __name__ == '__main__':
    main()