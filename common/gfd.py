import socket
import time
import threading
from communication_utils import *

COMPONENT_ID = "GFD"
membership = {}
lfd_connections = {}  # Map LFD component IDs to their connections
member_count = 0
lock = threading.Lock()
rm_socket = None
heartbeat_interval = 5

def register_with_rm(rm_ip, rm_port):
    """Registers GFD with RM by opening a persistent connection and sending the initial member count."""
    global rm_socket, member_count
    try:
        rm_socket = connect_to_socket(rm_ip, rm_port)
        if rm_socket:
            message = create_message(COMPONENT_ID, "register", member_count=member_count)
            send(rm_socket, message, "RM")
            printP(f"GFD registered with RM: {member_count} members")
    except socket.error as e:
        printR(f"Failed to register with RM: {e}")

def handle_lfd_connection(conn, addr):
    try:
        # Receive the initial registration message from the LFD
        message = receive(conn, COMPONENT_ID)

        # Retrieve and print the component_id from the LFD's registration message
        component_id = message.get("component_id", "Unknown")
        printP(f"Received registration from {component_id} at {addr}")
        lfd_connections[component_id] = conn  # Store the LFD connection
        threading.Thread(target=send_heartbeat_continuously, args=(conn, addr, component_id), daemon=True).start()

        # Handle further messages from this LFD in a loop
        while True:
            message = receive(conn, COMPONENT_ID)
            if not message:
                printR(f"LFD at {addr} disconnected.")
                break

            # Process the incoming message from the LFD
            if "LFD" in message.get("component_id"):
                handle_lfd_message(message)
            else:
                printY(f"Ignored message from unknown component: {message.get('component_id')}")
    except socket.error as e:
        printR(f"Error receiving message from LFD at {addr}: {e}")
    finally:
        conn.close()
        lfd_connections.pop(component_id, None)  # Remove the LFD connection on disconnect

def handle_lfd_message(message):
    action = message.get("message", "")
    server_id = message.get("message_data", "")
    if action == "add replica" and server_id:
        add_replica(server_id)
    elif action == "remove replica" and server_id:
        delete_replica(server_id)
    elif action == "heartbeat acknowledgment":
        pass
    else:
        printLP(f"Unknown action '{action}' from LFD")

def handle_rm_message(message):
    """Handles messages from the RM."""
    global lfd_connections
    action = message.get("message", "")
    server_id = message.get("server_id", "")
    if action == "recover_server" and server_id:
        lfd_connection = lfd_connections.get(f"LFD{server_id[-1]}")
        if lfd_connection:
            try:
                recovery_message = create_message(COMPONENT_ID, "recover_server", server_id=server_id)
                send(lfd_connection, recovery_message, f"LFD{server_id[-1]}")
                printG(f"Forwarded recovery message to LFD for server {server_id}")
            except socket.error as e:
                printR(f"Failed to forward recovery message to LFD for server {server_id}: {e}")
        else:
            printR(f"No LFD found for server {server_id}. Recovery message not sent.")

def send_heartbeat_continuously(conn, addr, component_id):
    """Sends heartbeat messages continuously to an LFD."""
    while True:
        try:
            message = create_message(COMPONENT_ID, "heartbeat")
            send(conn, message, component_id)
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
            printG(f"Replica '{replica_id}' added to membership.")
            print_membership()
            send_update_to_rm(replica_id)

def delete_replica(server_id):
    """Removes a replica from the membership and notifies RM."""
    global member_count
    with lock:
        if server_id in membership:
            del membership[server_id]
            member_count -= 1
            printR(f"Replica '{server_id}' deleted from membership.")
            print_membership()
            send_update_to_rm(server_id)

def send_update_to_rm(server_id):
    """Sends updated membership count to RM."""
    global rm_socket, member_count
    if rm_socket:
        update_membership = create_message(COMPONENT_ID, "update_membership", member_count=member_count, server_id=server_id)
        send(rm_socket, update_membership, "RM")
        printG(f"Sent updated membership count to RM: {member_count}")
    else:
        printR("No active connection to RM.")

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
    GFD_IP = '0.0.0.0'
    GFD_PORT = 12345
    RM_IP = '127.0.0.1'
    RM_PORT = 12346

    server_socket = initialize_component(COMPONENT_ID, "Global Fault Detector", GFD_IP, GFD_PORT, 5)

    # Register with RM
    register_with_rm(RM_IP, RM_PORT)

    try:
        while True:
            conn, addr = server_socket.accept()
            threading.Thread(target=handle_lfd_connection, args=(conn, addr), daemon=True).start()

            # Listen for RM messages in a separate thread
            if rm_socket:
                threading.Thread(target=handle_rm_connection, args=(rm_socket,), daemon=True).start()
    except KeyboardInterrupt:
        printY("GFD interrupted by user.")
    finally:
        server_socket.close()
        printR("GFD shutdown.")

def handle_rm_connection(sock):
    """Handles incoming messages from RM."""
    try:
        while True:
            message = receive(sock, COMPONENT_ID)
            if message:
                handle_rm_message(message)
    except socket.error as e:
        printR(f"Connection to RM lost: {e}")

if __name__ == '__main__':
    main()
