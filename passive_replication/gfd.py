import socket
import time
import threading
from communication_utils import *

COMPONENT_ID = "GFD"
membership = {}
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
        threading.Thread(target=send_heartbeat_continuously, args=(conn, addr, component_id), daemon=True).start()

        # Handle further messages from this LFD in a loop
        while True:
            message = receive(conn, COMPONENT_ID)
            if not message:
                printR(f"LFD at {addr} disconnected.")
                break

            # Process the incoming message from the LFD
            if "LFD" in message.get("component_id"):
                handle_lfd_message(component_id, message)
            else:
                printY(f"Ignored message from unknown component: {message.get('component_id')}")
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
        pass
    else:
        printLP(f"Unknown action '{action}' from LFD")

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
    if rm_socket:
        update_membership = create_message(COMPONENT_ID, "update_membership", member_count=member_count)
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
    except KeyboardInterrupt:
        printY("GFD interrupted by user.")
    finally:
        server_socket.close()
        printR("GFD shutdown.")

if __name__ == '__main__':
    main()
    