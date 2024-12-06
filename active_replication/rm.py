import socket
import json
import sys, os, threading
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))
from communication_utils import *

reliable_server = "S1"
available_servers = []
assign_initial_reliable = False

def handle_GFD_message(sock, message):
    global MEMBER_COUNT

    if message.get("component_id") != "GFD":
        printR(f"Received message from unknown sender: {message.get('component_id')}")
    elif message.get("message") == "register":
        gfd_member_count = message.get("member_count", 0)
    elif message.get("message") == "update_membership":
        new_member_count = message.get("member_count", MEMBER_COUNT)
        server_id = message.get("server_id", "unknown server")
        if new_member_count < MEMBER_COUNT:
            printR(f"RM Membership Decreased: {new_member_count} available servers")
            printY(f"Attempting to Automatically Recover {server_id}")

            # Send recovery command
            send(sock, create_message("RM", "recover_server", server_id=server_id), "GFD")
            remove_server(server_id, sock)
        elif new_member_count > MEMBER_COUNT:
            printG(f"RM Membership Increased: {new_member_count} available servers")
            add_server(server_id)
            global assign_initial_reliable
            if not assign_initial_reliable:
                # Assign the initial reliable server
                printY("Assigning initial reliable server.")
                assign_initial_reliable = True
                promote_new_reliable(sock)
        else:
            printY(f"RM Membership Unchanged: {new_member_count} available servers")
        MEMBER_COUNT = new_member_count
    else:
        timestamp = message.get("timestamp", "unknown time")
        printY(f"{timestamp}: Received unknown message from GFD: {message}")

def add_server(server_id):
    """Adds a server to the available servers list."""
    global available_servers
    if server_id not in available_servers:
        available_servers.append(server_id)
        printG(f"Server {server_id} added to available servers. Current list: {available_servers}")

def remove_server(server_id, gfd_sock):
    """Removes a server from the available servers list and handles reliable promotion if needed."""
    global available_servers, reliable_server

    if server_id in available_servers:
        available_servers.remove(server_id)
        printR(f"Server {server_id} removed from available servers. Current list: {available_servers}")

        # Handle reliable server change if necessary
        if server_id == reliable_server:
            promote_new_reliable(gfd_sock)

def promote_new_reliable(gfd_sock):
    """Promotes a new reliable server and notifies GFD, LFD, and the server."""
    global reliable_server, available_servers

    if "S1" in available_servers:
        new_reliable = "S1"
    elif "S2" in available_servers:
        new_reliable = "S2"
    elif "S3" in available_servers:
        new_reliable = "S3"
    else:
        new_reliable = None

    reliable_server = new_reliable

    # Notify GFD about the new reliable
    try:
        message = create_message("RM", "new_reliable", server_id=new_reliable)
        send(gfd_sock, message, "GFD")
        printG(f"Notified GFD that {new_reliable} is the new reliable server.")
    except Exception as e:
        printR(f"Failed to notify GFD about new reliable server: {e}")

def main():
    COMPONENT_NAME = "Replication Manager"
    COMPONENT_ID = "RM"
    RM_IP = '127.0.0.1'
    RM_PORT = 12346
    
    global MEMBER_COUNT
    MEMBER_COUNT = 0

    rm_socket = initialize_component(COMPONENT_ID, COMPONENT_NAME, RM_IP, RM_PORT, 1)

    printY("Waiting for GFD to connect...")

    while True:
        try:
            # Accept a connection from the GFD
            conn, addr = rm_socket.accept()
            printG(f"Connected to GFD at {addr}")
            while True:
                message = receive(conn, COMPONENT_ID)
                if not message:
                    printR("GFD disconnected.")
                    break
                handle_GFD_message(conn, message)
            conn.close()
            printY("Waiting to connect to GFD")  # Wait for GFD reconnection if disconnected
        except (socket.error, json.JSONDecodeError) as e:
            printR(f"Failed to process message from GFD: {e}")
            conn.close()
        except KeyboardInterrupt:
            printR("RM interrupted by user.")
            break

    # Close the RM socket when finished
    rm_socket.close()
    printR("RM shutdown.")

if __name__ == '__main__':
    main()