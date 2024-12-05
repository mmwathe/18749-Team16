import socket
import json
import sys, os, threading
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))
from communication_utils import *

# Global Variables
available_servers = []  # List of active servers
primary_server = "S1"   # Initial primary server
client_sockets = []

assign_intial_primary = False

def handle_GFD_message(sock, message):
    global MEMBER_COUNT, available_servers, primary_server

    if message.get("component_id") != "GFD":
        printR(f"Received message from unknown sender: {message.get('component_id')}")
        return

    action = message.get("message")
    server_id = message.get("server_id", "unknown server")
    new_member_count = message.get("member_count", MEMBER_COUNT)

    if action == "register":
        # Handle initial registration
        printG(f"GFD registered with RM. Current member count: {new_member_count}")

    elif action == "update_membership":
        if new_member_count > MEMBER_COUNT:
            # Server added
            printG(f"RM Membership Increased: {new_member_count} available servers")
            add_server(server_id)
        elif new_member_count < MEMBER_COUNT:
            # Server removed
            printR(f"RM Membership Decreased: {new_member_count} available servers")
            printY(f"Attempting to Automatically Recover {server_id}")
            send(sock, create_message("RM", "recover_server", server_id=server_id), "GFD")
            remove_server(server_id, sock)
        else:
            printY(f"RM Membership Unchanged: {new_member_count} available servers")
        MEMBER_COUNT = new_member_count
        global assign_intial_primary
        if not assign_intial_primary:
            # Assign the initial primary server
            printY("Assigning initial primary server.")
            assign_intial_primary = True
            promote_new_primary(sock)

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
    """Removes a server from the available servers list and handles primary promotion if needed."""
    global available_servers, primary_server

    if server_id in available_servers:
        available_servers.remove(server_id)
        printR(f"Server {server_id} removed from available servers. Current list: {available_servers}")

        # Handle primary server change if necessary
        if server_id == primary_server:
            promote_new_primary(gfd_sock)

def promote_new_primary(gfd_sock):
    """Promotes a new primary server and notifies GFD, LFD, and the server."""
    global primary_server, available_servers

    if "S1" in available_servers:
        new_primary = "S1"
    elif "S2" in available_servers:
        new_primary = "S2"
    elif "S3" in available_servers:
        new_primary = "S3"
    else:
        printR("No available servers to promote to primary!")
        return

    primary_server = new_primary

    for sock in client_sockets:
        send(sock, create_message("RM", "primary_server", primary_server=new_primary), "Client")
    printY(f"Promoting {primary_server} to primary server.")

    # Notify GFD about the new primary
    try:
        message = create_message("RM", "new_primary", server_id=new_primary)
        send(gfd_sock, message, "GFD")
        printG(f"Notified GFD that {new_primary} is the new primary server.")
    except Exception as e:
        printR(f"Failed to notify GFD about new primary server: {e}")


def accept_client_connections(server_socket):
    """Accepts client connections and sends the primary server IP to clients upon connection."""
    global primary_server
    while True:
        print("hi")
        try:
            client_socket, client_address = server_socket.accept()
            print(client_socket)
            print(client_address)
            client_sockets.append(client_socket)
            printG(f"Client connected: {client_address}")

            # Send the primary server IP to the client
            response = create_message("RM", "primary_server", primary_server=primary_server)
            send(client_socket, response, "Client")

            # Close the client socket after sending the IP
            #client_socket.close()
            printG(f"Sent primary server IP {primary_server} to client.")
        except Exception as e:
            printR(f"Error accepting client connections: {e}")

def main():
    COMPONENT_NAME = "Replication Manager"
    COMPONENT_ID = "RM"
    RM_IP = '127.0.0.1'
    RM_PORT = 12346
    CLIENT_PORT = 13579

    global MEMBER_COUNT
    MEMBER_COUNT = 0

    rm_socket = initialize_component(COMPONENT_ID, COMPONENT_NAME, RM_IP, RM_PORT, 1)
    client_socket = initialize_component(COMPONENT_ID, "Client Listener", RM_IP, CLIENT_PORT, 1)

    threading.Thread(target=accept_client_connections, args=(client_socket,), daemon=True).start()

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