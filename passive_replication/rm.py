import socket
import json
from communication_utils import *

def handle_GFD_message(sock, message):
    global MEMBER_COUNT

    if message.get("component_id") != "GFD":
        printR(f"Received message from unknown sender: {message.get('component_id')}")
    elif message.get("message") == "register":
        gfd_member_count = message.get("member_count", 0)
    elif message.get("message") == "update_membership":
        new_member_count = message.get("member_count", MEMBER_COUNT)
        if new_member_count > MEMBER_COUNT:
            printG(f"RM Membership Increased: {new_member_count} available servers")
        elif new_member_count < MEMBER_COUNT:
            printR(f"RM Membership Decreased: {new_member_count} available servers")
        else:
            printY(f"RM Membership Unchanged: {new_member_count} available servers")
        MEMBER_COUNT = new_member_count
    else:
        timestamp = message.get("timestamp", "unknown time")
        printY(f"{timestamp}: Received unknown message from GFD: {message}")

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