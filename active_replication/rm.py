import socket
import json

def printG(skk): print(f"\033[92m{skk}\033[00m")         # Green
def printR(skk): print(f"\033[91m{skk}\033[00m")         # Red
def printY(skk): print(f"\033[93m{skk}\033[00m")         # Yellow
def printLP(skk): print(f"\033[94m{skk}\033[00m")        # Light Purple
def printP(skk): print(f"\033[95m{skk}\033[00m")         # Purple
def printC(skk): print(f"\033[96m{skk}\033[00m")         # Cyan

COMPONENT_ID = "RM"
RM_IP = '127.0.0.1'
RM_PORT = 12346
MEMBER_COUNT = 0

def handle_GFD_message(message):    
    global MEMBER_COUNT
    if message.get("message") == "register":
        gfd_member_count = message.get("member_count", 0)
        printP(f"GFD registered: RM acknowledges {gfd_member_count} members")
    elif message.get("message") == "update_membership":
        new_member_count = message.get("member_count", MEMBER_COUNT)
        if new_member_count > MEMBER_COUNT:
            printP(f"RM Membership Increased: {new_member_count} available servers")
        elif new_member_count < MEMBER_COUNT:
            printP(f"RM Membership Decreased: {new_member_count} available servers")
        else:
            printP(f"RM Membership Unchanged: {new_member_count} available servers")
        MEMBER_COUNT = new_member_count
    else:
        timestamp = message.get("timestamp", "unknown time")
        printY(f"{timestamp}: Received unknown message from GFD: {message}")

def main():
    rm_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    rm_socket.bind((RM_IP, RM_PORT))
    rm_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    rm_socket.listen(1)  # Listen for a single connection from the GFD

    printC("====================================")
    printC("      Replication Manager (RM)      ")
    printC(f"RM active on {RM_IP, RM_PORT}")
    printC("====================================")

    printY("Waiting for GFD to connect...")

    while True:
        try:
            # Accept a connection from the GFD
            conn, addr = rm_socket.accept()
            printG(f"Connected to GFD at {addr}")

            # Continuously receive messages from the GFD
            while True:
                data = conn.recv(1024).decode()
                if not data:
                    printR("GFD disconnected.")
                    break

                # Parse the incoming message
                try:
                    message = json.loads(data)
                    if message.get("component_id") == "GFD":
                        handle_GFD_message(message)
                    else:
                        printY("Ignored message from unknown component")
                        
                except json.JSONDecodeError as e:
                    printR(f"Failed to decode message from GFD: {e}")

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