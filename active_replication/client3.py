import time
from client import Client

# Define color functions for printing
def print_sent(skk): print("\033[96m{}\033[00m".format(skk))  # Cyan for sent messages
def print_received(skk): print("\033[95m{}\033[00m".format(skk))  # Purple for received messages
def printR(skk): print("\033[91m{}\033[00m".format(skk))  # Red for errors
def printY(skk): print("\033[93m{}\033[00m".format(skk))  # Yellow for warnings
def printG(skk): print("\033[92m{}\033[00m".format(skk))  # Green for registrations

def main():
    SERVER_PORT = 12346  # Match the server port with the server
    CLIENT_ID = 'C3'  # Client ID for identification

    # Create a client instance (IP addresses are handled in the client file)
    client = Client(SERVER_PORT, CLIENT_ID)
    client.connect()

    try:
        while True:
            # Attempt reconnections to servers if any are disconnected
            client.reconnect()

            # Send an update message and process responses
            client.send_to_all_servers("update")
            client.receive_from_all_servers()

            time.sleep(2)  # Delay between requests
    except KeyboardInterrupt:
        printY("Client exiting...")
    finally:
        # Send exit message to all servers and close the connections
        client.send_to_all_servers("exit")
        client.close_connections()

if __name__ == '__main__':
    main()
