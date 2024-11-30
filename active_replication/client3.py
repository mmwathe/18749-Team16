import time, sys, os
from client import Client

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))
from communication_utils import *


def main():
    CLIENT_ID = 'C3'
    SERVER_PORT = 12346
    
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