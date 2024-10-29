import time
from client import Client

def main():
    SERVER_PORT = 12345
    CLIENT_ID = '1'

    # Create a client instance that will connect to all 3 servers
    client = Client(SERVER_PORT, CLIENT_ID)
    client.connect()

    try:
        while True:
            # Send a message to all servers
            client.send_message("update")
            client.receive_response()
            time.sleep(2)
    except KeyboardInterrupt:
        print("Client exiting...")
    finally:
        # Send exit message to all servers and close the connections
        client.send_message("exit")
        client.close_connections()

if __name__ == '__main__':
    main()