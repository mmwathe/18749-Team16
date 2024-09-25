import time
from dotenv import load_dotenv
import os
from client import Client


def main():
    SERVER_IP = os.getenv('SERVER_IP')
    SERVER_PORT = 12345
    CLIENT_ID = '1'  # Hardcoded client ID for now

    client = Client(SERVER_IP, SERVER_PORT, CLIENT_ID)

    if not client.connect():
        return

    try:
        while True:
            client.send_message("update")
            client.receive_response()
            time.sleep(2)
    except KeyboardInterrupt:
        print("Client exiting...")
    finally:
        client.send_message("exit")  # Send exit message to server
        client.close_connection()

if __name__ == '__main__':
    load_dotenv() 
    main()