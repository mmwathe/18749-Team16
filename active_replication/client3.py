from client import Client

def main():
    client = Client(server_port=12346, client_id="C3")
    client.run()

if __name__ == "__main__":
    main()