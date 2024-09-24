# Client-side (client.py)
import socket
import time

# Define the IP and Port of the server
SERVER_IP = '0.0.0.0'  # Replace with server's IP address
SERVER_PORT = 12345

# Create a TCP socket
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket.connect((SERVER_IP, SERVER_PORT))

print(f"Connected to server at {SERVER_IP}:{SERVER_PORT}")

try:
    while True:
        # Send 'ping' message to the server
        print("Sending 'ping' to server...")
        client_socket.sendall("ping".encode())
        
        # Wait for a response from the server
        response = client_socket.recv(1024).decode()
        print(f"Received from server: {response}")
        
        # Wait for 2 seconds before the next ping
        time.sleep(2)

        # Optionally, you can add a condition to break the loop, like:
        # if some_condition:
        #     client_socket.sendall("exit".encode())
        #     break

except KeyboardInterrupt:
    print("Client exiting...")

finally:
    client_socket.close()
    print("Client shutdown.")