# Server-side (server.py)
import socket

# Define the IP and Port for the server
SERVER_IP = '0.0.0.0'  # Listen on all available interfaces
SERVER_PORT = 12345

# Create a TCP socket
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((SERVER_IP, SERVER_PORT))
server_socket.listen(1)

print(f"Server listening on {SERVER_IP}:{SERVER_PORT}")

# Wait for a client to connect
client_socket, client_address = server_socket.accept()
print(f"Connection established with {client_address}")

while True:
    # Receive data from the client
    data = client_socket.recv(1024).decode()
    
    # Check if the data is a 'ping' message
    if data.lower() == 'ping':
        print("Received 'ping' from client. Sending 'pong'...")
        client_socket.sendall("pong".encode())
    else:
        print(f"Received unknown message: {data}")

    # Break the loop if client sends 'exit'
    if data.lower() == 'exit':
        break

# Close the connection
client_socket.close()
server_socket.close()
print("Server shutdown.")