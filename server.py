# Server-side (server.py)
import socket

def server_program():
    host = socket.gethostname()  # Get the hostname of the server
    port = 6644  # Choose a port above 1024

    server_socket = socket.socket()  # Create a socket object
    server_socket.bind((host, port))  # Bind the socket to a specific address and port

    server_socket.listen(2)  # Listen for incoming connections
    conn, address = server_socket.accept()  # Accept a connection
    print("Connection from: " + str(address))

    while True:
        data = conn.recv(1024).decode()  # Receive data from the client
        if not data:
            break
        print("from connected user: " + str(data))
        data = input(' -> ')  # Send data to the client
        conn.send(data.encode())  # Send data to the client

    conn.close()  # Close the connection

if __name__ == '__main__':
    server_program()