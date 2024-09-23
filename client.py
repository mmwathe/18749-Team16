# Client-side (client.py)
import socket

def client_program():
    host = socket.gethostname()  # As both code is running on same pc
    port = 6644  # Socket server port number

    client_socket = socket.socket()  # Create a socket object
    client_socket.connect((host, port))  # Connect to the server

    message = input(" -> ")  # Take input

    while message.lower().strip() != 'bye':
        client_socket.send(message.encode())  # Send message to the server
        data = client_socket.recv(1024).decode()  # Receive response from the server
        print('Received from server: ' + data)  # Show the response
        message = input(" -> ")  # Again take input

    client_socket.close()  # Close the connection

if __name__ == '__main__':
    client_program()