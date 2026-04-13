import socket
import threading

def send_command(command):
    try:
        s = socket.socket()
        s.connect(('localhost', 5000))
        s.sendall(command.encode())
        response = s.recv(1024).decode()
        print(f"Command: {command.strip()} -> Response: {response.strip()}")
    except Exception as e:
        print(f"Error with command {command.strip()}: {e}")
    finally:
        s.close()

# Commands to be sent concurrently to the same key 'username'
commands = [
    'SET username Alice\n',
    'SET username Bob\n',
    'GET username\n',
    'SET username Charlie\n',
    'GET username\n'
]

threads = []

for cmd in commands:
    thread = threading.Thread(target=send_command, args=(cmd,))
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()

print("All requests completed.")
