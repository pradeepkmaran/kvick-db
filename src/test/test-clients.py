import socket

def test_service(port, commands):
    for cmd in commands:
        s = socket.socket()
        s.connect(('localhost', port))
        s.sendall(cmd.encode())
        response = s.recv(1024).decode()
        print(f"Response from port {port}: {response.strip()}")
        s.close()

# Test service on port 5000
commands_5000 = ['SET username Pradeep\n', 'GET username\n']
test_service(5000, commands_5000)

# Test service on port 8081
commands_8081 = ['SET username Alice\n', 'GET username\n']
test_service(8081, commands_8081)
