import socket

client = socket.socket()
ip = '192.168.0.114'
port = 22564
client.connect((ip, port))
while True:
    date = input('>>:').strip()
    if not date:
        break
    else:
        client.sendall(date.encode('utf-8'))
        response = client.recv(1024)
        print(response.decode('utf-8'))
client.close()
