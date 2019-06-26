import socket


ip = ''
port = 22564
server = socket.socket()
server.bind((ip, port))
server.listen(5)

while True:
    print("server waiting...")
    conn, client_address = server.accept()
    while True:
        try:
            client_data = conn.recv(1024)
            if not client_data:
                print(client_address, "close connection!")
                break
            else:
                print(client_data.decode('utf-8'))
        except Exception:
            print(client_address, "close connection!")
            break
        date = input('>>:').strip()
        conn.sendall(date.encode('utf-8'))
    conn.close()

