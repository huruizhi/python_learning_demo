import socket

client = socket.socket()
ip = '192.168.0.155'
port = 22564
client.connect((ip, port))
while True:
    date = input('cmd:').strip()
    if not date:
        continue
    elif date == 'q':
        break
    else:
        client.sendall(date.encode('utf-8'))
        output = ''
        cmd_ack_result = client.recv(512)
        cmd_rev_msg = cmd_ack_result.decode('utf-8').split("|")
        client.sendall('OK'.encode('utf-8'))
        print(cmd_rev_msg)
        if cmd_rev_msg[0] == "CMD_RESULT_SIZE":
            cmd_msg_len = int(cmd_rev_msg[1])
            if cmd_msg_len == 0:
                continue
            while True:
                response = client.recv(512).decode('utf-8')
                msg_len = len(response)
                output += response
                cmd_msg_len -= msg_len
                print(cmd_msg_len)
                if cmd_msg_len == 0:
                    print(output)
                    print('----------recv_done----------')
                    break
client.close()

