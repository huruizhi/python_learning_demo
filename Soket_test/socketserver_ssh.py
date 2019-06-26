import socketserver
import subprocess


class MySocketServer(socketserver.BaseRequestHandler):

    def handle(self):
        conn = self.request
        while True:
            cmd = conn.recv(1024).decode('utf-8')
            print(cmd)
            if not cmd:
                print("connection closed")
                break
            else:
                cmd_exec = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                output = cmd_exec.stdout.read()
                output_len = "CMD_RESULT_SIZE|%s" % len(output)
                print(output)
                conn.sendall(output_len.encode('utf-8'))
                conn.recv(10)
                conn.send(output)


if __name__ == "__main__":
    host = ("192.168.0.155", 22564)
    server = socketserver.ThreadingTCPServer(host, MySocketServer)
    print("server start!")
    server.serve_forever()
