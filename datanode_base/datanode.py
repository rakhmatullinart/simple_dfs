import os
import socket
from time import sleep

from datanode_base import helpers as tools
from datanode_base import datanode_fs as fs

# 18801
# 18802
# 18803


class Datanode:
    def __init__(self,):
        """
        nn_ip: ip of Namenode
        """
        self.namenode = socket.socket()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("", int(os.environ["PORT"])))
        self.sock.listen()

    def connect_to_server(self,):
        while True:
            try:
                self.namenode.connect(
                    (
                        socket.gethostbyname(os.environ.get("SERVER_IP", "localhost")),
                        int(os.environ["SERVER_PORT"]),
                    )
                )
                break
            except:
                sleep(3)

        while True:
            data = self.namenode.recv(1500)
            if data:
                print("recv: ", data)
                self.handle(data)
            else:
                print("Client disconnected")
                break

    def handle(self, query):
        query = query.decode().split(" ")
        op = query[0]
        input_path = query[1] if len(query) > 1 else None
        output_path = query[2] if len(query) > 2 else None
        replica_ip = query[3] if len(query) > 3 else None
        replica_port = int(query[4]) if len(query) > 4 else None
        print("op: ", op)
        print("path1: ", input_path)
        print("path2: ", output_path)
        print("replicaIP: ", replica_ip)
        print("replicaPORT: ", replica_port)
        if op == "WRITE_REPL":
            print("Going to recv file {}".format(input_path))
            source_dn, source_addr = self.sock.accept()
            tools.recv_file(source_dn, fs.GetLocalPath(input_path))

            # send response to peer
            source_dn = socket.socket()
            source_dn.connect((output_path, int(replica_port)))
            source_dn.send(b"SUCCESS")
        if op == "WRITE":
            # download file from client
            client_ip = output_path
            client = socket.socket()
            client.connect((client_ip, 7777))
            tools.recv_file(client, fs.GetLocalPath(input_path))

            # send file to replica
            replica = socket.socket()
            replica.connect((replica_ip, replica_port))
            tools.send_file(replica, fs.GetLocalPath(input_path))

            # get response from replica
            replica, addr = self.sock.accept()
            result = replica.recv(10)
            print(result)

            # send response to client
            client = socket.socket()
            client.connect((client_ip, 7777))
            client.send(result)
        if op == "WRITE_ALONE":
            client_ip = output_path
            # download file from client
            client = socket.socket()
            client.connect((client_ip, 7777))
            tools.recv_file(client, fs.GetLocalPath(input_path))
            client = socket.socket()
            client.connect((client_ip, 7777))
            client.send(b"SUCCESS")
        if op == "WRITE_EXISTING_REPL":
            # send file to replica
            replica = socket.socket()
            replica.connect((replica_ip, replica_port))
            tools.send_file(replica, fs.GetLocalPath(input_path))

            # get response from replica
            replica, addr = self.sock.accept()
            result = replica.recv(10)
            print(result)
        if op == "READ":
            client_ip = output_path
            client = socket.socket()
            client.connect((client_ip, 7777))
            tools.send_file(client, fs.GetLocalPath(input_path))
        if op == "REMOVE":
            if input_path == "/":
                fs.Initialize()
            else:
                fs.FileDelete(input_path)
        if op == "COPY":
            fs.FileCopy(input_path, output_path)
        if op == "MOVE":
            fs.FileMove(input_path, output_path)
        if op == "CREATE":
            fs.FileCreate(input_path)
        if op == "PING":
            print("PING came")
            self.namenode.send(b"ALIVE")
        if op == "MAKEDIR":
            print("MAKEDIR")
            fs.DirectoryMake(input_path)
        if op == "REMOVEDIR":
            print("REMOVEDIR")
            fs.DirectoryDelete(input_path)


if __name__ == "__main__":
    d = Datanode()

    d.connect_to_server()
