import socket
from time import sleep

import helpers as tools
import os


class Client:
    def __init__(self):
        """
        nn_ip: ip of Namenode
        """
        self.namenode = None
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("", 7777))
        self.sock.listen()

    def connect_to_server(self,):
        self.namenode = socket.socket()
        while True:
            try:
                self.namenode.connect(
                    (
                        os.environ.get("SERVER_IP", "localhost"),
                        int(os.environ["SERVER_PORT"]),
                    )
                )
                break
            except ConnectionRefusedError:
                sleep(3)

    def init_cluster(self):
        msg = b"INIT"
        self.namenode.send(msg)
        data = self.namenode.recv(100)
        print("FS size: ", data)

    def touch(self, filepath):
        msg = "CREATE {}".format(filepath)
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(100).decode()
        print("Status: {}".format(data))

    def upload(self, local_path, remote_path):
        msg = "WRITE {} {}".format(remote_path, os.path.getsize(local_path))
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(100).decode()
        print("Status: {}".format(data))
        if data.split(" ")[0] == "ERROR":
            if len(data.split(" ")) > 1:
                print(data.split(" ")[1])
            return
        datanode, addr = self.sock.accept()
        tools.send_file(datanode, local_path)
        datanode, _ = self.sock.accept()
        result = datanode.recv(100)
        print(result)

    def download(self, remote_path, local_path):
        msg = "READ {}".format(remote_path)
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(100).decode()
        print("Status: {}".format(data))
        if data.split(" ")[0] == "ERROR":
            if len(data.split(" ")) > 1:
                print(data.split(" ")[1])
            return
        datanode, addr = self.sock.accept()
        tools.recv_file(datanode, local_path)

    def remove(self, remote_path):
        msg = "REMOVE {}".format(remote_path)
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(100).decode()
        print("Status: {}".format(data))
        if data.split(" ")[0] == "ERROR":
            if len(data.split(" ")) > 1:
                print(data.split(" ")[1])
            return

    def fileinfo(self, remote_path):
        msg = "FILEINFO {}".format(remote_path)
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(100).decode()
        print("Status: {}".format(data))
        if data.split(" ")[0] == "ERROR":
            if len(data.split(" ")) > 1:
                print(data.split(" ")[1])
            return

    def copy(self, init_path, dest_path):
        msg = "COPY {} {}".format(init_path, dest_path)
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(100).decode()
        print("Status: {}".format(data))
        if data.split(" ")[0] == "ERROR":
            if len(data.split(" ")) > 1:
                print(data.split(" ")[1])
            return

    def move(self, init_path, dest_path):
        msg = "MOVE {} {}".format(init_path, dest_path)
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(100).decode()
        print("Status: {}".format(data))
        if data.split(" ")[0] == "ERROR":
            if len(data.split(" ")) > 1:
                print(data.split(" ")[1])
            return

    def read_dir(self, init_path):
        msg = "READDIR {}".format(init_path)
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(8192).decode()
        print("Status: {}".format(data))
        if data.split(" ")[0] == "ERROR":
            if len(data.split(" ")) > 1:
                print(data.split(" ")[1])
            return

    def open_dir(self, init_path):
        msg = "OPENDIR {}".format(init_path)
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(1024).decode()
        print("Status: {}".format(data))
        if data.split(" ")[0] == "ERROR":
            if len(data.split(" ")) > 1:
                print(data.split(" ")[1])
            return

    def make_dir(self, init_path):
        msg = "MAKEDIR {}".format(init_path)
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(1024).decode()
        print("Status: {}".format(data))
        if data.split(" ")[0] == "ERROR":
            if len(data.split(" ")) > 1:
                print(data.split(" ")[1])
            return

    def remove_dir(self, init_path):
        msg = "REMOVEDIR {}".format(init_path)
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(1024).decode()
        print("Status: {}".format(data))
        if data.split(" ")[0] == "ACCEPT":
            print("Are you sure you want delete non-empty directory? YES/NO")
            answer = input()
            self.namenode.send(str.encode(answer))
            data = self.namenode.recv(1024).decode()
            print("Status: {}".format(data))
            return
        if data.split(" ")[0] == "ERROR":
            if len(data.split(" ")) > 1:
                print(data.split(" ")[1])
            return

    @staticmethod
    def run_cli():
        tokens = input().split(" ")
        length = len(tokens)
        try:
            while tokens[0] != "exit":
                option = tokens[0]
                if option == "write":
                    if length < 3:
                        print("Usage: write /local_path /DFS_path")
                    else:
                        # 1 arg is local file, 2 arg is DFS path
                        c.upload(tokens[1], tokens[2])
                if option == "read":
                    if length < 3:
                        print("Usage: read /DFS_path /local_path")
                    else:
                        # 1 arg is DFS file path, 2 arg is local destination path
                        c.download(tokens[1], tokens[2])
                if option == "remove":
                    if length < 2:
                        print("Usage: remove /DFS_path")
                    else:
                        # Arg is DFS file path
                        c.remove(tokens[1])
                if option == "info":
                    if length < 2:
                        print("Usage: info /DFS_path")
                    else:
                        # Arg is DFS file path
                        c.fileinfo(tokens[1])
                if option == "copy":
                    if length < 3:
                        print("Usage: copy /DFS_path /DFS_dest_path")
                    else:
                        # 1 arg is DFS file path to copy, 2 arg is DFS destination path
                        c.copy(tokens[1], tokens[2])
                if option == "move":
                    if length < 3:
                        print("Usage: move /DFS_path /DFS_dest_path")
                    else:
                        # 1 arg is DFS file path, 2 arg is DFS destination path
                        c.move(tokens[1], tokens[2])
                if option == "dirread":
                    if length < 2:
                        print("Usage: dirread /DFS_path")
                    else:
                        # Arg is DFS dir path
                        c.read_dir(tokens[1])
                if option == "dirremove":
                    if length < 2:
                        print("Usage: dirremove /DFS_path")
                    else:
                        # Arg is DFS dir path
                        c.remove_dir(tokens[1])
                if option == "dirmake":
                    if length < 2:
                        print("Usage: dirmake /DFS_path")
                    else:
                        # Arg is DFS dir path
                        c.make_dir(tokens[1])
                if option == "diropen":
                    if length < 2:
                        print("Usage: diropen /DFS_path")
                    else:
                        # Arg is DFS dir path
                        c.open_dir(tokens[1])
                if option == "init":
                    # NO ARG
                    c.init_cluster()
                if option == "create":
                    if length < 2:
                        print("Usage: create /DFS_path")
                    else:
                        # Arg is DFS file path
                        c.touch(tokens[1])
                tokens = input().split(" ")
                length = len(tokens)
        except Exception as e:
            print(str(e))


if __name__ == "__main__":
    c = Client()

    c.connect_to_server()
    c.init_cluster()
    c.run_cli()
