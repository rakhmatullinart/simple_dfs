import os
import sys
import socket
import namenode_fs as fs


class NameNode:
    def __init__(self):
        import socket

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("", 8800))
        self.sock.listen()
        self.client = None
        self.clients = []
        self.sockets = None
        # self.datanodes = {
        #     'datanode1': (os.environ.get("DATANODE1_HOST", 'localhost'), 8801),
        #     'datanode2': (os.environ.get("DATANODE2_HOST", 'localhost'), 8802),
        #     'datanode3': (os.environ.get("DATANODE3_HOST", 'localhost'), 8803),
        #                   }
        self.datanodes = self.read_from_config()

    @staticmethod
    def read_from_config():
        config_file = "./config.txt"
        datanodes = {}
        with open(config_file, "r") as f:
            lines = f.readlines()
            print(lines)
            for line in lines:
                stripped_line = line.strip()
                if stripped_line.startswith("#") or not stripped_line:
                    continue

                tokens = stripped_line.split()
                if len(tokens) != 3:
                    raise ValueError(f"config file error: line {stripped_line}")
                datanode_name, datanode_host, namenode_port = tokens
                datanodes[datanode_name] = (
                    os.environ.get(datanode_host, "localhost"),
                    int(namenode_port),
                )
        print(datanodes)
        return datanodes

    def start_server(self):
        print("Waiting datanodes to connect.")
        self.sockets = {}
        for name, (ip, port) in self.datanodes.items():
            self.sockets[name] = self.get_connection(port)

        while True:
            print("Waiting client to connect.")
            self.client, addr = self.sock.accept()
            print(str(addr) + " connected")
            while True:
                data = self.client.recv(1500)
                if data:
                    print("recv: ", data)
                    # workaround for local and remote testing
                    if self.datanodes[list(self.datanodes)[0]][0] == 'localhost':
                        client_ip = 'localhost'
                    else:
                        client_ip = 'client'
                    sys.stdout.flush()
                    self.handle(data, client_ip)
                    sys.stdout.flush()
                else:
                    self.client.close()
                    print("Client disconnected")
                    break

    def get_connection(self, dn_port):
        # returns socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", dn_port))
        sock.listen()
        sock, addr = sock.accept()
        print(str(addr) + " connected")
        return sock

    def to_dn(self, i, msg):
        if i == "all":
            for name, sock in self.sockets.items():
                sock.send(str.encode(msg))
        else:
            self.sockets["datanode" + str(i)].send(str.encode(msg))

    def get_down_nodes(self):
        result = []
        for name, sock in self.sockets.items():
            try:
                sock.send(str.encode("PING"))
                data = sock.recv(5)
                if not data:
                    result.append(name)
            except socket.error:
                result.append(name)
        return result

    def recover_from_fault(self):
        fallen_nodes = self.get_down_nodes()
        print(fallen_nodes, self.datanodes.keys())
        if fallen_nodes and (len(self.datanodes) - len(fallen_nodes)) > 1:
            for path, dn_from, dn_to in fs.GetFilesToReplicate(
                fallen_nodes, self.datanodes.keys()
            ):
                print("FROM FS: ", path, dn_from, dn_to)
                self.to_dn(
                    dn_from[-1],
                    "WRITE_EXISTING_REPL {} _ {} {}".format(
                        path,
                        self.datanodes[dn_to][0],
                        "1" + str(self.datanodes[dn_to][1]),
                    ),
                )
                self.to_dn(
                    dn_to[-1],
                    "WRITE_REPL {} {} _ {}".format(
                        path,
                        str(self.datanodes[dn_from][0]),
                        "1" + str(self.datanodes[dn_from][1])
                    ),
                )
        else:
            fs.GetFilesToReplicate(fallen_nodes, self.datanodes.keys())
        for key in fallen_nodes:
            self.datanodes.pop(
                key, None
            )  # rm fallen nodes from their respective placeholders
        for key in fallen_nodes:
            self.sockets.pop(key, None)
        print("new dns: ", self.datanodes)

    def handle(self, query, client_ip):
        query = query.decode().split(" ")
        op = query[0]
        input_path = query[1] if len(query) > 1 else None
        output_path = query[2] if len(query) > 2 else None
        print("op: ", op)
        print("path1: ", input_path)
        print("path2: ", output_path)

        if op == "INIT":
            fs.Initialize()
            self.client.send(b"1GB")
            self.to_dn("all", "REMOVE {}".format("/"))
        else:
            self.recover_from_fault()
        if op == "CREATE":
            self.recover_from_fault()
            ret = fs.GetFile(input_path)
            if ret == None:
                print("CREATE {}".format(input_path))
                datanodes_to_put = list(self.datanodes.keys())[
                    : max(len(self.datanodes), 2)
                ]
                print("NODES TO PUT: ", datanodes_to_put)
                fs.FileCreate(input_path, nodes=datanodes_to_put)
                self.to_dn("all", "CREATE {}".format(input_path))
            elif -1:
                self.client.send(b"ERROR THE DIRECTORY DOES NOT EXIST")
                return
            self.client.send(b"SUCCESS")
        if op == "WRITE":
            # self.recover_from_fault()
            print("WRITE")
            # check if possible to store file
            size = int(output_path)
            datanodes_to_put = list(self.datanodes.keys())[
                : len(self.datanodes) if len(self.datanodes) <= 2 else 2
            ]
            print("NODES TO PUT: ", datanodes_to_put)
            result = fs.FileWrite(input_path, nodes=datanodes_to_put, filesize=size)
            if result == None:
                self.client.send(b"ERROR")
                return
            else:
                self.client.send(b"SUCCESS")
                if len(datanodes_to_put) > 1:
                    self.to_dn(
                        int(datanodes_to_put[1][-1]),
                        "WRITE {} {} {} {}".format(
                            result,
                            client_ip,
                            self.datanodes[datanodes_to_put[0]][0],
                            "1" + str(self.datanodes[datanodes_to_put[0]][1]),
                        ),
                    )
                    self.to_dn(
                        int(datanodes_to_put[0][-1]),
                        "WRITE_REPL {} {} _ {}".format(
                            result,
                            str(self.datanodes[datanodes_to_put[1]][0]),
                            "1" + str(self.datanodes[datanodes_to_put[1]][1]),
                        ),
                    )
                else:
                    self.to_dn(
                        int(datanodes_to_put[0][-1]),
                        "WRITE_ALONE {} {}".format(result, client_ip),
                    )

        if op == "READ":
            # self.recover_from_fault()
            print("READ")
            self.client.send(b"SUCCESS")
            # define datanodes to store
            path, dn = fs.FileRead(input_path)
            self.to_dn(dn[-1], "READ {} {}".format(path, client_ip))
        if op == "REMOVE":
            print("REMOVE")
            self.client.send(b"SUCCESS")
            path = fs.FileDelete(input_path)
            self.to_dn("all", "REMOVE {}".format(path))
        if op == "FILEINFO":
            print("send INFO")
            msg = "SUCCESS {}".format(fs.FileInfo(input_path))
            self.client.send(msg.encode())
        if op == "READDIR":
            msg = "SUCCESS\n {}".format(fs.DirRead(input_path))
            self.client.send(msg.encode())
            # define datanodes
        if op == "OPENDIR":
            fs.DirOpen(input_path)
            self.client.send(b"SUCCESS")
        if op == "REMOVEDIR":
            print("REMOVEDIR")
            if fs.IsEmpty(input_path):
                self.to_dn("all", "REMOVEDIR {}".format(fs.DirDelete(input_path)))
                self.client.send(b"SUCCESS")
            else:
                self.client.send(b"ACCEPT")
                answer = self.client.recv(100).decode()
                if answer == "YES":
                    self.to_dn("all", "REMOVEDIR {}".format(fs.DirDelete(input_path)))
                    self.client.send(b"SUCCESS")
                else:
                    self.client.send(b"SUCCESS")
        if op == "MAKEDIR":
            print("MAKEDIR")
            self.client.send(b"SUCCESS")
            self.to_dn("all", "MAKEDIR {}".format(fs.DirCreate(input_path)))
        if op == "COPY":
            print("COPY")
            input_path, output_path = fs.FileCopy(input_path, output_path)
            self.client.send(b"SUCCESS")
            self.to_dn("all", "COPY {} {}".format(input_path, output_path))
        if op == "MOVE":
            print("MOVE")
            input_path, output_path = fs.FileMove(input_path, output_path)
            self.client.send(b"SUCCESS")
            self.to_dn("all", "MOVE {} {}".format(input_path, output_path))


if __name__ == "__main__":
    n = NameNode()
    
    orig_stdout = sys.stdout
    f = open('STDOUT.txt', 'w+')
    sys.stdout = f
     
    n.start_server()

    sys.stdout = orig_stdout
    f.close()
