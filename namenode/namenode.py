import socket
import sys

sys.path.insert(0, '..')
from fs import namenode_fs as fs


class NameNode:

    def __init__(self):
        import socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(('', 8800))
        self.sock.listen()
        self.client = None
        self.clients = []
        self.sockets = None
        self.datanodes = {'datanode1': ('localhost', 8801),  # my port I'' rceive connectoins on
                          'datanode2': ('localhost', 8802),
                          'datanode3': ('localhost', 8803),
                          }

    def start_server(self):
        print('Waiting datanodes to connect.')
        self.sockets = {}
        for name, (ip, port) in self.datanodes.items():
            self.sockets[name] = self.get_connection(port)

        print('Waiting client to connect.')
        self.client, addr = self.sock.accept()
        print(str(addr) + ' connected')
        while True:
            data = self.client.recv(1500)
            if data:
                print('recv: ', data)
                self.handle(data, addr[0])
            else:
                self.client.close()
                print('Client disconnected')
                break

    def get_connection(self, dn_port):
        # returns socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('', dn_port))
        sock.listen()
        sock, addr = sock.accept()
        print(str(addr) + ' connected')
        return sock

    def to_dn(self, i, msg):
        if i == 'all':
            for name, sock in self.sockets.items():
                sock.send(str.encode(msg))
        else:
            self.sockets['datanode' + str(i)].send(str.encode(msg))

    def get_down_nodes(self):
        result = []
        for name, sock in self.sockets.items():
            sock.send(str.encode('PING'))
            data = sock.recv(5)
            if not data: result.append(name)
        return result

    def handle(self, query, client_ip):
        query = query.decode().split(' ')
        op = query[0]
        input_path = query[1] if len(query) > 1 else None
        output_path = query[2] if len(query) > 2 else None
        print('op: ', op)
        print('path1: ', input_path)
        print('path2: ', output_path)
        if op == 'INIT':
            fs.Initialize()
            self.client.send(b'1GB')
            self.to_dn('all', 'REMOVE {}'.format('/'))
        if op == 'CREATE':
            ret = fs.GetFile(input_path)
            if ret == None:
                print('CREATE {}'.format(input_path))
                fs.FileCreate(input_path)
                self.to_dn('all', 'CREATE {}'.format(input_path))
            elif -1:
                self.client.send(b'ERROR THE DIRECTORY DOES NOT EXIST')
                return
            self.client.send(b'SUCCESS')
        if op == 'WRITE':
            fallen_nodes = self.get_down_nodes()
            print(fallen_nodes, self.datanodes.keys())
            if fallen_nodes and (len(self.datanodes) - len(fallen_nodes)) > 1:
                for path, dn_from, dn_to in fs.GetFilesToReplicate(fallen_nodes, self.datanodes.keys()):
                    print('FROM FS: ', path, dn_from, dn_to)
                    self.to_dn(dn_from, 'WRITE_EXISTING_REPL {} _ {} {}'.format(path,
                                                                                self.datanodes[dn_to][0],
                                                                                '1' + str(self.datanodes[dn_to][1])))
                    self.to_dn(dn_to, 'WRITE_REPL {} _ _ {}'.format(path, '1' + str(self.datanodes[dn_from][1])))
            for key in fallen_nodes: self.datanodes.pop(key, None) # rm fallen nodes from their respective placeholders
            for key in fallen_nodes: self.sockets.pop(key, None)
            print('new dns: ', self.datanodes)
            print('WRITE')
            # check if possible to store file
            size = int(output_path)
            datanodes_to_put = list(self.datanodes.keys())[:max(len(self.datanodes), 2)]
            result = fs.FileWrite(input_path, nodes=datanodes_to_put, filesize=size)
            if result == None:
                self.client.send(b'ERROR')
                return
            else:
                self.client.send(b'SUCCESS')
                if len(datanodes_to_put) > 1:
                    self.to_dn(int(datanodes_to_put[1][-1]), 'WRITE {} {} {} {}'.format(result, client_ip,
                                                                                        self.datanodes[
                                                                                            datanodes_to_put[0]][0],
                                                                                        '1' + str(self.datanodes[
                                                                                                      datanodes_to_put[
                                                                                                          0]][1])))
                    self.to_dn(int(datanodes_to_put[0][-1]),
                               'WRITE_REPL {} _ _ {}'.format(result, '1' + str(self.datanodes[datanodes_to_put[1]][1])))
                else:
                    self.to_dn(int(datanodes_to_put[0][-1]), 'WRITE_ALONE {} {}'.format(result, client_ip))

        if op == 'READ':
            print('READ')
            self.client.send(b'SUCCESS')
            # define datanodes to store
            self.to_dn(1, 'READ {} {}'.format(input_path, client_ip))
        if op == 'REMOVE':
            print('REMOVE')
            self.client.send(b'SUCCESS')
            self.to_dn('all', 'REMOVE {}'.format(input_path))
        if op == 'FILEINFO':
            print('send INFO')
            self.client.send(b'SUCCESS 100GB')
        if op == 'COPY':
            self.client.send(b'SUCCESS')
            self.to_dn(1, 'COPY {} {}'.format(input_path, output_path))
            self.to_dn(2, 'COPY {} {}'.format(input_path, output_path))
        if op == 'MOVE':
            self.client.send(b'SUCCESS')
            # define datanodes
            self.to_dn(1, 'MOVE {} {}'.format(input_path, output_path))
            self.to_dn(2, 'MOVE {} {}'.format(input_path, output_path))
        if op == 'READDIR':
            msg = 'SUCCESS\n {}'.format(fs.DirRead(input_path))
            self.client.send(msg.encode())
            # define datanodes
        if op == 'READDIR':
            self.client.send(b'SUCCESS fileList')
            # define datanodes
        if op == 'OPENDIR':
            self.client.send(b'SUCCESS')
            # define datanodes
        if op == 'REMOVEDIR':
            print('REMOVEDIR')
            self.client.send(b'SUCCESS')
            self.to_dn('all', 'REMOVEDIR {}'.format(input_path))
        if op == 'MAKEDIR':
            print('MAKEDIR')
            self.client.send(b'SUCCESS')
            self.to_dn('all', 'MAKEDIR {}'.format(input_path))


if __name__ == '__main__':
    n = NameNode()

    n.start_server()
