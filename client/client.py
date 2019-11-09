import socket
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
        self.sock.bind(('', 7777))
        self.sock.listen()
    
    def connect_to_server(self, ip='localhost', port=8800):
        self.namenode = socket.socket()
        self.namenode.connect((ip,port))
        
        
        
    def init_cluster(self):
        msg = b'INIT'
        self.namenode.send(msg)
        data = self.namenode.recv(100)
        print('FS size: ', data)
        
    def touch(self, filepath):
        msg = 'CREATE {}'.format(filepath)
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(100).decode()
        print('Status: {}'.format(data))
                    
    def upload(self, local_path, remote_path):
        msg = 'WRITE {} {}'.format(remote_path, os.path.getsize(local_path))
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(100).decode()
        print('Status: {}'.format(data))
        if data.split(' ')[0] == 'ERROR':
            if len(data.split(' ')) > 1: print(data.split(' ')[1])
            return
        datanode, addr = self.sock.accept()
        tools.send_file(datanode, local_path)
        datanode, _ = self.sock.accept()
        result = datanode.recv(100)
        print(result)

    def download(self, remote_path, local_path):
        msg = 'READ {}'.format(remote_path)
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(100).decode()
        print('Status: {}'.format(data))
        if data.split(' ')[0] == 'ERROR':
            if len(data.split(' ')) > 1: print(data.split(' ')[1])
            return
        datanode, addr = self.sock.accept()
        tools.recv_file(datanode, local_path)

    def remove(self, remote_path):
        msg = 'REMOVE {}'.format(remote_path)
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(100).decode()
        print('Status: {}'.format(data))
        if data.split(' ')[0] == 'ERROR':
            if len(data.split(' ')) > 1: print(data.split(' ')[1])
            return

    def fileinfo(self, remote_path):
        msg = 'FILEINFO {}'.format(remote_path)
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(100).decode()
        print('Status: {}'.format(data))
        if data.split(' ')[0] == 'ERROR':
            if len(data.split(' ')) > 1: print(data.split(' ')[1])
            return

    def copy(self, init_path, dest_path):
        msg = 'COPY {} {}'.format(init_path, dest_path)
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(100).decode()
        print('Status: {}'.format(data))
        if data.split(' ')[0] == 'ERROR':
            if len(data.split(' ')) > 1: print(data.split(' ')[1])
            return

    def move(self, init_path, dest_path):
        msg = 'MOVE {} {}'.format(init_path, dest_path)
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(100).decode()
        print('Status: {}'.format(data))
        if data.split(' ')[0] == 'ERROR':
            if len(data.split(' ')) > 1: print(data.split(' ')[1])
            return

    def read_dir(self, init_path):
        msg = 'READDIR {}'.format(init_path)
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(8192).decode()
        print('Status: {}'.format(data))
        if data.split(' ')[0] == 'ERROR':
            if len(data.split(' ')) > 1: print(data.split(' ')[1])
            return

    def open_dir(self, init_path):
        msg = 'OPENDIR {}'.format(init_path)
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(1024).decode()
        print('Status: {}'.format(data))
        if data.split(' ')[0] == 'ERROR':
            if len(data.split(' ')) > 1: print(data.split(' ')[1])
            return

    def make_dir(self, init_path):
        msg = 'MAKEDIR {}'.format(init_path)
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(1024).decode()
        print('Status: {}'.format(data))
        if data.split(' ')[0] == 'ERROR':
            if len(data.split(' ')) > 1: print(data.split(' ')[1])
            return

    def remove_dir(self, init_path):
        msg = 'REMOVEDIR {}'.format(init_path)
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(1024).decode()
        print('Status: {}'.format(data))
        if data.split(' ')[0] == 'ERROR':
            if len(data.split(' ')) > 1: print(data.split(' ')[1])
            return


if __name__ == '__main__':
    c = Client()

    c.connect_to_server()
    c.init_cluster()

    option = input()
    tokens = option.split(' ')
    while tokens[0] != 'exit':
        option = tokens[0]
        if option =='write':
            # 1 arg is local file, 2 arg is DFS path
            c.upload(tokens[1], tokens[2])
        if option =='read':
            # 1 arg is DFS file path, 2 arg is local destination path
            c.download(tokens[1], tokens[2])
        if option =='remove':
            # Arg is DFS file path
            c.remove(tokens[1])
        if option =='info':
            # Arg is DFS file path
            c.fileinfo(tokens[1])
        if option =='copy':
            # 1 arg is DFS file path to copy, 2 arg is DFS destination path
            c.copy(tokens[1], tokens[2])
        if option =='move':
            # 1 arg is DFS file path, 2 arg is DFS destination path
            c.move(tokens[1], tokens[2])
        if option =='dirread':
            # Arg is DFS dir path
            c.read_dir(tokens[1])
        if option =='dirremove':
            # Arg is DFS dir path
            c.remove_dir(tokens[1])
        if option =='dirmake':
            # Arg is DFS dir path
            c.make_dir(tokens[1])
        if option =='diropen':
            # Arg is DFS dir path
            c.open_dir(tokens[1])
        if option =='init':
            # NO ARG
            c.init_cluster()
        if option =='create':
            # Arg is DFS file path
            c.touch(tokens[1])
        option = input()
        tokens = option.split(' ')
