import socket
import helpers as tools
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
        msg = 'WRITE {}'.format(remote_path)
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(100).decode()
        print('Status: {}'.format(data))
        if data.split(' ')[0] == 'ERROR':
            print(data.split(' ')[1])
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
            print(data.split(' ')[1])
            return
        datanode, addr = self.sock.accept()
        tools.recv_file(datanode, local_path)

    def remove(self, remote_path):
        msg = 'REMOVE {}'.format(remote_path)
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(100).decode()
        print('Status: {}'.format(data))
        if data.split(' ')[0] == 'ERROR':
            print(data.split(' ')[1])
            return

    def fileinfo(self, remote_path):
        msg = 'FILEINFO {}'.format(remote_path)
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(100).decode()
        print('Status: {}'.format(data))
        if data.split(' ')[0] == 'ERROR':
            print(data.split(' ')[1])
            return

    def copy(self, init_path, dest_path):
        msg = 'COPY {} {}'.format(init_path, dest_path)
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(100).decode()
        print('Status: {}'.format(data))
        if data.split(' ')[0] == 'ERROR':
            print(data.split(' ')[1])
            return

    def move(self, init_path, dest_path):
        msg = 'MOVE {} {}'.format(init_path, dest_path)
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(100).decode()
        print('Status: {}'.format(data))
        if data.split(' ')[0] == 'ERROR':
            print(data.split(' ')[1])
            return

    def read_dir(self, init_path):
        msg = 'READDIR {}'.format(init_path)
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(1024).decode()
        print('Status: {}'.format(data))
        if data.split(' ')[0] == 'ERROR':
            print(data.split(' ')[1])
            return

    def open_dir(self, init_path):
        msg = 'OPENDIR {}'.format(init_path)
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(1024).decode()
        print('Status: {}'.format(data))
        if data.split(' ')[0] == 'ERROR':
            print(data.split(' ')[1])
            return

    def make_dir(self, init_path):
        msg = 'MAKEDIR {}'.format(init_path)
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(1024).decode()
        print('Status: {}'.format(data))
        if data.split(' ')[0] == 'ERROR':
            print(data.split(' ')[1])
            return

    def remove_dir(self, init_path):
        msg = 'REMOVEDIR {}'.format(init_path)
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(1024).decode()
        print('Status: {}'.format(data))
        if data.split(' ')[0] == 'ERROR':
            print(data.split(' ')[1])
            return

c = Client()

c.connect_to_server()
option = input()
while option != '-1':
    if option =='1':
        c.upload('helpers.py', 'VIRTUALFILE')
    if option =='2':
        c.download('VIRTUALFILE', "DOWNLOADEDSUKA")
    if option =='3':
        c.remove('VIRTUALFILE')
    if option =='4':
        c.fileinfo('VIRTUALFILE')
    if option =='5':
        c.copy('VIRTUALFILE', 'newFILE')
    if option =='6':
        c.move('VIRTUALFILE', 'newFILE')
    if option =='7':
        c.read_dir('DIR')
    if option =='8':
        c.remove_dir('DIR')
    if option =='9':
        c.make_dir('DIR')
    if option =='10':
        c.open_dir('DIR')
    option = input()
