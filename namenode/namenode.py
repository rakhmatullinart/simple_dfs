import socket
from fs import namenode_fs as fs


class NameNode:
    
    def __init__(self):


        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(('', 8800))
        self.sock.listen()
        self.client = None
        self.clients = []
        self.sockets = None
        self.datanodes = {'datanode1':('localhost', 8801), #my port I'' rceive connectoins on
                          'datanode2':('localhost', 8802)}
        
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
        if i=='all':
            for name, sock in self.sockets.items():
                sock.send(str.encode(msg))
        else:
            self.sockets['datanode'+str(i)].send(str.encode(msg))
    
    def handle(self, query, client_ip):
        query = query.decode().split(' ')
        op = query[0]
        input_path = query[1] if len(query) > 1 else None
        output_path = query[2] if len(query) > 2 else None
        print('op: ', op)
        print('path1: ', input_path)
        print('path2: ', output_path)
        if op == 'INIT':
            # rm -r /*
            # get fs size
            size = fs.Initialize()
            self.client.send(b'100GB')
            self.to_dn('all', 'REMOVE {}'.format('/*'))
        if op == 'CREATE':
            # touch 'filename'
            if fs.FileCreate(input_path) != None:
                print('CREATE {}'.format(input_path))
                self.client.send(b'SUCCESS')
            else:
                self.client.send(b'ERROR CANT CREATE FILE')
            self.to_dn('all', 'CREATE {}'.format(input_path))
        if op == 'WRITE':
            print('WRITE')
            self.client.send(b'SUCCESS')
            # define datanodes to store
            self.to_dn(1, 'WRITE {} {} {}'.format(input_path, client_ip, 
                                                    self.datanodes['datanode2'][0]))
            self.to_dn(2, 'WRITE_REPL {}'.format(input_path))
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
            #define datanodes
            self.to_dn(1, 'MOVE {} {}'.format(input_path, output_path))
            self.to_dn(2, 'MOVE {} {}'.format(input_path, output_path))
        if op == 'READDIR':
            self.client.send(b'SUCCESS fileList')
            #define datanodes
        if op == 'READDIR':
            self.client.send(b'SUCCESS fileList')
            #define datanodes
        if op == 'OPENDIR':
            self.client.send(b'SUCCESS')
            #define datanodes
        if op == 'REMOVEDIR':
            print('REMOVEDIR')
            self.client.send(b'SUCCESS')
            self.to_dn('all', 'REMOVEDIR {}'.format(input_path))
        if op == 'MAKEDIR':
            print('MAKEDIR')
            self.client.send(b'SUCCESS')
            self.to_dn('all', 'MAKEDIR {}'.format(input_path))
            

            

n = NameNode()

n.start_server()