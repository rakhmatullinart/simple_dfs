import socket, os
import helpers as tools

class Datanode:
    
    def __init__(self, my_port=18802):
        """
        nn_ip: ip of Namenode
        """
        self.namenode = socket.socket()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(('', my_port))
        self.sock.listen()
        self.client = socket.socket()
    
    def connect_to_server(self, ip='localhost', port=8800):
        self.namenode.connect((ip,port))
        while True:
            data = self.namenode.recv(1500)
            if data:
                print('recv: ', data)
                self.handle(data)
            else:
                print('Client disconnected')
                break
        
    def handle(self, query):
        query = query.decode().split(' ')
        op = query[0]
        input_path = query[1] if len(query) > 1 else None
        client_ip = query[2] if len(query) > 2 else None
        replica_ip = query[3] if len(query) > 3 else None
        print('op: ', op)
        print('path1: ', input_path)
        print('path2: ', client_ip)
        print('replicato: ', replica_ip)
        if op == 'WRITE':
            client = socket.socket()
            client.connect((client_ip, 7777))
            tools.recv_file(client, input_path)
            replica = socket.socket()
            replica.connect((replica_ip, 18802))
            tools.send_file(replica, input_path)
            replica, addr = self.sock.accept()
            result = replica.recv(10)
            print(result)
            client = socket.socket()
            client.connect((client_ip,7777))
            client.send(result)
        if op == 'READ':
            client = socket.socket()
            client.connect((client_ip, 7777))
            tools.send_file(client, input_path)
        if op == 'WRITE_REPL':
            source_dn, source_addr = self.sock.accept()
            tools.recv_file(source_dn, input_path+str('REPL'))
            source_dn = socket.socket()
            source_dn.connect((source_addr[0], 18801))
            source_dn.send(b'SUCCESS')
        if op == 'REMOVE':
            pass # rm
        if op == 'COPY':
            pass # cp input output
        if op == 'MOVE':
            pass # cp input output



if __name__ == '__main__':
    d = Datanode()

    d.connect_to_server(port=8802)