import os
import sys
sys.path.insert(0, "..")
from datanode_base.datanode import Datanode

if __name__ == "__main__":

    orig_stdout = sys.stdout
    f = open('STDOUT.txt', 'w+')
    sys.stdout = f

    os.environ["PORT"] = "18802"
    os.environ["SERVER_PORT"] = "8802"
    d = Datanode()
    d.connect_to_server()

    sys.stdout = orig_stdout
    f.close()
