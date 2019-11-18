import os
import sys
sys.path.insert(0, "..")
from datanode_base.datanode import Datanode

if __name__ == "__main__":
    os.environ["PORT"] = "18801"
    os.environ["SERVER_PORT"] = "8801"
    d = Datanode()
    d.connect_to_server()
