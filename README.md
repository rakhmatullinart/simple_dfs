# Simple dfs 
Simple dfs implementation using Python and sockets

### Deployment local
#### Option 1
- execute command in separate terminal windows to see the logs
##### Start namenode and datanodes
Better use **virtual environment**
1. `python namenode/namenode.py`
2. `python datanode1/datanode1.py`
3. `python datanode2/datanode2.py`
4. `python datanode3/datanode3.py`
##### Start client application
1. `python client/client.py`

#### Option 2
- execute dfs in background (logs hidden)
1. give permissions to sh script with `chmod +x run_dfs.sh`
1. `./run_dfs.sh`
2. `python client/client.py`

### Deployment with docker swarm

1. `docker swarm init on master node (any node)`
2. `docker swarm join --token <token>`
3. On master node
    `docker node ls`

    ![Alt docker node ls](src/node_ls.png?raw=true "Title")
4. In docker-compose.yml write node ID for every service to resolve placement of service
    ```placement:
        constraints:
          - node.id == sah2w4n4do4alvy1roguglt42  # change to your node id```
5. Copy docker-compose.yml to master node
6. `docker stack deploy -c docker-compose.yml dfs`