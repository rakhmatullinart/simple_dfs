# Simple dfs 
Simple dfs implementation using Python and sockets

### Deployment local
- execute command in separate terminal windows to see the logs
##### Start namenode and datanodes
Better use **virtual environment**
1. `cd namenode && python namenode.py`
2. `cd datanode1 && python datanode.py`
3. `cd datanode2 && python datanode.py`
4. `cd datanode3 && python datanode.py`
##### Start client application
1. `python client/client.py`


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
5. Copy deploy/docker-compose.yml to master node
6. `docker stack deploy -c docker-compose.yml dfs`
7. View docker stack online (need to set up visualizer) on http://18.221.112.199:8080 (just our case)

### Architecture

![Alt Class+modules diagram](src/full_cd.png?raw=true "Title")


### Communication protocols
 
![](src/cp_INIT.png?raw=true "Title")
![](src/cp_PUT.png?raw=true "Title")
![](src/cp_GET.png?raw=true "Title")
![](src/cp_LS.png?raw=true "Title")
![](src/cp_CD.png?raw=true "Title")
![](src/cp_COPY.png?raw=true "Title")
![](src/cp_MOVE.png?raw=true "Title")
![](src/cp_INFO.png?raw=true "Title")
![](src/cp_MKDIR.png?raw=true "Title")
![](src/cp_RMDIR.png?raw=true "Title")
![](src/cp_REMOVE.png?raw=true "Title")
![](src/cp_CREATE.png?raw=true "Title")

