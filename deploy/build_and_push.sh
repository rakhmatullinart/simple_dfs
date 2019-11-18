docker build -t rakhmatullinart/namenode:$TAG -f namenode/Dockerfile ./namenode
docker push rakhmatullinart/namenode:$TAG


docker build -t rakhmatullinart/datanode:$TAG -f datanode/Dockerfile ./datanode
docker push rakhmatullinart/datanode:$TAG


docker build -t rakhmatullinart/client:$TAG -f client/Dockerfile ./client
docker push rakhmatullinart/client:$TAG
