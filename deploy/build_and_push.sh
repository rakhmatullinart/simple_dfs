docker build -t rakhmatullinart/namenode:$TAG -f deploy/namenode/Dockerfile .
docker push rakhmatullinart/namenode:$TAG


docker build -t rakhmatullinart/datanode:$TAG -f deploy/datanode/Dockerfile .
docker push rakhmatullinart/datanode:$TAG


docker build -t rakhmatullinart/client:$TAG -f deploy/client/Dockerfile .
docker push rakhmatullinart/client:$TAG
