#!/bin/bash
mkdir -p platforms/client/source platforms/client/output platforms/client/expected && \
    docker build -t client-image -f cloudlab/client/Dockerfile . && \
    docker build -t mysql-image -f cloudlab/mysql/Dockerfile . && \
    docker build -t mongodb-image -f cloudlab/mongodb/Dockerfile . && \
    docker compose -f cloudlab/docker-compose.yml up
