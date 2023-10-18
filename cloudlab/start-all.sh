#!/bin/bash
docker build -t client-image cloudlab/client && \
    docker build -t mysql-image cloudlab/mysql && \
    docker build -t mongodb-image cloudlab/mongodb && \
    docker compose -f cloudlab/docker-compose.yml up
