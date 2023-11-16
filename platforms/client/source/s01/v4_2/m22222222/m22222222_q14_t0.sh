#!/bin/sh

apt-get update
apt-get install -y python3 python3-pip redis-server
pip3 install pandas redis
