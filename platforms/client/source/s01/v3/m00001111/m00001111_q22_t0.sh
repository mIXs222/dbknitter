#!/bin/bash

#update packages
apt-get -y update

#set default MySQL password
DEBIAN_FRONTEND=noninteractive apt-get install -y mysql-server

#install Python and required packages
apt-get install -y python3 python3-pip
pip3 install mysql-connector-python pymongo
