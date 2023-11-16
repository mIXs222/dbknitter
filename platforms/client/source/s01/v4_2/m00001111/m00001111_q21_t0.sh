#!/bin/bash
# update and upgrade
sudo apt-get update && sudo apt-get upgrade
# install python3
sudo apt-get install python3
# install pip3
sudo apt-get install python3-pip
# install mysql-connector and pymongo using pip3
sudo -H pip3 install mysql-connector-python pymongo
# make sure wget is up to date
sudo apt-get install wget 
# download the pandas module
pip3 install pandas
