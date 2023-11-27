#!/bin/bash

echo "Updating package lists for upgrades and new package installations..."
sudo apt-get update

echo "Installing python3-pip..."
sudo apt-get install python3-pip -y

echo "Installing pymysql..."
pip3 install pymysql

echo "Installing pandas..."
pip3 install pandas

echo "Finished installing dependencies."
