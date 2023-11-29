# setup.sh
#!/bin/bash
# Updates package lists, installs python3, python3-pip and pymysql
sudo apt-get update
sudo apt-get install -y python3 python3-pip
pip3 install pymysql
