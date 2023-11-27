# save as install_dependencies.sh
#!/bin/bash
sudo apt update
sudo apt install -y python3-pip
pip3 install pymysql pymongo
