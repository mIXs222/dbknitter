# install_dependencies.sh
#!/bin/bash

# Update package list and install pip
sudo apt update
sudo apt install python3-pip -y

# Install Python libraries
pip3 install pandas pymysql direct-redis
