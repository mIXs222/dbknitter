# install_dependencies.sh
#!/bin/bash

# Update package lists
sudo apt update

# Make sure pip, Python's package installer, is installed
sudo apt install -y python3-pip

# Install Python libraries required to run the script
pip3 install pymysql pandas direct-redis
