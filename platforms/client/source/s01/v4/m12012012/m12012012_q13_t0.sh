# install_dependencies.sh
#!/bin/bash

# Update package lists
sudo apt-get update

# Install pip, Python's package manager
sudo apt-get install -y python3-pip

# Install required Python libraries
pip3 install pymysql pymongo
