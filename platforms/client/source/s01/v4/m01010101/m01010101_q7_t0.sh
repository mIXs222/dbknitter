# install_dependencies.sh
#!/bin/bash

# Install Python and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install the required Python libraries
pip3 install pymysql pymongo pandas
