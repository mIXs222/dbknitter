# install_dependencies.sh
#!/bin/bash

# Update system and install Python3 and pip if they are not available
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymysql pandas redis direct_redis
