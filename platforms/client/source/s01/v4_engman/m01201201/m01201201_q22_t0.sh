# install_dependencies.sh
#!/bin/bash

# Update packages and install Python and pip if they are not available
sudo apt-get update -y
sudo apt-get install -y python3 python3-pip

# Install Python dependencies
pip3 install pymysql pandas direct-redis
