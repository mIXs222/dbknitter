# install_dependencies.sh
#!/bin/bash

# Update the repositories
apt-get update

# Install python3 and pip if they are not already installed
apt-get install -y python3 python3-pip

# Install the required Python libraries
pip3 install pymysql pandas redis direct-redis
