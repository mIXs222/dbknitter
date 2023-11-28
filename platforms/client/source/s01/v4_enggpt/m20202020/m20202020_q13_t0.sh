# Bash script (install_dependencies.sh)
#!/bin/bash

# Update package manager and install Python and pip
apt-get update
apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pandas direct_redis
