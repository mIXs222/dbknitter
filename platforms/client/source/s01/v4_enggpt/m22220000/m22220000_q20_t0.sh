# install_dependencies.sh
#!/bin/bash
set -e

# Ensure pip is available
apt-get update && apt-get install -y python3-pip

# Install required Python libraries
pip3 install pymysql direct_redis pandas
