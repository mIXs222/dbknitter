# install_dependencies.sh

#!/bin/bash

# Update repository and install Python and pip
apt-get update
apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql
pip3 install pymongo
pip3 install pandas
pip3 install direct_redis
