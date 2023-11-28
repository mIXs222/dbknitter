# install_dependencies.sh

#!/bin/bash

# Update package lists just in case
apt-get update

# Install MySQL and MongoDB python client libraries
pip install pymysql pymongo
