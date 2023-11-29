# install_dependencies.sh

#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python (if not already installed)
sudo apt-get install -y python3

# Install pip (you might not need this if you already have pip installed)
sudo apt-get install -y python3-pip

# Install required Python libraries
pip3 install pymysql pandas

# Since 'direct_redis' isn't a standard package, you may need to acquire it from the proper
# source or repository. Assuming it exists in the Python Package Index, you may use pip to install.
# Otherwise, you must follow the correct installation procedure.
pip3 install direct_redis
