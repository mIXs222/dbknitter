# Save this as install_dependencies.sh
#!/bin/bash

# Install Python and Pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymongo
pip3 install direct_redis
pip3 install pandas
