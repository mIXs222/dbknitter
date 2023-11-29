# setup.sh

#!/bin/bash

# Install Python and pip if not already installed
sudo apt update
sudo apt install -y python3 python3-pip

# Install required Python packages
pip3 install pymongo pandas

# Installing the direct_redis package (not available by default in pip)
# You may need to add the package to your PYTHONPATH manually or install from source
git clone https://github.com/mymarilyn/direct_redis
cd direct_redis
python3 setup.py install
cd ..
rm -rf direct_redis

# Note: If this is being provided as a script rather than a doc/explanation,
# it's essential to add execute permissions to the script using chmod +x setup.sh
