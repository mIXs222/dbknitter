# install.sh
#!/bin/bash

# Update package list (optional but recommended)
apt-get update

# Install Python and pip, if not already installed
apt-get install -y python3 python3-pip

# Install pandas
pip3 install pandas

# Install direct_redis or any other additional necessary packages
pip3 install direct-redis

# Run the Python script
python3 query.py
