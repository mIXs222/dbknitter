# install_dependencies.sh
#!/bin/bash

# Update package list and install pip and Python development files
sudo apt-get update
sudo apt-get install -y python3-pip python3-dev

# Install pymongo for MongoDB connection
pip3 install pymongo

# Install pandas for data manipulation
pip3 install pandas

# Install direct_redis for Redis connection
pip3 install direct_redis

# Note that the above commands assume a Debian/Ubuntu-based system,
# and correct privileges (or use of `sudo`) for package installation.
