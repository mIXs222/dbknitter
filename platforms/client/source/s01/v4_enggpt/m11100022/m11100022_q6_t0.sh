# install_dependencies.sh

#!/bin/bash

# Update package lists
apt-get update

# Upgrade existing packages
apt-get upgrade

# Assuming Python3 and pip are already installed, if not, you should install them using:
# apt-get install python3 python3-pip

# Install pandas
pip3 install pandas

# Since 'direct_redis' is not a standard Python package and not available
# in PyPI, it is assumed that 'direct_redis' can be installed via a Git repository.
# Replace 'git+https://github.com/your-repo/direct_redis.git' with the actual URL of the repository.

# Install direct_redis (Mock example, replace with actual git repo if necessary)
pip3 install git+https://github.com/your-repo/direct_redis.git

# Or if 'direct_redis' can be installed from a local directory replace the next line with
# pip3 install /path/to/direct_redis/
