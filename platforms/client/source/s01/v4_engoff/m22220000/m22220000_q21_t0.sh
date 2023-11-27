# install_dependencies.sh
#!/bin/bash

# Update and Upgrade Package List
sudo apt-get update
sudo apt-get upgrade -y

# Install pip if not installed
sudo apt-get install python3-pip -y

# Install required Python libraries
pip3 install pymysql pandas

# Since direct_redis is a custom library not available in public repositories,
# we'll need a method of installing it. If the library was available it would
# typically be installed via pip, assuming it was on PyPI:
# pip3 install direct_redis

# If direct_redis is provided as a local file or from a private repository, the
# instruction would vary. Since I don't have access to the actual direct_redis
# module, I'll leave it up to the actual installation method required for that library.
