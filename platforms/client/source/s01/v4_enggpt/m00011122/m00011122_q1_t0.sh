# bash script
#!/bin/bash

# Update the package list
sudo apt-get update

# Install pip for Python 3
sudo apt-get install -y python3-pip

# Install pandas library
pip3 install pandas

# Install direct_redis library
pip3 install git+https://github.com/amyangfei/direct_redis.git
