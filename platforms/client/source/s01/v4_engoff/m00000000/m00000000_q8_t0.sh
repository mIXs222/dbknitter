# install.sh
#!/bin/bash

# Updating Repositories
apt-get update

# Installing Python 3 and PIP
apt-get install python3 python3-pip -y

# Installing PyMySQL
pip3 install PyMySQL
