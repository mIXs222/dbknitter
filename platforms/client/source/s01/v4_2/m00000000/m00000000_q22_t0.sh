# bash_script.sh

#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python and pip
sudo apt-get install python3.8
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python3.8 get-pip.py

# Install pymysql
pip install pymysql

# Install csv module
pip install python-csv
