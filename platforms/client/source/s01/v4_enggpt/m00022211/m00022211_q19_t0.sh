# Bash script to install dependencies for running Python code
#!/bin/bash

# Update the package manager
sudo apt-get update

# Install Python and pip if they are not already installed
sudo apt-get install python3 python3-pip -y

# Install the required Python libraries
pip3 install pymysql pymongo
