# Bash script (setup.sh)
#!/bin/bash

# Update the package list
apt-get update -y

# Install python3 and pip if they are not installed
apt-get install python3 -y
apt-get install python3-pip -y

# Install required Python packages
pip3 install pymysql pandas redis direct-redis
