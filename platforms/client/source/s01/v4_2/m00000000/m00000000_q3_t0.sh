#!/bin/bash
# Update system
apt-get update -y
apt-get upgrade -y

# Install Python and pip
apt-get install python3 -y
apt-get install python3-pip -y

# Install python libraries
pip3 install pandas
pip3 install pymysql 

# End of Script
