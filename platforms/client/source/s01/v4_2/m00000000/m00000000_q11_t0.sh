#!/bin/bash
#Check if pip is installed. If not, install it
if ! which pip > /dev/null; then
   sudo apt-get install python-pip
fi

# Install required Python libraries
pip install pandas
pip install pymysql
