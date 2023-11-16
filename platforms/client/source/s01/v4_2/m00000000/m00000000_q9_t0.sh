
# File: dependencies.sh

#!/bin/bash
sudo apt update && sudo apt upgrade -y
sudo apt install python3-dev python3-pip -y
pip3 install pymysql
