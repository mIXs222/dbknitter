# install_dependencies.sh
# Assuming this script is run with superuser privileges

# Update package lists
apt-get update

# Install Python3 and pip if they are not already installed
apt-get install -y python3 python3-pip

# Install the required Python libraries
pip3 install pymysql pymongo pandas direct_redis
