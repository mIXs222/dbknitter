# install.sh

# Update package list
apt-get update

# Install Python pip
apt-get install -y python3-pip

# Install required Python packages
pip3 install pandas pymysql pymongo direct-redis
