# install_dependencies.sh

# Update package lists
sudo apt-get update

# Install Python and pip if they are not already installed
sudo apt-get install -y python3 python3-pip

# Install pymongo, pandas, and direct_redis
pip3 install pymongo pandas direct_redis
