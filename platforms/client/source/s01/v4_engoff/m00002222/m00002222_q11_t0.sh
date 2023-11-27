# dependencies_install.sh

# Update package list
sudo apt-get update

# Install Python and pip
sudo apt-get install -y python3 python3-pip

# Install Python MySQL client
pip3 install pymysql

# Install Python Pandas library
pip3 install pandas

# Install direct_redis, which might need to be installed from the source if not available via pip
pip3 install direct-redis

# Note: If `direct_redis` is not directly installable via pip,
# You may have to install it from source or an external repository.
# Replace 'direct-redis' with the correct package name or installation method for direct_redis
