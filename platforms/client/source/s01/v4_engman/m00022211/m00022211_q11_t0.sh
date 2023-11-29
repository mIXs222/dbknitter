# setup.sh

# Update and install Python
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install pymysql and pandas
pip3 install pymysql pandas

# Install direct_redis
pip3 install git+https://github.com/Avature/direct-redis.git
