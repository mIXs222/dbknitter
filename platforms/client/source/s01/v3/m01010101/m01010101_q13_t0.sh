$ echo "mysql-connector-python" >> requirements.txt
$ echo "pymongo" >> requirements.txt
$ python -m pip install -r requirements.txt

apt-get update
apt-get install -y python3 python3-pip
pip3 install mysql-connector-python pymongo
