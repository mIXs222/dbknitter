echo "Installing python ..."
sudo apt update
sudo apt install python3.8

echo "Installing pip, python's package manager ..."
sudo apt install python3-pip

echo "Installing necessary python packages ..."
pip3 install redis
pip3 install pandas
pip3 install pandasql
