sudo apt-get update
sudo apt-get install -y python3-pip 
sudo apt-get install -y python3-venv
sudo apt-get install -y libmysqlclient-dev
sudo apt-get install -y python3-dev

python3 -m venv myenv
source myenv/bin/activate
pip install mysql-connector-python
pip install pandas
python python_code.py
