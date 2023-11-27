# Dependency Installation Bash
 echo 'Starting script'
 
 echo 'Updating system repositories'
 sudo apt update

 echo 'Installing Python pip'
 sudo apt install -y python3-pip

 echo 'Installing Python dev tools'
 sudo apt install -y build-essential libssl-dev libffi-dev python3-dev

 echo 'Installing virtualenv'
 sudo apt install -y python3-venv

 echo 'Creating virtual environment'
 python3 -m venv myenv

 echo 'Activating virtual environmet'
 source myenv/bin/activate
  
 echo 'Installing MySQL Connector'
 pip install mysql-connector-python

 echo 'Installing Pymongo'
 pip install pymongo

 echo 'Installing Pandas'
 pip install pandas

 echo 'Done'
