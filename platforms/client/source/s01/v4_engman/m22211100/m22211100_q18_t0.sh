# Filename: setup.sh
#!/bin/bash

# Create a virtual environment and activate it
python3 -m venv env
source env/bin/activate

# Update pip and install Python dependencies
pip install --upgrade pip
pip install pymysql==1.0.2 pymongo==4.1.1

# Run the python script
python query.py
