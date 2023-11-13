#!/bin/bash
echo "Installing dependencies...."
pip install mysql-connector-python pandas
echo "Dependencies were installed."
echo "Running the python code..."
python connect_mysql.py
echo "Python code ran successfully."
