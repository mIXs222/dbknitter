#!/bin/bash
python3 -m venv env
source env/bin/activate
pip install pymysql
pip install pandas

# to run python code
python mengxi.py
