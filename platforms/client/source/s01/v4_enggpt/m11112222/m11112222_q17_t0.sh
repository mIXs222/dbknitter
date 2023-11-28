#!/bin/bash

# creates a python virtual environment
python3 -m venv env

# activates the virtual environment
source env/bin/activate

# upgrade pip to its latest version
pip install --upgrade pip

# install pymongo
pip install pymongo

# install pandas
pip install pandas

# install redis and direct_redis
pip install redis direct_redis
