#!/bin/bash

# Activate virtualenv or create one if it does not exist
VIRTUALENV_NAME="query_env"
if [[ ! -d "$VIRTUALENV_NAME" ]]; then
    python3 -m venv $VIRTUALENV_NAME
fi
source "$VIRTUALENV_NAME/bin/activate"

# Install the necessary Python packages
pip install pymysql pandas direct-redis

# Run the Python script (assuming it's named run_query.py)
python run_query.py
