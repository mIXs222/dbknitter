#!/bin/bash

# Install pip if not installed
if ! command -v pip &>/dev/null; then
    echo "pip could not be found, installing..."
    curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
    python get-pip.py
    rm get-pip.py
fi

# Install the required Python libraries
pip install pymongo pandas redis direct-redis
