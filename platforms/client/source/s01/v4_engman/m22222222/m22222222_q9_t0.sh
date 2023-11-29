#!/bin/bash

# Install pip if not already installed
which pip >/dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "Installing pip..."
    curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
    python get-pip.py
    rm get-pip.py
fi

# Install necessary Python packages
pip install pandas redis direct_redis
