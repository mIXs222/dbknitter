#!/bin/bash
# Check if pip is installed, if not, install it
if ! command -v pip &> /dev/null; then
    echo "pip could not be found, installing..."
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        sudo apt-get update
        sudo apt-get install python3-pip -y
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        # Mac OSX
        curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
        python3 get-pip.py
    else
        echo "Script does not support this OS."
        exit 1
    fi
fi

# Install pymysql
pip install pymysql
