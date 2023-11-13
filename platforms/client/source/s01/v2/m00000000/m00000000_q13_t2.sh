# Bash Commands (setup.sh)

#!/bin/bash

# Install pip if not already installed
if ! command -v pip >/dev/null 2>&1; then 
    curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
    python get-pip.py
    rm get-pip.py
fi

# Install necessary Python packages
pip install pymysql pandas sqlalchemy
