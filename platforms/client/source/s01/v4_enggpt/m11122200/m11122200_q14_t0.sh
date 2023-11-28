#!/bin/bash

# Ensure the script is executable: chmod +x setup.sh

# Activate virtual environment or create one if it doesn't exist
if [[ ! -d "venv" ]]; then
  python3 -m venv venv
fi
source venv/bin/activate

# Install necessary packages
pip install pymysql pymongo pandas

# Notify completion
echo "Setup complete. You can now run the script with 'python query.py'"
