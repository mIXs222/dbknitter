# install_dependencies.sh
#!/bin/bash

# Update the system and get required build tools
sudo apt-get update
sudo apt-get install -y build-essential

# Install Python 3 and pip if they are not already installed
sudo apt-get install -y python3 python3-pip

# Install Python packages needed
pip3 install pandas redis direct_redis

# Set executable permissions for the script
chmod +x forecast_revenue_change.py

# Run the Python script to generate the query output
./forecast_revenue_change.py
