# install_dependencies.sh
#!/bin/bash

# Create a virtual environment
python3 -m venv venv

# Activate the virtual environment
source venv/bin/activate

# Install dependencies
pip install pymongo pandas direct_redis

# Deactivate the virtual environment
deactivate
