# install_dependencies.sh

#!/bin/bash
# Create and activate a virtual environment
python3 -m venv query_environment
source query_environment/bin/activate

# Install Pandas and custom DirectRedis dependency (assuming available)
pip install pandas
pip install direct-redis

# Run the Python script
python query_redis.py

# Deactivate the virtual environment
deactivate
