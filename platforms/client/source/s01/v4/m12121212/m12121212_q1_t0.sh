#!/bin/bash
# install_dependencies.sh

# Create a virtual environment
python3 -m venv venv_query_redis

# Activate the virtual environment
source venv_query_redis/bin/activate

# Install dependencies
pip install pandas
pip install direct_redis

# Note: There is no actual direct_redis package. This should be adjusted to the correct Redis client that provides the DirectRedis functionality for your specific setup.
