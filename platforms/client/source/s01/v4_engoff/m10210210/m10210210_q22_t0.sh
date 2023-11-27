#!/bin/bash
# setup_dependencies.sh

# Install Python MongoDB driver (pymongo)
pip install pymongo

# Install pandas, required for DataFrame operations
pip install pandas

# Install direct_redis for connecting to Redis and its dependencies
pip install jsonpickle
pip install direct_redis
