#!/bin/bash

# Create and activate a virtual environment
python3 -m venv venv_query_analysis
source venv_query_analysis/bin/activate

# Install required dependencies
pip install pandas
pip install direct_redis

# Deactivate the virtual environment
deactivate
