#!/bin/bash

# Create a virtual environment (optional)
python3 -m venv mongoenv
source mongoenv/bin/activate

# Install pymongo
pip install pymongo

# Run the Python script (assuming it's named query_script.py)
python query_script.py
