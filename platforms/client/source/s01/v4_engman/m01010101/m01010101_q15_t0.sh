#!/bin/bash

# Update the package list
sudo apt-get update -y

# Install MongoDB Python client (pymongo)
pip install pymongo

# Install csv module (should come by default with Python)
# No action is needed for csv since it's part of Python's standard library.
