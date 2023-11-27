#!/bin/bash

# Create and activate a virtual environment

# Install pip if not installed
command -v pip >/dev/null 2>&1 || { echo >&2 "Pip not installed. Installing pip.";  curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py; python get-pip.py; }

# Install Redis and Pandas
pip install direct-redis pandas
