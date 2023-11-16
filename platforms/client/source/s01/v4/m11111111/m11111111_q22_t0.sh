# file: install_dependencies.sh

#!/bin/bash

# Ensure pip is installed
python3 -m ensurepip --upgrade

# Install pymongo to interact with MongoDB
pip3 install pymongo
