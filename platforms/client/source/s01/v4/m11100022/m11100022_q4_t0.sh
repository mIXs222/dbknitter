#!/bin/bash

# Create a virtual environment
python -m venv env

# Activate the virtual environment
source env/bin/activate

# Install the pandas library
pip install pandas

# The direct_redis library may have to be obtained through other means, as it is not a standard Python package.
# Typically, you would use pip to install a package from PyPI, but in this case, you should install it from wherever the source is, for example:
# pip install git+https://github.com/user/repo.git
# For demonstration purposes, the next line assumes it can be installed directly.
pip install direct_redis

# Deactivate the virtual environment
deactivate
