# install_dependencies.sh
#!/bin/bash

# Ensure pip is available
python -m ensurepip --upgrade

# Install necessary Python packages
pip install pandas
pip install direct_redis
