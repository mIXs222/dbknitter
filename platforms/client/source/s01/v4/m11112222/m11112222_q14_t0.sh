# install_dependencies.sh
#!/bin/bash

# Install Python MongoDB driver (pymongo)
pip install pymongo

# Install Redis driver (redis-py) along with direct_redis for Pandas dataframe support
pip install redis direct_redis

# Install Pandas
pip install pandas
