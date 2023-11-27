#!/bin/bash

# Update package lists
apt-get update

# Install Python and pip if they are not installed
apt-get install -y python3 python3-pip

# Install Python MongoDB client, direct_redis and pandas
pip3 install pymongo pandas

# Install the redis package for direct_redis
apt-get install -y redis

# direct_redis does not seem to be a standard Python package. The script assumes
# it is available. If this is a custom library, it would need to be installed manually
# or by specifying the exact location where it can be installed from.
# Since it's not a standard package, I'm commenting it out.
# pip3 install direct_redis
