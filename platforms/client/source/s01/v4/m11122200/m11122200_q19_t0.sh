# install_dependencies.sh
#!/bin/bash

# Update package manager (assumes Debian-based system)
sudo apt update

# Install MongoDB and MySQL drivers
pip install pymongo pymysql
