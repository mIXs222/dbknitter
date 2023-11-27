# create sh file
echo "
#!/bin/bash

# Ensure pip, setuptools, and wheel are up to date
python -m pip install --upgrade pip setuptools wheel

# Install required Python packages
pip install mysql-connector-python
pip install pymongo
pip install pandas
" > setup.sh

chmod +x setup.sh

# Run the bash file to install dependencies
./setup.sh
