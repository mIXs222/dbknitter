#!/bin/bash

# Install pip if not already installed
if ! which pip > /dev/null; then
   curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
   python get-pip.py
   rm get-pip.py
fi

# Install dependencies
pip install pandas
pip install redis
