#!/bin/bash
# file: install_dependencies.sh

# Ensure pip, setuptools, and wheel are up to date
pip install --upgrade pip setuptools wheel

# Install pymysql, which is a required package to connect to the MySQL database
pip install pymysql
