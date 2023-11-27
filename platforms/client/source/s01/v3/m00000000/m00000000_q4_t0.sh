#1. Python install dependencies
echo "pip install mysql-connector-python pymongo" > install_dependencies.sh

#2. Run python script
echo "python query.py" >> run.sh
echo "cat query_output.csv" >> run.sh
chmod +x install_dependencies.sh
chmod +x run.sh
