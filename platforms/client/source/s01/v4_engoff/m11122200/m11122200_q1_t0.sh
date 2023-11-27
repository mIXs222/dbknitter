# Write the Bash script to a file named 'install_dependencies.sh'
with open('install_dependencies.sh', 'w') as file:
    file.write("""#!/bin/bash
sudo apt-get update
sudo apt-get install -y python3-pip
pip3 install pymysql
""")
