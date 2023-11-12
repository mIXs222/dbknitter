In the following example, we assume the configuration is 00001111

1. make sure all the containers are stopped
2. clear all the containers

   bash cloudlab/clear-all.sh

3. start all the containers

   bash cloudlab/start-all.sh

4. After all the containers are started, create a new terminal and run:

   bash load_tables.sh 00001111

5. After successfully load the table, enter client docker container

   docker exec -it cloudlab-client-1 bash

6. enter platform

   cd platform

7. run the grader (config, gpt_generated source code directory)

   bash run_grader.sh 00001111 /platform/source/s01/v1_9
