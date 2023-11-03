In the following example, we assume the configuration is "[0, 0, 0, 0, 1, 1, 1, 1]"

1. make sure all the containers are stopped
2. clear all the containers
   bash cloudlab/clear-all.sh
3. start all the containers
   bash cloudlab/start-all.sh
4. After all the containers are started, create a new terminal and run:
   bash load_table.sh "[0, 0, 0, 0, 1, 1, 1, 1]"
5. After successfully load the table, enter client docker container
   docker exec -it cloudlab-client-1 bash
6. enter platform
   cd platform
7. run the grader
   run_grader.sh 00001111
