# Use the official MongoDB Community Server image as the base image
FROM mongodb/mongodb-community-server:6.0-ubi8


# Expose port 27017 for MongoDB connections
EXPOSE 27017

RUN python3 -m pip install pandas pymongo

#to run the container
#docker run  --name mongodb -d -p 27017:27017 dbknitter_mongodb

#to build the docker image
#docker build -t dbknitter_mongodb .

#to run the container and mount the data
#docker run  --name mongodb -d -v "$(pwd)/data:home/data" -p 27017:27017 dbknitter_mongodb

# to enter the bash of a running container
#docker exec -it mongodb bash

# to enter the bash of a running container as root
# docker exec -u root -it mongodb bash
