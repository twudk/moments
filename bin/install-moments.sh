#!/bin/bash

# Create the directory if it doesn't exist
mkdir -p moments

# Navigate into the directory
cd moments

# Create the docker-compose.yml file
cat <<EOF >docker-compose.yml
services:
  moments:
    image: tawu04/moments:latest
    environment:
      MYSQL_ADDRESS: "ws.twu.dk"
      MYSQL_DB: "db"
      MYSQL_PORT: 3306
EOF

# Run docker-compose in detached mode
sudo docker compose up -d
