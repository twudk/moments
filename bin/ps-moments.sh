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
      KAFKA_BROKER_ADDRESS: "ws.twu.dk:29092"
EOF

# Run docker-compose in detached mode
sudo docker compose up -d
