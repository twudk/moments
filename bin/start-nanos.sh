#!/bin/bash

# Define a list of hosts
HOSTS="host1.example.com host2.example.com host3.example.com"

# Command to run on each host
COMMAND="uname -a"

for HOST in $HOSTS; do
    ssh "$HOST" "$COMMAND"
done
