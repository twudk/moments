#!/bin/bash

read -sp "Enter root password: " PASSWORD
echo # Move to a new line

# List of hosts
hosts=("nano1.twu.dk" "nano2.twu.dk" "nano3.twu.dk" "nano4.twu.dk" "nano5.twu.dk" "nano6.twu.dk" "nano7.twu.dk" "nano8.twu.dk")                                                                                                                                                                                    dk" "nano7.twu.dk" "nano8.twu.dk")

# SSH user
user="root"

# Command to run on each host
command="hostname ; cd /home/tw/moments ; sudo docker compose down -v"

# Loop through each host and run the command
for host in "${hosts[@]}"
do
  sshpass -p "$PASSWORD" ssh -o StrictHostKeyChecking=no "$user@$host" "$command"
done
