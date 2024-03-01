from kafka import KafkaConsumer, TopicPartition
import csv
import json
import os

# Define the Kafka topic you want to consume from
topic_name = 'opt_request'

# Create a KafkaConsumer for the specified topic
consumer = KafkaConsumer(
    bootstrap_servers=['ws.twu.dk:29092'],  # List of broker addresses
    auto_offset_reset='earliest',  # Start reading at the earliest message
    group_id='group',  # Consumer group ID
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize the message from JSON
)

# Get the list of partitions for the topic
partitions = consumer.partitions_for_topic(topic_name)
# Create a TopicPartition instance for each partition
topic_partitions = [TopicPartition(topic_name, p) for p in partitions]

# Assign the consumer to all partitions
consumer.assign(topic_partitions)

for tp in topic_partitions:
    consumer.seek(tp, 0)

header = ["batch_id", "request_id", "symbol", "start_date", "end_date"]


# Function to append a row to the CSV
def append_to_csv(file_name, row_data):
    with open(file_name, 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(row_data)


# Consume messages from the topic
counter = 0
output_dir = '../data/output'
for message in consumer:
    print(f"Received message: {counter} {message.partition} - {message.offset} - {message.value}")
    batch_id = message.value['batch_id']
    symbol = message.value['symbol']
    output_file = '%s/parameters_v2_req%s.csv' % (output_dir, '')
    if not os.path.exists(output_file):
        offset_ = header + ['partition', 'offset']
        append_to_csv(output_file, offset_)
    row = [message.value[key] for key in header]
    row.append(message.partition)
    row.append(message.offset)
    append_to_csv(output_file, row)
    counter = counter + 1

# Remember to close the consumer when done
consumer.close()
