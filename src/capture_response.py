from kafka import KafkaConsumer, TopicPartition
import csv
import json
import os

# Define the Kafka topic you want to consume from
topic_name = 'opt_response'

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

header = ["batch_id", "request_id", "symbol", "start_date", "end_date", "profit_target", "stop_limit",
          "sleep_after_loss", "max_days", "exposure_time", "return_pct", "buy_and_hold_return_pct", "max_draw_down",
          "sqn", "handler_host"]


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
    if batch_id in ('277d15b3-ecdb-46c4-99d1-86fb19401e27', '5e050c38-be1b-41d4-872d-ffea12a00414',
                    '937bb270-3a18-49e0-af4b-6b72142ba201', 'ac23ec15-5108-4794-90c8-5d2b30cd6486'):
        continue
    symbol = message.value['symbol']
    output_file = '%s/parameters_v2_%s.csv' % (output_dir, symbol)
    if not os.path.exists(output_file):
        append_to_csv(output_file, header)
    row = [message.value[key] for key in header]
    append_to_csv(output_file, row)
    counter = counter + 1

# Remember to close the consumer when done
consumer.close()
