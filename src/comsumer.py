from kafka import KafkaConsumer, TopicPartition

# Define the Kafka topic you want to consume from
topic_name = 'opt_response'

# Create a KafkaConsumer for the specified topic
consumer = KafkaConsumer(
    bootstrap_servers=['ws.twu.dk:29092'],  # List of broker addresses
    auto_offset_reset='earliest',  # Start reading at the earliest message
    group_id='group',  # Consumer group ID
    value_deserializer=lambda x: x.decode('utf-8')  # Deserialize messages from Kafka as UTF-8 strings
)

# Get the list of partitions for the topic
partitions = consumer.partitions_for_topic(topic_name)
# Create a TopicPartition instance for each partition
topic_partitions = [TopicPartition(topic_name, p) for p in partitions]

# Assign the consumer to all partitions
consumer.assign(topic_partitions)

for tp in topic_partitions:
    consumer.seek(tp, 0)

# Consume messages from the topic
counter = 0
for message in consumer:
    print(f"Received message: {counter} {message.partition} - {message.offset} - {message.value}")
    counter = counter + 1

# Remember to close the consumer when done
consumer.close()
