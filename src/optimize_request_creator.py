import sys
import os
import json
from kafka import KafkaProducer
import moments as f

# Kafka configuration
kafka_broker_address = os.getenv("KAFKA_BROKER_ADDRESS")
kafka_topic_opt_request = 'opt_request'
kafka_topic_opt_response = 'opt_response'

if kafka_broker_address is None:
    print("Kafka broker address is empty, set env variable KAFKA_BROKER_ADDRESS")
    sys.exit()
else:
    print("Kafka broker address: " + kafka_broker_address)

# Create a Kafka producer instance
producer = KafkaProducer(
    bootstrap_servers=[kafka_broker_address],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # Serialize the message to JSON formatted string
    acks='all'
)


def send_request(batch_id, request_id, symbol, start_date, end_date):
    message = {
        'batch_id': batch_id,
        'request_id': request_id,
        'symbol': symbol,
        'optimize_on': 'SQN',
        'sampling_step': 3,
        'start_date': start_date.isoformat(),
        'end_date': end_date.isoformat()
    }
    # Send the message
    try:
        producer.send(kafka_topic_opt_request, value=message)
    except Exception as e:
        print(f"Failed to send message: {e}")


# Example usage
if __name__ == '__main__':
    batch_id = f.generate_random_uuid()

    tickers = ["XLK"]

    total_days_in_range = 365 * 2  # x years
    unit_in_days = 7  # Capture every 7 days
    walk_back_in_days = 30 * 6  # Walk back 6 months

    for ticker in tickers:
        for offset in range(0, int((total_days_in_range - walk_back_in_days) / unit_in_days) - 1):
            try:
                start_date, end_date = f.get_start_end_date(unit_in_days * offset, walk_back_in_days)
                send_request(batch_id, f.generate_random_uuid(), ticker, start_date, end_date)
            except Exception as e:
                print(e)

    producer.flush()
