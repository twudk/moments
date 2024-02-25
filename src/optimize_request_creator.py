import json

from kafka import KafkaProducer

import moments as f
import logging

logging.basicConfig(level=logging.DEBUG)

# Configuration for Kafka Producer
kafka_broker_address = 'ws.twu.dk:9092'  # Change this to your Kafka broker address
kafka_topic = 'opt_request'  # Change this to your Kafka topic

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
        'start_date': start_date.isoformat(),
        'end_date': end_date.isoformat()
    }
    # Send the message
    try:
        producer.send(kafka_topic, value=message)
    except Exception as e:
        print(f"Failed to send message: {e}")


# Example usage
if __name__ == '__main__':
    batch_id = f.generate_random_uuid()

    tickers = ["SPY"]

    total_days_in_range = 365 * 1  # x years
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
