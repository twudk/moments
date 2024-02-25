import sys
import os
import json
from kafka import KafkaProducer
import moments as f
import datetime

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
        'sampling_step': 1,
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

    start_date = '2023-01-01'
    end_date = '2023-03-31'
    sampling_frequency = 5

    tickers = ["XLK"]

    total_days_in_range = 365 * 2  # x years
    walk_back_in_days = 30 * 6  # Walk back 6 months

    for ticker in tickers:
        stock_data = f.download_stock_data(ticker, start_date, end_date)

        for sampling_end_date in stock_data.iloc[::5].index.date:
            sampling_start_date = sampling_end_date - datetime.timedelta(days=30 * 6)
            try:
                send_request(batch_id, f.generate_random_uuid(), ticker, sampling_start_date, sampling_end_date)
            except Exception as e:
                print(e)

    producer.flush()
