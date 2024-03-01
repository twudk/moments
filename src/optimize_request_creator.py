import sys
import os
import json
from kafka import KafkaProducer
import moments as f
from datetime import datetime
from datetime import timedelta

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


def send_request(r_batch_id, r_request_id, r_symbol, r_start_date, r_end_date):
    message = {
        'batch_id': r_batch_id,
        'request_id': r_request_id,
        'symbol': r_symbol,
        'optimize_on': 'SQN',
        'sampling_step': 1,
        'start_date': r_start_date.isoformat(),
        'end_date': r_end_date.isoformat()
    }
    # Send the message
    try:
        producer.send(kafka_topic_opt_request, value=message)
    except Exception as e:
        print(f"Failed to send message: {e}")


# Example usage
if __name__ == '__main__':
    batch_id = f.generate_random_uuid()

    tickers_us_sectors = ["XLF", "XLI", "XLV", "XLC", "XLK", "XLB", "XLRE", "XLY", "XLP", "XLE", "XLU"]
    tickers_us_industries = ["SMH", "ITB", "JETS", "FDN", "XRT", "IBB", "PHO", "IGV", "KIE", "AMLP", "KRE"]
    start_date = '2015-01-01'
    end_date = '2024-02-26'

    sampling_frequency = 5  # Sample every 5 days
    walk_back_in_days = 30 * 6  # Walk back 6 months

    for ticker in ['SPY']:
        stock_data = f.download_stock_data(
            ticker,
            datetime.strptime(start_date, '%Y-%m-%d').date(),
            datetime.strptime(end_date, '%Y-%m-%d').date())

        for sampling_end_date in stock_data.iloc[::sampling_frequency].index.date:
            sampling_start_date = sampling_end_date - timedelta(days=walk_back_in_days)
            try:
                send_request(batch_id, f.generate_random_uuid(), ticker, sampling_start_date, sampling_end_date)
            except Exception as e:
                print(e)

    producer.flush()
