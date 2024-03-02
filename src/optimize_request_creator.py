import os
import sys
import json
import logging
from datetime import datetime, timedelta
from kafka import KafkaProducer
import moments as f  # Assuming 'moments' is an application-specific module

# Configure logging
logging.basicConfig(level=logging.INFO)

# Constants
KAFKA_BROKER_ADDRESS = os.getenv("KAFKA_BROKER_ADDRESS")
KAFKA_TOPIC_OPT_REQUEST = 'opt_request'
KAFKA_TOPIC_OPT_RESPONSE = 'opt_response'

if not KAFKA_BROKER_ADDRESS:
    logging.error("Kafka broker address is empty. Set the environment variable KAFKA_BROKER_ADDRESS.")
    sys.exit(1)

logging.info("Kafka broker address: %s", KAFKA_BROKER_ADDRESS)

# Kafka Producer Configuration
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER_ADDRESS],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all'
)


def send_request(batch_id: str, request_id: str, symbol: str, start_date: datetime, end_date: datetime) -> None:
    """Sends a request to Kafka topic."""
    message = {
        'batch_id': batch_id,
        'request_id': request_id,
        'symbol': symbol,
        'optimize_on': 'SQN',
        'sampling_step': 1,
        'start_date': start_date.isoformat(),
        'end_date': end_date.isoformat()
    }
    try:
        producer.send(KAFKA_TOPIC_OPT_REQUEST, value=message)
        logging.info(f"Message sent for symbol: {symbol}")
    except Exception as e:
        logging.error(f"Failed to send message: {e}")


if __name__ == '__main__':
    batch_id = f.generate_random_uuid()

    tickers = ["XLF"]  # Example ticker, extend as needed
    start_date = datetime.strptime('2024-01-01', '%Y-%m-%d').date()
    end_date = datetime.strptime('2024-02-26', '%Y-%m-%d').date()

    sampling_frequency = 5
    walk_back_in_days = 30 * 6

    for ticker in tickers:
        stock_data = f.download_stock_data(ticker, start_date, end_date)

        for sampling_end_date in stock_data.iloc[::sampling_frequency].index.date:
            sampling_start_date = sampling_end_date - timedelta(days=walk_back_in_days)
            send_request(batch_id, f.generate_random_uuid(), ticker, sampling_start_date, sampling_end_date)

    producer.flush()
