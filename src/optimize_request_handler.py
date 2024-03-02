import sys
import os
import json
from datetime import datetime
import socket
from kafka import KafkaConsumer, KafkaProducer
import moments as f
import dao
from backtesting import Backtest
import logging

logging.basicConfig(level=logging.INFO)

MYSQL_HOST = os.getenv("MYSQL_ADDRESS")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))  # Default port if not set
MYSQL_DB = os.getenv("MYSQL_DB")
MYSQL_USER = 'tw'
MYSQL_PASSWORD = 'tw'
KAFKA_BROKER_ADDRESS = os.getenv("KAFKA_BROKER_ADDRESS")
KAFKA_TOPIC_OPT_REQUEST = 'opt_request'
if KAFKA_BROKER_ADDRESS is None:
    sys.exit("Kafka broker address is empty, set env variable KAFKA_BROKER_ADDRESS")
HOSTNAME = socket.gethostname()


def create_kafka_consumer(broker_address, topic):
    return KafkaConsumer(
        topic,
        bootstrap_servers=[broker_address],
        auto_offset_reset='latest',
        enable_auto_commit=False,
        group_id='request-handler-group',
        session_timeout_ms=304999,
        max_poll_interval_ms=304999,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )


def create_kafka_producer(broker_address):
    return KafkaProducer(
        bootstrap_servers=[broker_address],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all'
    )


def consume_messages(consumer):
    for message in consumer:
        process_message(message)
        consumer.commit()


def process_message(message):
    data = message.value
    batch_id, request_id = data['batch_id'], data['request_id']
    symbol, optimize_on = data['symbol'], data['optimize_on']
    sampling_step = data['sampling_step']
    start_date, end_date = datetime.fromisoformat(data['start_date']), datetime.fromisoformat(data['end_date'])

    if dao.check_request_id_exists(MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB, request_id):
        return

    try:
        stock_data = f.download_stock_data(symbol, start_date, end_date)
        backtest_x = Backtest(stock_data, f.MyStrategy, cash=1000000, exclusive_orders=True, trade_on_close=True)

        opt_stats_x, _ = backtest_x.optimize(
            o_profit_target=range(2, 10, sampling_step),
            o_stop_limit=range(2, 5, sampling_step),
            o_max_days=range(16, 24, sampling_step),
            o_sleep_after_loss=range(2, 10, sampling_step),
            maximize=optimize_on,
            return_heatmap=True
        )

        data_to_insert = {
            "batch_id": batch_id,
            "request_id": request_id,
            "symbol": symbol,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "profit_target": int(opt_stats_x._strategy.o_profit_target),
            "stop_limit": int(opt_stats_x._strategy.o_stop_limit),
            "sleep_after_loss": int(opt_stats_x._strategy.o_sleep_after_loss),
            "max_days": int(opt_stats_x._strategy.o_max_days),
            'exposure_time': opt_stats_x['Exposure Time [%]'],
            'return_pct': opt_stats_x['Return [%]'],
            'buy_and_hold_return_pct': opt_stats_x['Buy & Hold Return [%]'],
            'max_draw_down': opt_stats_x['Max. Drawdown [%]'],
            'sqn': opt_stats_x['SQN'],
            'handler_host': HOSTNAME,
            'request_partition': message.partition,
            "offset": message.offset
        }

        dao.add_opt_trading_parameters(MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB, data_to_insert)
    except Exception as e:
        logging.info(f"Error processing message {message}: {e}")


def main():
    logging.info(f"Kafka broker address: {KAFKA_BROKER_ADDRESS}")
    consumer = create_kafka_consumer(KAFKA_BROKER_ADDRESS, KAFKA_TOPIC_OPT_REQUEST)
    try:
        consume_messages(consumer)
    except KeyboardInterrupt:
        logging.error("Stopping consumer")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
