import sys
import os
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
import json
import moments as f
import dao as dao
from backtesting import Backtest
import socket

hostname = socket.gethostname()

# Mysql
host = os.getenv("MYSQL_ADDRESS")
port = int(os.getenv("MYSQL_PORT"))
db = os.getenv("MYSQL_DB")
user = 'tw'
password = 'tw'

# Kafka configuration
kafka_broker_address = os.getenv("KAFKA_BROKER_ADDRESS")
kafka_topic_opt_request = 'opt_request'
kafka_topic_opt_response = 'opt_response'

if kafka_broker_address is None:
    print("Kafka broker address is empty, set env variable KAFKA_BROKER_ADDRESS")
    sys.exit()
else:
    print("Kafka broker address: " + kafka_broker_address)

# Create a Kafka consumer instance
consumer = KafkaConsumer(
    kafka_topic_opt_request,
    bootstrap_servers=[kafka_broker_address],
    auto_offset_reset='latest',  # Start reading at the earliest message if the specified offset is invalid
    enable_auto_commit=False,  # Automatically commit offsets
    group_id='request-handler-group',  # Consumer group ID
    session_timeout_ms=304999,
    max_poll_interval_ms=304999,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize the message from JSON
)

# Create a Kafka producer instance
producer = KafkaProducer(
    bootstrap_servers=[kafka_broker_address],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # Serialize the message to JSON formatted string
    acks='all'
)

# Consume messages
try:
    for message in consumer:
        batch_id = message.value['batch_id']
        request_id = message.value['request_id']
        symbol = message.value['symbol']
        optimize_on = message.value['optimize_on']
        sampling_step = message.value['sampling_step']
        start_date = datetime.fromisoformat(message.value['start_date'])
        end_date = datetime.fromisoformat(message.value['end_date'])

        request_partition = message.partition
        request_offset = message.offset

        print(
            f"Received message: {message.value} from topic: {message.topic}, partition: {request_partition}, offset: {request_offset}")

        if dao.check_request_id_exists(host, port, user, password, db, request_id):
            continue

        try:
            stock_data = f.download_stock_data(symbol, start_date, end_date)
            backtest_x = Backtest(stock_data, f.MyStrategy, cash=1000000, exclusive_orders=True, trade_on_close=True)

            opt_stats_x, heatmap = backtest_x.optimize(
                o_profit_target=range(2, 10, sampling_step),
                o_stop_limit=range(2, 5, sampling_step),
                o_max_days=range(16, 24, sampling_step),
                o_sleep_after_loss=range(2, 10, sampling_step),
                maximize=optimize_on,
                return_heatmap=True
            )

            exposure_time = opt_stats_x['Exposure Time [%]']
            return_pct = opt_stats_x['Return [%]']
            buy_and_hold_return_pct = opt_stats_x['Buy & Hold Return [%]']
            max_draw_down = opt_stats_x['Max. Drawdown [%]']
            sqn = opt_stats_x['SQN']

            # Data to insert, now as a dictionary
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
                'exposure_time': exposure_time,
                'return_pct': return_pct,
                'buy_and_hold_return_pct': buy_and_hold_return_pct,
                'max_draw_down': max_draw_down,
                'sqn': sqn,
                'handler_host': hostname,
                'request_partition': request_partition,
                "offset": request_offset
            }

            print(data_to_insert)

            dao.add_opt_trading_parameters(host, port, user, password, db, data_to_insert)

            consumer.commit()

        except Exception as e:
            print(e)

except KeyboardInterrupt:
    print("Stopping consumer")

# Close the consumer connection
consumer.close()
