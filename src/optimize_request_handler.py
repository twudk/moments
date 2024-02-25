from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import json
import moments as f
from backtesting import Backtest

rounding_digits = 4
cash = 1000000
optimize_on = 'SQN'
sample_step = 1

# Kafka configuration
kafka_broker_address = 'ws.twu.dk:9092'
kafka_topic_opt_request = 'opt_request'
kafka_topic_opt_response = 'opt_response'

# Create a Kafka consumer instance
consumer = KafkaConsumer(
    bootstrap_servers=[kafka_broker_address],
    auto_offset_reset='earliest',  # Start reading at the earliest message if the specified offset is invalid
    enable_auto_commit=True,  # Automatically commit offsets
    group_id='request-handler-group',  # Consumer group ID
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize the message from JSON
)

# Create a Kafka producer instance
producer = KafkaProducer(
    bootstrap_servers=[kafka_broker_address],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # Serialize the message to JSON formatted string
    acks='all'
)

# Assign the consumer to a specific topic and partition
partition = 0  # Specify the partition you want to consume from (assuming single partition for simplicity)
topic_partition = TopicPartition(kafka_topic_opt_request, partition)
consumer.assign([topic_partition])

# Specify the offset from which to start consuming
# offset = 10  # Change this to the offset from which you want to consume
# consumer.seek(topic_partition, offset)

# Consume messages
try:
    for message in consumer:
        batch_id = message.value['batch_id']
        request_id = message.value['request_id']
        symbol = message.value['symbol']
        start_date = datetime.fromisoformat(message.value['start_date'])
        end_date = datetime.fromisoformat(message.value['end_date'])

        print(
            f"Received message: {message.value} from topic: {message.topic}, partition: {message.partition}, offset: {message.offset}")

        try:
            stock_data = f.download_stock_data(symbol, start_date, end_date)
            backtest_x = Backtest(stock_data, f.MyStrategy, cash=cash, exclusive_orders=True, trade_on_close=True)

            opt_stats_x, heatmap = backtest_x.optimize(
                o_profit_target=range(2, 10, sample_step),
                o_stop_limit=range(2, 5, sample_step),
                o_max_days=range(16, 24, sample_step),
                o_sleep_after_loss=range(2, 10, sample_step),
                maximize=optimize_on,
                return_heatmap=True
            )

            exposure_time = opt_stats_x['Exposure Time [%]']
            return_pct = opt_stats_x['Return [%]']
            buy_and_hold_return_pct = opt_stats_x['Buy & Hold Return [%]']
            max_draw_down = opt_stats_x['Max. Drawdown [%]']
            sqn = opt_stats_x['SQN']

            message = {
                'batch_id': batch_id,
                'request_id': request_id,
                'symbol': symbol,
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'profit_target': int(opt_stats_x._strategy.o_profit_target),
                'stop_limit': int(opt_stats_x._strategy.o_stop_limit),
                'sleep_after_loss': int(opt_stats_x._strategy.o_sleep_after_loss),
                'max_days': int(opt_stats_x._strategy.o_max_days),
                'exposure_time': exposure_time,
                'return_pct': return_pct,
                'buy_and_hold_return_pct': buy_and_hold_return_pct,
                'max_draw_down': max_draw_down,
                'sqn': sqn,
            }

            # Send the message
            try:
                producer.send(kafka_topic_opt_response, value=message)
                producer.flush()
            except Exception as e:
                print(f"Failed to send message: {e}")

            print(message)

        except Exception as e:
            print(e)

except KeyboardInterrupt:
    print("Stopping consumer")

# Close the consumer connection
consumer.close()
