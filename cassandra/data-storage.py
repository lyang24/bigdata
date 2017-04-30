#- point to any kafka cluster and topic
#- point to any cassandra cluster * table
from cassandra.cluster import Cluster
from kafka import KafkaConsumer
from kafka.errors import KafkaError

import argparse
import atexit
import json
import logging

# - default kafka topic to read from
topic_name = 'stock-price'

# - default kafka broker location
kafka_broker = 'localhost:9092'

# - default cassandra nodes to connect
cassandra_cluster = 'localhost'

# - default keyspace to use
key_space = 'bittiger'

# - default table to use
data_table = 'stock'

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-storage')
logger.setLevel(logging.DEBUG)


def persist_data(stock_data, cassandra_session):
    """
    persist stock data into cassandra
    :param stock_data:
        the stock data looks like this:
        [{
            "Index": "NASDAQ",
            "LastTradeWithCurrency": "109.36",
            "LastTradeDateTime": "2016-08-19T16:00:02Z",
            "LastTradePrice": "109.36",
            "LastTradeTime": "4:00PM EDT",
            "LastTradeDateTimeLong": "Aug 19, 4:00PM EDT",
            "StockSymbol": "AAPL",
            "ID": "22144"
        }]
    :param cassandra_session:

    :return: None
    """
    # try:
    logger.debug('Start to persist data to cassandra %s' %stock_data)
    parsed = json.loads(stock_data.decode("utf-8"))[0]   #注意解码
    symbol = parsed.get('StockSymbol')
    price = float(parsed.get('LastTradePrice'))
    tradetime = parsed.get('LastTradeDateTime')
    statement = "INSERT INTO %s (stock_symbol, trade_time, trade_price) VALUES ('%s', '%s', %f)" % (table, symbol, tradetime, price)
    cassandra_session.execute(statement)
    logger.info('Persistend data to cassandra for symbol: %s, price: %f, tradetime: %s' % (symbol, price, tradetime))
# except Exception:
    #     logger.error('Failed to persist data to cassandra %s', stock_data)


def shutdown_hook(consumer, session):
    """
    a shutdown hook to be called before the shutdown
    :param consumer: instance of a kafka consumer
    :param session: instance of a cassandra session
    :return: None
    """
    try:
        logger.info('Closing Kafka Consumer')
        consumer.close()
        logger.info('Kafka Consumer closed')
        logger.info('Closing Cassandra Session')
        session.shutdown()
        logger.info('Cassandra Session closed')
    except KafkaError as kafka_error:
        logger.warn('Failed to close Kafka Consumer, caused by: %s', kafka_error.message)
    finally:
        logger.info('Existing program')


if __name__ == '__main__':

    # - setup command line arguments 命令行提示，参数
    parser = argparse.ArgumentParser() 
    parser.add_argument('topic_name', help='the kafka topic to subscribe from')
    parser.add_argument('kafka_cluster', help='the location of the kafka broker')
    parser.add_argument('cassandra_cluster', help='the cluster to use in cassandra')
    parser.add_argument('keyspace', help='the keyspace to use in cassandra')
    parser.add_argument('table', help='the data table to use')
    #parser.add_argument('contact_points', help='the contact points for cassandra')

    # - parse arguments
    args = parser.parse_args()
    topic_name = args.topic_name
    kafka_cluster = args.kafka_cluster
    cassandra_cluster = args.cassandra_cluster
    key_space = args.keyspace
    table = args.table

    #print(type(kafka_cluster))
    
    # - initiate a simple kafka consumer
    consumer = KafkaConsumer(
        topic_name,
        group_id = 'my_group',
        bootstrap_servers=kafka_cluster #新版kafka用bootstrap
    )
    # - initiate a cassandra session
    cassandra_cluster_obj = Cluster(
        contact_points=cassandra_cluster.split(',')
    )
    session = cassandra_cluster_obj.connect()

    #prepare statement prevent injection
    session.execute("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'} AND durable_writes = 'true'" % key_space)
    session.set_keyspace(key_space)
    session.execute("CREATE TABLE IF NOT EXISTS %s (stock_symbol text, trade_time timestamp, trade_price float, PRIMARY KEY (stock_symbol,trade_time))" % table)

    # - setup proper shutdown hook
    atexit.register(shutdown_hook, consumer, session)
    for msg in consumer:
        persist_data(msg.value, session)