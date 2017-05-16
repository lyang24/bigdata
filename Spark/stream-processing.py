import atexit
import logging
import json
import sys
import time

from pyspark import SparkContext
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


logging.basicConfig()  #create a default stream handler for root logger, if a handler is created, then it won't work.
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG) #control all log handler's level, only outputs the level higher or equal to DEBUG

formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')

file_handler = logging.FileHandler('Average_stock_Price.log')
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.INFO)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.DEBUG)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)




topic = 'stock-analyzer'
target_topic = 'average-stock-price'
brokers ='localhost:9092'
kafka_producer = None

def process(time, rdd):
    num_of_record = rdd.count()
    logger.debug("Begin process...")
    if num_of_record == 0:
        logger.debug("No records...")
        return

    #get different stocks, calculate the average stock price of previous 5 records (sliding window)

    stockAndPrice = rdd.map(lambda record: (json.loads(record[1])[0].get('StockSymbol') ,float(json.loads(record[1])[0].get('LastTradePrice'))))
    stockAndAverage = stockAndPrice.reduceByKey(lambda a, b: (a + b)/2 )
    savg= stockAndAverage.collect()
    for record in savg:
        stock_symbol = record[0]
        average = record[1]
        logger.info('Average price of stock {} is {}'.format(stock_symbol, average))
        data = json.dumps({
            'average': average
        })
        try:
            logger.debug('Sending average price {} to kafka for stock {}'.format(data, stock_symbol))
            kafka_producer.send(target_topic,value =data.encode("utf-8"))
        except KafkaError as error:
            logger.warn('Failed to send average stock price to kafka, caused by: %s', error.message)


def shutdown_hook(producer):
    """
    a shutdown hook to be called before the shutdown
    :param producer: instance of a kafka producer
    :return: None
    """
    try:
        logger.debug('Flushing pending messages to kafka, timeout is set to 10s')
        producer.flush(10)
        logger.debug('Finish flushing pending messages to kafka')
    except KafkaError as kafka_error:
        logger.warn('Failed to flush pending messages to kafka, caused by: %s', kafka_error.message)
    finally:
        try:
            logger.debug('Closing kafka connection')
            producer.close(10)
        except Exception as e:
            logger.warn('Failed to close kafka connection, caused by: %s', e.message)



if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Usage: stream-process.py [topic] [target-topic] [broker-list]")
        exit(1)

    # - create SparkContext and StreamingContext
    sc = SparkContext("local[2]", "StockAveragePrice") 
    #sc.setLogLevel('DEBUG')
    ssc = StreamingContext(sc, 5) #5sec per run

    topic, target_topic, brokers = sys.argv[1:]

    # - instantiate a kafka stream for processing
    directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {'metadata.broker.list': brokers}) #exactly once delievery
    directKafkaStream.foreachRDD(process)

    # - instantiate a simple kafka producer
    kafka_producer = KafkaProducer(
        bootstrap_servers=brokers
    )

    # - setup proper shutdown hook
    atexit.register(shutdown_hook, kafka_producer)

    ssc.start()
    ssc.awaitTermination()
