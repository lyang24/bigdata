import atexit
import logging
import json
import time

from googlefinance import getQuotes
from apscheduler.schedulers.background import BackgroundScheduler

from flask import (
    Flask,
    request,
    jsonify
)

from kafka import KafkaProducer
from kafka.errors import (
    KafkaError,
    KafkaTimeoutError
)


logging.basicConfig()  #create a default stream handler for root logger, if a handler is created, then it won't work.
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) #control all log handler's level, only outputs the level higher or equal to DEBUG

formatter = logging.Formatter('%(asctime)s:%(message)s')

file_handler = logging.FileHandler('Received_Data.log')
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.INFO)


stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.DEBUG)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)



app = Flask(__name__)
app.config.from_envvar('ENV_CONFIG_FILE')
kafka_broker = app.config['CONFIG_KAFKA_ENDPOINT']
topic_name = app.config['CONFIG_KAFKA_TOPIC']

producer = KafkaProducer(
    bootstrap_servers = kafka_broker
)

schedule = BackgroundScheduler()
schedule.add_executor('threadpool')
schedule.start()

symbols = set() #定义一个symbol的set


def shutdown_hook():
    """
    a shutdown hook to be called before the shutdown
    """
    try:
        logger.info('Flushing pending messages to kafka, timeout is set to 10s')
        producer.flush(10)
        logger.info('Finish flushing pending messages to kafka')
    except KafkaError as kafka_error:
        logger.warn('Failed to flush pending messages to kafka, caused by: %s', kafka_error.message)
    finally:
        try:
            logger.info('Closing kafka connection')
            producer.close(10)
        except Exception as e:
            logger.warn('Failed to close kafka connection, caused by: %s', e.message)
    try:
        logger.info('shutdown scheduler')
        schedule.shutdown()
    except Exception as e:
        logger.warn('Failed to shutdown scheduler, caused by: %s', e.message)


def fetch_price(symbol):
    """
    helper function to retrieve stock data and send it to kafka
    :param symbol: symbol of the stock
    :return: None
    """
    logger.debug('Start to fetch stock price for %s', symbol)
    try:
        price = json.dumps(getQuotes(symbol)).encode("utf-8")
        logger.info('Retrieved stock info for %s, the price is %s', symbol, price)
        producer.send(topic=topic_name, value=price)
        logger.debug('Sent stock price for %s to Kafka', symbol)
    except KafkaTimeoutError as timeout_error:
        logger.warn('Failed to send stock price for %s to kafka, caused by: %s', (symbol, timeout_error.message))
    except Exception:
        logger.warn('Failed to fetch stock price for %s', symbol)

#Uncaught TypeError: Cannot read property 'x' of undefined
@app.route('/<symbol>', methods=['POST'])
def add_stock(symbol):
    if not symbol:
        return jsonify({
            'error': 'Stock symbol cannot be empty'
        }), 400
    if symbol in symbols:
        pass
    else:
        symbols.add(symbol)
        logger.debug('Add stock retrieve job %s' % symbol)
        schedule.add_job(fetch_price, 'interval', [symbol], seconds=2,id=symbol)
    return jsonify(results=list(symbols)), 200


@app.route('/<symbol>', methods=['DELETE'])
def del_stock(symbol):
    if not symbol:
        return jsonify({
            'error': 'Stock symbol cannot be empty'
        }), 400
    if symbol not in symbols:
        pass
    else:
        symbols.remove(symbol)
        schedule.remove_job(symbol)
    return jsonify(results=list(symbols)), 200

if __name__ == '__main__':
    atexit.register(shutdown_hook)
    app.run(host='127.0.0.1', port=app.config['CONFIG_APPLICATION_PORT'])
