# talk to any kafka and topic, configuration
# fetch stock price every second
import argparse
import json
import time
import logging
import atexit


from apscheduler.schedulers.background import BackgroundScheduler
from kafka.errors import KafkaError, KafkaTimeoutError
from kafka import KafkaProducer
from googlefinance import getQuotes


logging.basicConfig()
logger = logging.getLogger('data-producer')
logger.setLevel(logging.DEBUG)


symbol = 'AAPL'
topic_name = 'stock'
kafka_broker = '127.0.0.1:9092'

def shutdown_hook():
	try:
		producer.flush(10)
		logger.info("shutdow resources")
	except KafkaError as ke:
		logger.warn('failed to flush kafka')
	finally:
		producer.close(10)
		schedule.shutdown()



def fetch_price(producer, symbol):
	try:
		price = json.dumps(getQuotes(symbol)).encode('utf-8')
		logger.debug('received stock price %s' % price)
		producer.send(topic=topic_name, value=price, timestamp_ms=int(time.time())) ###此处注意转换type为int

	except KafkaTimeoutError as timeout_error:
		logger.warn('failed to send stock price for %s to kakfa' %symbol)
	except Exception:
		logger.warn('failed to send stock price')
if __name__ == '__main__':
	#argparser
	parser = argparse.ArgumentParser()
	parser.add_argument('symbol', help = 'the symbol of the stock')
	parser.add_argument('topic_name', help = 'topic_name')
	parser.add_argument('kafka_broker', help = 'broker')
	
	args = parser.parse_args()
	symbol = args.symbol
	topic_name = args.topic_name
	kafka_broker = args.kafka_broker

	producer = KafkaProducer(bootstrap_servers=kafka_broker)
	
	schedule = BackgroundScheduler()
	schedule.add_executor('threadpool')
	
	schedule.add_job(fetch_price, 'interval', [producer, symbol], seconds= 3)
	
	schedule.start()

	#fetch_price(producer,symbol)
	atexit.register(shutdown_hook,producer)

	while True:
		pass

