# talk to any kafka and topic, configuration
# fetch stock price every second
import argparse
import json
import time
import logging
import atexit
import requests



from py_zipkin.zipkin import zipkin_span
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


def http_transport_handler(span):
	body = '\x0c\x00\x00\x00\x01'.encode("utf-8")+ span
	requests.post(
		'http://localhost:9411/api/v1/spans',
		data = body,
		headers={'Content-Type':'application/x-thrift'}
		)

def shutdown_hook():
	try:
		producer.flush(10)
		logger.info("shutdow resources")
	except KafkaError as ke:
		logger.warn('failed to flush kafka')
	finally:
		producer.close(10)
		schedule.shutdown()

@zipkin_span(service_name='demo', span_name='fetch_price')
def fectch_price(symbol):
	return json.dumps(getQuotes(symbol)).encode('utf-8')

@zipkin_span(service_name='demo', span_name='send_to_kafka')
def send_to_kafka(producer, price):
	try:
		logger.debug('received stock price %s' % price)
		producer.send(topic=topic_name, value=price, timestamp_ms=time.time())
	except KafkaTimeoutError as timeout_error:
		logger.warn('failed to send stock price for %s to kakfa' %symbol)
	except Exception:
		logger.warn('failed to send stock price')

def fetch_price_and_send(producer, symbol):
	with zipkin_span(service_name="demo", span_name='data_producer', transport_handler=http_transport_handler, sample_rate = 100.0):
		price = fectch_price(symbol)
		send_to_kafka(producer, price)




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

	# print(symbol)
	# print(topic_name)
	# print(kafka_broker)

	producer = KafkaProducer(bootstrap_servers=kafka_broker)
	
	schedule = BackgroundScheduler()
	schedule.add_executor('threadpool')
	
	schedule.add_job(fetch_price_and_send, 'interval', [producer, symbol], seconds= 1)
	
	schedule.start()

	#fetch_price(producer,symbol)
	atexit.register(shutdown_hook)

	while True:
		pass

