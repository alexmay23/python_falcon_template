import json
import logging
import re
import threading
import uuid
import amqp
import time

logger = logging.getLogger(__name__)


class Client(object):
    def __init__(self, name, prefix='am', host='localhost', user='guest', password='guest', vhost='/', timeout=5,
                 dumper=None, exchange_type='topic'):
        if not prefix.endswith('_'):
            prefix += '_'
        if len(name) > 0 and not name.endswith('_'):
            name += '_'
        self.credentials = {
            'host': host,
            'userid': user,
            'password': password,
            'virtual_host': vhost
        }
        if dumper is None:
            dumper = json
        self.dumper = dumper
        self.timeout = timeout
        self.prefix = prefix
        self.name = name
        self.exchange_name = self.prefix + 'exchange_' + exchange_type
        self.exchange_type = exchange_type
        self.connection = None
        self.channel = None
        self.connect()

    def connect(self):
        self.connection = amqp.Connection(**self.credentials)
        self.channel = self.connection.channel()

    def close(self):
        self.channel.close()
        # self.connection.close()

    def call(self, key, *args, **kwargs):
        correlation_id = str(uuid.uuid4())
        reply = self.channel.queue_declare(exclusive=True).queue
        routing_key = (self.prefix + self.name).replace('_', '.') + key
        body = self.dumper.dumps({'args': args, 'kwargs': kwargs})
        self.channel.basic_publish(
            amqp.Message(body, reply_to=reply, correlation_id=correlation_id),
            self.exchange_name, routing_key=routing_key,

        )
        logger.debug('Message %s with routing key %s published', body, routing_key)
        logger.debug('Waiting for reply in queue %s with correlation id %s', reply, correlation_id)

        message = {}

        def _reply(_message):
            logger.debug(
                'Reply %s with correlation id %s received', _message.body, _message.properties['correlation_id']
            )
            if _message.properties['correlation_id'] == correlation_id:
                message['result'] = self.dumper.loads(_message.body)
                self.channel.basic_ack(_message.delivery_tag)

        while True:
            self.channel.basic_consume(reply, callback=_reply)
            self.connection.drain_events(self.timeout)
            if 'result' in message:
                return message['result']

    def publish(self, _name, *args, **kwargs):
        routing_key = (self.prefix + self.name).replace('_', '.') + _name
        body = self.dumper.dumps({'args': args, 'kwargs': kwargs})
        self.channel.basic_publish(amqp.Message(body), self.exchange_name, routing_key=routing_key)
        logger.debug('Message %s with routing key %s published', body, routing_key)


def run_server(server):
    try:
        server.start()
    except KeyboardInterrupt:
        server.channel.close()
        server.connection.close()


class Server(object):
    def __init__(self, name='', prefix='am', threaded=False, host='localhost', user='guest', password='guest',
                 vhost='/', dumper=None, exchange_type='topic', prefetch_count=5):
        if not prefix.endswith('_'):
            prefix += '_'
        if len(name) > 0 and not name.endswith('_'):
            name += '_'
        self.credentials = {
            'host': host,
            'userid': user,
            'password': password,
            'virtual_host': vhost
        }
        if dumper is None:
            dumper = json
        self.dumper = dumper
        self.prefix = prefix
        self.prefetch_count = prefetch_count
        self.name = name
        self.exchange_name = self.prefix + 'exchange_' + exchange_type
        self.queue_name = self.prefix + name + 'queue_' + exchange_type
        self.threaded = threaded
        self.connection = None
        self.channel = None
        self.exchange_type = exchange_type
        self.endpoints = []

    def register_endpoint(self, key, endpoint):
        key = (self.prefix + self.name).replace('_', '.') + key
        pattern = '[^.]*'.join([re.escape(i) for i in key.split('*')])
        self.endpoints.append((key, re.compile(r'^{}$'.format(pattern)), endpoint))

    def connect(self):
        self.connection = amqp.Connection(**self.credentials)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(self.exchange_name, self.exchange_type)
        self.channel.basic_qos(0, self.prefetch_count, False)
        if self.exchange_type == 'fanout':
            self.queue_name = self.channel.queue_declare().queue
        else:
            self.channel.queue_declare(self.queue_name, auto_delete=False)
        self.prepare_queues()

    def prepare_queues(self):
        for key, pattern, endpoint in self.endpoints:
            self.channel.queue_bind(self.queue_name, self.exchange_name, routing_key=key)
            if self.exchange_type == 'fanout':
                self.channel.basic_consume(self.queue_name, callback=self.consume, no_ack=True)
            else:
                self.channel.basic_consume(self.queue_name, callback=self.consume)
            logger.debug('Endpoint %s bound to %s', key, self.queue_name)

    def handle(self, message):
        logger.debug('Starting execution')
        for key, pattern, endpoint in self.endpoints:
            if pattern.match(message.delivery_info['routing_key']):
                try:
                    msg = self.dumper.loads(message.body)
                    result = endpoint(*msg.get('args', []), **msg.get('kwargs', {}))
                    if self.exchange_type != 'fanout':
                        message.channel.basic_ack(message.delivery_tag)
                except Exception as e:
                    logger.error(e)
                    result = e
                logger.debug('Execution ended')
                if message.properties.get('reply_to'):
                    try:
                        body = self.dumper.dumps(result)
                    except Exception:
                        body = self.dumper.dumps({'error': 'Error dump result'})
                    logger.debug(
                        'Sending a reply %s to %s with correlation id %s', body, message.properties['reply_to'],
                        message.properties['correlation_id']
                    )
                    self.channel.basic_publish(
                        amqp.Message(body, correlation_id=message.properties['correlation_id']),
                        routing_key=message.properties['reply_to']
                    )

    def consume(self, message):
        logger.debug('Message %s with routing key %s received', message.body, message.delivery_info['routing_key'])
        if self.threaded:
            p = threading.Thread(target=self.handle, args=(message,))
            p.daemon = True
            p.start()
        else:
            self.handle(message)

    def start(self):
        logger.debug('Start consuming')
        try:
            self.connect()
            while True:
                self.channel.wait()
        except KeyboardInterrupt:
            logger.debug('Stop consuming')
            self.channel.close()
            # self.connection.close()
        except Exception as e:
            try:
                self.channel.close()
            except Exception:
                pass
            logger.error(e)
            logger.debug('Reconnect')
            time.sleep(5)
            self.start()