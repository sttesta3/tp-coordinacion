import pika
import random
import string
from .middleware import MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareQueue, MessageMiddlewareExchange

AMQP_NETWORK_EXCEPTIONS = (pika.exceptions.AMQPConnectionError,pika.exceptions.AMQPChannelError, pika.exceptions.StreamLostError, ConnectionResetError)

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        # https://www.rabbitmq.com/tutorials/tutorial-one-python
        # Sin argumentos crea una cola clasica (en el ejemplo crea una quorum)
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
            self.channel = self.connection.channel()
            self.queue = self.channel.queue_declare(queue=queue_name)
        except AMQP_NETWORK_EXCEPTIONS:
            raise MessageMiddlewareDisconnectedError
        except Exception:
            raise MessageMiddlewareMessageError()

    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)
            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            on_message_callback(message=body,ack=ack,nack=nack)

        try: 
            self.channel.basic_consume(queue=self.queue.method.queue, on_message_callback=callback, auto_ack=False)
            self.channel.start_consuming()
        except pika.exceptions.ChannelClosed:
            raise MessageMiddlewareDisconnectedError()
        except AMQP_NETWORK_EXCEPTIONS:
            raise MessageMiddlewareDisconnectedError
        except Exception:
            # pika.exceptions.ReentrancyError cae bajo esta Excepcion
            raise MessageMiddlewareMessageError()

    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except AMQP_NETWORK_EXCEPTIONS:
            raise MessageMiddlewareDisconnectedError
        except Exception:
            raise MessageMiddlewareDisconnectedError()

    def send(self, message):
        try: 
            self.channel.basic_publish(
                exchange='',            # TODO: Default exchange 
                routing_key=self.queue.method.queue,    
                body=message
            )
        except pika.exceptions.ChannelClosed(): # https://pika.readthedocs.io/en/stable/modules/exceptions.html
            raise MessageMiddlewareDisconnectedError()
        except AMQP_NETWORK_EXCEPTIONS:
            raise MessageMiddlewareDisconnectedError
        except Exception as e:
            print("Error ", e)
            raise MessageMiddlewareMessageError()

    def close(self):
        # No debe revisarse connection.is_open ya que close lo revisa y raise ConnectionWrongStateError
        try:
            self.connection.close()
        except AMQP_NETWORK_EXCEPTIONS:
            raise MessageMiddlewareDisconnectedError
        except Exception as e:
            print(f"Error {e}")
            raise MessageMiddlewareCloseError()

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    def __init__(self, host, exchange_name, routing_keys):
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
            self.channel = self.connection.channel()

            # Exchange durable porque tiene que compartirse entre todos 
            self.exchange_name = exchange_name
            self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct', durable=True)

            # Cola exclusiva bindeada a todas las llaves de ruteo 
            self.queue = self.channel.queue_declare(queue='', exclusive=True)
            self.routing_keys = routing_keys
            for key in self.routing_keys: 
                self.channel.queue_bind(exchange=exchange_name, queue=self.queue.method.queue, routing_key=key)

        except AMQP_NETWORK_EXCEPTIONS:
            raise MessageMiddlewareDisconnectedError
        except Exception:
            raise MessageMiddlewareMessageError()


    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)
            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            on_message_callback(message=body,ack=ack,nack=nack)


        try: 
            self.channel.basic_consume(queue=self.queue.method.queue, on_message_callback=callback, auto_ack=False)
            self.channel.start_consuming()
        except pika.exceptions.ChannelClosed:
            raise MessageMiddlewareDisconnectedError()
        except AMQP_NETWORK_EXCEPTIONS:
            raise MessageMiddlewareDisconnectedError
        except Exception:
            # pika.exceptions.ReentrancyError cae bajo esta Excepcion
            raise MessageMiddlewareMessageError()

    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except AMQP_NETWORK_EXCEPTIONS:
            raise MessageMiddlewareDisconnectedError
        except Exception:
            raise MessageMiddlewareDisconnectedError()

    def send(self, message):
        try: 
            for key in self.routing_keys:
                self.channel.basic_publish(exchange=self.exchange_name, routing_key=key, body=message)
        except pika.exceptions.ChannelClosed(): # https://pika.readthedocs.io/en/stable/modules/exceptions.html
            raise MessageMiddlewareDisconnectedError()
        except AMQP_NETWORK_EXCEPTIONS:
            raise MessageMiddlewareDisconnectedError
        except Exception as e:
            print("Error ", e)
            raise MessageMiddlewareMessageError()

    def close(self):
        # No debe revisarse connection.is_open ya que close lo revisa y raise ConnectionWrongStateError
        try:
            self.connection.close()
        except AMQP_NETWORK_EXCEPTIONS:
            raise MessageMiddlewareDisconnectedError
        except Exception as e:
            print(f"Error {e}")
            raise MessageMiddlewareCloseError()