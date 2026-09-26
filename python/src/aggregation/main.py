import os
import logging
import bisect
import signal 

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.client_fruit_top = {}  

    def _process_data(self, client, fruit, amount):
        logging.info(f"Processing data message {client} {fruit} {amount}")
        self.client_fruit_top[client] = self.client_fruit_top.get(client, [])
        new_fruit_value = fruit_item.FruitItem(fruit, amount)
        i = 0 
        while i < len(self.client_fruit_top[client]) and self.client_fruit_top[client][i].fruit != fruit:
            i += 1 
        if i < len(self.client_fruit_top[client]):
            new_fruit_value += self.client_fruit_top[client].pop(i) 

        bisect.insort(self.client_fruit_top[client], new_fruit_value)

    def _process_eof(self, client):
        logging.info(f"Received EOF {client}")
        fruit_chunk = list(self.client_fruit_top[client][-TOP_SIZE:])
        fruit_chunk.reverse()
        fruit_top = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )
        
        self.output_queue.send(message_protocol.internal.serialize((client, fruit_top)))
        self.client_fruit_top.pop(client)

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        try:
            fields = message_protocol.internal.deserialize(message)
            if len(fields) == 3:
                self._process_data(*fields)
                ack()
            elif len(fields) == 1:
                self._process_eof(*fields)
                ack()
            else:
                logging.error(f"Error del protocolo: se recibieron {fields}")
                nack()
        except Exception as e:
            logging.error(f"Error general: {e}")
            nack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)

    def stop(self):
        try:
            self.input_exchange.stop_consuming()
            self.input_exchange.close()
            self.output_queue.close()
        except Exception as e:
            logging.warning(f"Error durante manejo de sigterm: {e}")

def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()

    def handle_sigterm(signum, frame):
        logging.info(f"Señal recibida {signum}. Deteniendo..")
        return aggregation_filter.stop()

    signal.signal(
        signal.SIGTERM,
        lambda signum, frame: handle_sigterm(),
    )

    aggregation_filter.start()
    return 0

if __name__ == "__main__":
    main()
