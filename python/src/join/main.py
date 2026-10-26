import os
import logging
import signal 

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

class JoinTop:
    def __init__(self):
        self.top = []
        self.count = 0

    def merge(self,top):
        final_top = self.top + top
        final_top.sort(reverse=True)
        self.top = final_top[:TOP_SIZE]
        self.count += 1

    def result(self):
        return [ [ fruit.fruit, fruit.amount ] for fruit in self.top ]

    def reach_aggregation_amount(self):
        return self.count >= AGGREGATION_AMOUNT

class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.client_fruit_top: dict[JoinTop] = {}
        self.normal_operation = True
        
    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        try:
            client, fruit_top_json = message_protocol.internal.deserialize(message)
            fruit_top = [ fruit_item.FruitItem(fruit_json[0],fruit_json[1]) for fruit_json in fruit_top_json ]
            self.client_fruit_top[client] = self.client_fruit_top.get(client,JoinTop())
            self.client_fruit_top[client].merge(fruit_top)

            if self.client_fruit_top[client].reach_aggregation_amount():
                self.output_queue.send(message_protocol.internal.serialize([client, self.client_fruit_top[client].result()]))
                self.client_fruit_top.pop(client)

            ack()
        except Exception as e :
            nack()
            self._state_dependent_log(e)
            
    def start(self):
        try:
            self.input_queue.start_consuming(self.process_messsage)
        except Exception as e:
            self._state_dependent_log(e)

    def stop(self):
        self.normal_operation=False
        try:
            self.input_queue.stop_consuming()
            self.input_queue.close()
            self.output_queue.close()
            return 0
        except Exception as e:
            logging.warning(f"Error durante detención del servidor: {e}")
            return 1 
        
    def _state_dependent_log(self, exception: Exception):
        if self.normal_operation:
            logging.error(f"Error general durante operacion normal: {exception}")
            raise exception 
        else:
            logging.warning(f"Error general durante operacion anormal: {exception}")

def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()

    def handle_sigterm(signum, frame):
        logging.info(f"Señal recibida {signum}. Deteniendo..")
        return join_filter.stop()

    signal.signal(
        signal.SIGTERM,
        lambda signum, frame: handle_sigterm(signum,frame),
    )

    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
