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


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.client_fruit_top = {}
        self.received_tops_by_client = {}

    def _merge_tops(self, top1: list[fruit_item.FruitItem], top2: list[fruit_item.FruitItem]):
        result = []
        i_1 = 0
        i_2 = 0
        while i_1 + i_2 < TOP_SIZE:
            if top1[i_1] < top2[i_2]:
                result.append(top2[i_2])
                i_2 += 1
            else:
                result.append(top1[i_1])
                i_1 += 1
        return result

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        try:
            client, fruit_top_json = message_protocol.internal.deserialize(message)
            fruit_top = [ fruit_item.FruitItem(fruit_json[0],fruit_json[1]) for fruit_json in fruit_top_json ]
            if client in self.client_fruit_top:
                self.client_fruit_top[client] = self._merge_tops(self.client_fruit_top[client], fruit_top)
                self.received_tops_by_client[client] += 1 
            else: 
                self.client_fruit_top[client] = fruit_top
                self.received_tops_by_client[client] = 1

            if self.received_tops_by_client[client] == AGGREGATION_AMOUNT:
                self.output_queue.send(message_protocol.internal.serialize([ [ fruit.fruit, fruit.amount ] for fruit in self.client_fruit_top[client] ]))
                self.received_tops_by_client.pop(client)
                self.client_fruit_top.pop(client)

            ack()
        except Exception as e :
            logging.error(f"Error general: {e}")
            nack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)

    def stop(self):
        try:
            self.input_queue.stop_consuming()
            self.input_queue.close()
            self.output_queue.close()
        except Exception as e:
            logging.warning(f"Error durante manejo de sigterm: {e}")

def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()

    def handle_sigterm(signum, frame):
        logging.info(f"Señal recibida {signum}. Deteniendo..")
        return join_filter.stop()

    signal.signal(
        signal.SIGTERM,
        lambda signum, frame: handle_sigterm(),
    )

    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
