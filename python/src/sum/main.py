import os
import logging
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.amount_by_client_fruit = {}

    def _process_data(self, client, fruit, amount):
#        logging.info(f"Process data {client} {fruit} {amount}")
        self.amount_by_client_fruit[client] = self.amount_by_client_fruit.get(client, {})
        self.amount_by_client_fruit[client][fruit] = self.amount_by_client_fruit[client].get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client, sums_already_red):
        logging.info(f"Sending data messages {client} \n{[ {fruit.fruit,fruit.amount} for fruit in self.amount_by_client_fruit[client].values() ]}")
        for final_fruit_item in self.amount_by_client_fruit[client].values():
            # Los mensajes tienen afinidad por fruta y por cliente 
            # El objetivo es que se distribuya la carga de forma estadisticamente uniforme, cumpliendo con los siguientes escenarios
            # - Muchos clientes con la misma fruta: OK, al agregar al cliente el trabajo es balanceado
            # - Un unico cliente con muchas frutas: OK, al agregar la fruta es balanceado 
            # Nota: No hay fruta cuyo nombre tenga menos de tres letras 
            aggregator = ( sum(ord(char) for char in final_fruit_item.fruit) + sum(ord(char) for char in str(client)) ) % AGGREGATION_AMOUNT
            self.data_output_exchanges[aggregator].send(
                message_protocol.internal.serialize(
                    [client, final_fruit_item.fruit, final_fruit_item.amount]
                )
            )

        sums_already_red.append(ID)  
        if len(sums_already_red) < SUM_AMOUNT:   # Quedan Instancias de Sum sin recibir este EOF
            self.input_queue.send(message_protocol.internal.serialize([client,sums_already_red]))
        else:                               # Ultimo Sum. Envio EOF a los Aggregators 
            logging.info(f"Broadcasting EOF message {client}")            
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(message_protocol.internal.serialize([client]))

        self.amount_by_client_fruit.pop(client)

    def process_data_messsage(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            if len(fields) == 3:
                self._process_data(*fields)
                ack()
            elif len(fields) == 2:                              # EOF ya recibido por otros sums
                if not ID in fields[1]:   
                    self._process_eof(*fields)
                    ack()                                      
                else:                                           # Ya recibi este EOF
                    nack()                                      # Devuelvo mensaje a la cola ? (requeue=True) 
            elif len(fields) == 1:                              # EOF del cliente
                self._process_eof(*fields, [])
                ack()
            else:
                logging.error(f"Error del protocolo: se recibieron {fields}")
                nack()
        except Exception as e:
            logging.error(f"Error general: {e}")
            nack()
            raise e
        
    def start(self):
        self.input_queue.start_consuming(self.process_data_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
