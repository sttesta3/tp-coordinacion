# Coordinación de instancias de Sum 

Cuando uno de los sum recibe el EOF del cliente, procede a enviar la suma de frutas a los aggregators y a encolar un nuevo mensaje con el id de cliente y la cantidad de sums que recibieron el EOF (explícitado en siguiente código). 

```python
    sums_already_red += 1  
    if sums_already_red < SUM_AMOUNT:   # Quedan Instancias de Sum sin recibir este EOF
        self.input_queue.send(message_protocol.internal.serialize([client,sums_already_red]))
    else:                               # Ultimo Sum. Envio EOF a los Aggregators 
        logging.info(f"Broadcasting EOF message {client}")            
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client]))

    self.amount_by_client_fruit.pop(client)
```

Si un SUM vuelve a recibir el EOF de un cliente que ya recibió (es decir, de un cliente que no está en self.amount_by_client_fruit), realizará nack() del mensaje tal sea reencolado para que los demas Sums lo reciban.  

**Garantías** Se enviarán solo #SUM_AMOUNT EOFs por cliente. Se envia un EOF a cada aggregator luego de que **TODOS los Sum** hayan recibido el EOF. 

**Limitaciones:** Si una instancia de Sum recibe un EOF de un cliente del cual nunca recibió frutas, este procedera a hacer nack() del mensaje, entrando en un loop infinito y nunca devolviendo el resultado al cliente. La probabilidad de ocurrencia de este escenario se relaciona con la proporcion entre cantidad de SUMs y tamaño del archivo a procesar (si el archivo es muy chico o hay una gran cantidad de instancias de Sum). 

Debido a que tenemos tres SUMs y archivos de 300-400 líneas esta limitación se considera aceptable, ya que mitigar esto implicaría mantener una estructura adicional para que los SUMs tengan memoria de los clientes que recibieron mensajes, o un protocolo de comunicacion con mayor complejidad.  

# Coordinación de instancias de Aggregation 

Los aggregators recibiran las frutas segun el nombre de la fruta y el id del cliente. Esto genera afinidad (fruta,cliente)-aggregator, resultando en que todos los sums envian cierta fruta de cierto cliente al mismo aggregator. 

```python
    aggregator = ( sum(ord(char) for char in final_fruit_item.fruit) + sum(ord(char) for char in str(client)) ) % AGGREGATION_AMOUNT
    self.data_output_exchanges[aggregator].send(
        message_protocol.internal.serialize(
            [client, final_fruit_item.fruit, final_fruit_item.amount]
        )
    )
```

Se tomo esta decisión con el objetivo de distribuir la carga entre los distintos aggregator, independientemente de la cantidad de clientes o de frutas involucradas.

Al recibir el eof, envian su top al join y limpian la metadata del cliente.

# Lenguaje Elegido

Python

