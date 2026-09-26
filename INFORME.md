# Coordinación de instancias de Sum 

Cuando un Sum recibe el mensaje de EOF del cliente procede a enviar las frutas al aggregator correspondiente (afinidad cliente-fruta-aggregator) y a encolar un nuevo mensaje de EOF que contiene id del cliente y una lista con su ID de Sum. Los proximos Sum que reciban el EOF harán lo mismo, pero agregaran su ID al final de la lista (simil Token-Ring). Cuando todos los nodos hayan recibido el EOF se envia un broadcast a los aggregator para que disparen la consolidación y envío de tops parciales.

```python
    sums_already_red.append(ID)  
    if len(sums_already_red) < SUM_AMOUNT:   # Quedan Instancias de Sum sin recibir este EOF
        self.input_queue.send(message_protocol.internal.serialize([client,sums_already_red]))
    else:                               # Ultimo Sum. Envio EOF a los Aggregators 
        logging.info(f"Broadcasting EOF message {client}")            
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client]))

    self.amount_by_client_fruit.pop(client)
```

Nota: En caso de que un Sum reciba un EOF repetido, simplemente procede a hacer nack() tal que este mensaje sea reencolado para eventualmente ser consumido por los Sums restantes. 

```python
    if not ID in fields[1]:          
        self._process_eof(*fields)
        ack()                                      
    else:                            # Ya recibi el eof (estoy en la lista de Sums)
        nack()                       # Devuelvo mensaje a la cola (requeue=True) 
```

**Garantías** Se enviarán solo #SUM_AMOUNT EOFs por cliente. Se envía un EOF a cada aggregator luego de que **TODOS los Sum** hayan recibido el EOF. 

**Limitaciones:** Para grupos de una gran cantidad de nodos se tendrá un overhead considerable de NACKs. Este problema podría mitigarse con una estructura de Token-Ring, pero dicha solución implicaría otras complicaciones (condiciones de carrera entre el hilo consumidor de cola y el hilo gestor del token ring, sincronización de dichos hilos, etc) que acabarían resultando en un sistema con mayor complejidad e inclusive quiza con una performance inferior para el escenario del trabajo debido al overhead de sincronización. Dado el alcance del trabajo, la decisión final fue la solución más sencilla y sin overhead de sincronización.    

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

