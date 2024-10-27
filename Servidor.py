from concurrent import futures
import grpc
import orders_pb2
import orders_pb2_grpc
from kafka import KafkaProducer
from elasticsearch import Elasticsearch
import json
import time
import random


KAFKA_BROKER = ['kafka1:9092', 'kafka2:9093', 'kafka3:9094']

producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
consumer = KafkaConsumer(TOPIC, bootstrap_servers=KAFKA_BROKER)

TOPIC = 'order_updates'
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER
)



# Conectar a Elasticsearch especificando el esquema
es = Elasticsearch([{'scheme': 'http', 'host': 'elasticsearch', 'port': 9200}])

class OrderStateMachine:
    STATES = ['Procesando', 'Preparación', 'Enviado', 'Entregado', 'Finalizado']

    def __init__(self, order_id):
        self.order_id = order_id
        self.state_index = 0

    def next_state(self):
        if self.state_index < len(self.STATES) - 1:
            self.state_index += 1
            return self.STATES[self.state_index]
        else:
            return self.STATES[-1]

    def get_current_state(self):
        return self.STATES[self.state_index]

class OrderManagementServicer(orders_pb2_grpc.OrderManagementServicer):
    def __init__(self):
        self.orders = {}

    def CreateOrder(self, request, context):
        order_id = f"ORD{random.randint(1000, 9999)}"
        state_machine = OrderStateMachine(order_id)
        self.orders[order_id] = {'state_machine': state_machine, 'email': request.email}
        initial_state = state_machine.get_current_state()
        self.publish_kafka_message(order_id, initial_state, request.email)
        self.log_metrics(order_id, initial_state, "Pedido recibido")
        return orders_pb2.OrderResponse(status="Order Created", order_id=order_id)

    def update_order_state(self, order_id):
        if order_id in self.orders:
            state_machine = self.orders[order_id]['state_machine']
            new_state = state_machine.next_state()
            email = self.orders[order_id]['email']
            self.publish_kafka_message(order_id, new_state, email)
            self.log_metrics(order_id, new_state, f"Estado actualizado a {new_state}")
            return new_state
        else:
            raise ValueError(f"Pedido {order_id} no encontrado")

    def publish_kafka_message(self, order_id, state, email):
        message = {
            "order_id": order_id,
            "state": state,
            "email": email,
            "timestamp": time.time()
        }
        producer.send(TOPIC, message)
        print(f"Publicado en Kafka: {message}")

    def log_metrics(self, order_id, state, description):
        document = {
            "order_id": order_id,
            "state": state,
            "description": description,
            "timestamp": time.time()
        }
        es.index(index="order_metrics", body=document)
        print(f"Registrado en ElasticSearch: {document}")

    def simulate_processing(self, order_id):
        try:
            while True:
                current_state = self.update_order_state(order_id)
                if current_state == "Finalizado":
                    break
                time.sleep(random.uniform(1, 3))
        except ValueError as e:
            print(e)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    orders_pb2_grpc.add_OrderManagementServicer_to_server(OrderManagementServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Servidor gRPC en ejecución (puerto 50051)")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()

