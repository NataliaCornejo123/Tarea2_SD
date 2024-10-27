from kafka import KafkaConsumer
import smtplib
import json

KAFKA_BROKER = ['kafka1:9092', 'kafka2:9093', 'kafka3:9094']
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
consumer = KafkaConsumer(TOPIC, bootstrap_servers=KAFKA_BROKER)

TOPIC = 'order_updates'
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587
EMAIL_ADDRESS = 'sdtarea23@gmail.com'  
EMAIL_PASSWORD = 'TareaSD2024-2!'  

def send_email(to_address, subject, message):
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        email_message = f'Subject: {subject}\n\n{message}'
        server.sendmail(EMAIL_ADDRESS, to_address, email_message)
        print(f'Correo enviado a {to_address} con asunto {subject}')

def run_consumer():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    print("Escuchando eventos de Kafka para notificaciones...")

    for message in consumer:
        event = message.value
        order_id = event['order_id']
        state = event['state']
        email = event['email']
        timestamp = event['timestamp']
        subject = f'Actualización de estado del pedido {order_id}'
        message = f'Su pedido con ID {order_id} ahora está en el estado: {state}.\nTimestamp: {timestamp}'
        send_email(email, subject, message)

if __name__ == '__main__':
    run_consumer()

