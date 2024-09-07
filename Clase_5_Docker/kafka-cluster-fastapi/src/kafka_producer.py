from kafka import KafkaProducer
import json
import time
from kafka.errors import NoBrokersAvailable

# Función para inicializar el productor de Kafka con reintentos
def init_kafka_producer(retries=5, delay=5):
    producer = None
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=["kafka:9092"],
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print("Conexión exitosa a Kafka")
            break
        except NoBrokersAvailable:
            print(f"Intento {i+1}/{retries}: Kafka no está disponible, reintentando en {delay} segundos...")
            time.sleep(delay)
    
    if producer is None:
        raise Exception("No se pudo conectar a Kafka después de varios intentos")
    
    return producer

# Inicializar el productor con reintentos
producer = init_kafka_producer()

# Función para enviar los datos a Kafka
def send_to_kafka(transaction):
    try:
        producer.send("fraud_transactions", value=transaction)
        producer.flush()
        print("Transacción enviada con éxito a Kafka")
    except Exception as e:
        print(f"Error al enviar la transacción: {e}")