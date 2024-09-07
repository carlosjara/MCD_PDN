from fastapi import FastAPI, BackgroundTasks
from kafka_producer import send_to_kafka
from faker import Faker
import random
import numpy as np
import asyncio


# Inicializar FastAPI y Faker
app = FastAPI()
fake = Faker()


# Función para generar datos falsos de transacciones fraudulentas
def generate_fraudulent_data():
    transaction = {
        "time": random.uniform(0, 172792),
        "amount": random.uniform(500, 5000),
        "V1": np.random.normal(-5, 2),
        "V2": np.random.normal(5, 2),
        "V3": np.random.normal(-5, 2),
        "V4": np.random.normal(5, 2),
        "V5": np.random.normal(-5, 2),
        "V6": np.random.normal(5, 2),
        "V7": np.random.normal(-5, 2),
        "V8": np.random.normal(5, 2),
        "V9": np.random.normal(-5, 2),
        "V10": np.random.normal(5, 2),
        "V11": np.random.normal(-5, 2),
        "V12": np.random.normal(5, 2),
        "V13": np.random.normal(-5, 2),
        "V14": np.random.normal(5, 2),
        "V15": np.random.normal(-5, 2),
        "V16": np.random.normal(5, 2),
        "V17": np.random.normal(-5, 2),
        "V18": np.random.normal(5, 2),
        "V19": np.random.normal(-5, 2),
        "V20": np.random.normal(5, 2),
        "V21": np.random.normal(-5, 2),
        "V22": np.random.normal(5, 2),
        "V23": np.random.normal(-5, 2),
        "V24": np.random.normal(5, 2),
        "V25": np.random.normal(-5, 2),
        "V26": np.random.normal(5, 2),
        "V27": np.random.normal(-5, 2),
        "V28": np.random.normal(5, 2),
    }
    return transaction


# Tarea de fondo para producir mensajes cada 5 segundos durante 10 minutos
async def produce_transactions_for_10_minutes():
    for _ in range(120):
        transaction = generate_fraudulent_data()
        send_to_kafka(transaction)
        await asyncio.sleep(5)  # Esperar 5 segundos entre cada transacción


# Endpoint para iniciar la tarea de producción de mensajes
@app.get("/start-producing")
async def start_producing_transactions(background_tasks: BackgroundTasks):
    background_tasks.add_task(produce_transactions_for_10_minutes)
    return {"message": "Producción de transacciones iniciada. Se enviarán mensajes cada 5 segundos durante 10 minutos."}
