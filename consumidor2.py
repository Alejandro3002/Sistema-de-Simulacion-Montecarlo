# consumidor.py
import pika
import json
import os
from math import exp, sqrt, log  # Necesario para evaluar la función
import time

# --- Clase para el Consumidor Worker OO ---
class ConsumidorWorker:
    def __init__(self, worker_id, rabbitmq_host='localhost'):
        self.worker_id = worker_id
        self.host = rabbitmq_host
        self.connection = None
        self.channel = None
        self.model = None
        self.function_expr = None
        
        self._connect()
        self.declare_queues()
        print(f"[*] Worker {self.worker_id} conectado a RabbitMQ en {self.host}")

    def _connect(self):
        """Conexión al servidor RabbitMQ con usuario y contraseña."""
        try:
            credentials = pika.PlainCredentials('admin', '1234')  # <-- CREDENCIALES

            params = pika.ConnectionParameters(
                host=self.host,
                credentials=credentials,
                heartbeat=60,
                blocked_connection_timeout=300
            )

            self.connection = pika.BlockingConnection(params)
            self.channel = self.connection.channel()

        except Exception as e:
            print(f"Error al conectar con RabbitMQ: {e}")
            time.sleep(5)
            exit(1)

    def declare_queues(self):
        """Declaración de colas y QoS."""
        self.channel.queue_declare(queue='modelo_q', durable=True)
        self.channel.queue_declare(queue='escenarios_q', durable=True)
        self.channel.queue_declare(queue='resultados_q', durable=True)

        # Fair dispatch
        self.channel.basic_qos(prefetch_count=1)

    def leer_modelo(self):
        """Lee el modelo UNA sola vez desde la cola modelo_q."""
        print(f"[{self.worker_id}] Intentando leer el modelo...")
        method_frame, properties, body = self.channel.basic_get(queue='modelo_q')

        if method_frame:
            self.model = json.loads(body)
            self.function_expr = self.model.get('FUNCTION')
            print(f"[{self.worker_id}] Modelo cargado. Función: {self.function_expr}")
            self.channel.basic_ack(method_frame.delivery_tag)
            return True
        else:
            print(f"[{self.worker_id}] Sin modelo aún. Reintentando...")
            return False

    def ejecutar_modelo(self, scenario):
        """Ejecuta la función matemática del modelo."""
        if not self.function_expr:
            print(f"[{self.worker_id}] ERROR: Modelo no cargado.")
            return None

        scope = scenario.copy()
        scope.update({'exp': exp, 'sqrt': sqrt, 'log': log})

        try:
            result_val = eval(self.function_expr, {'__builtins__': None}, scope)
            return float(result_val)
        except Exception as e:
            print(f"[{self.worker_id}] Error al ejecutar modelo: {e}")
            return None

    def publicar_resultado(self, scenario_id, result_val):
        """Publica el resultado en resultados_q."""
        result_message = {
            'worker_id': self.worker_id,
            'scenario_id': scenario_id,
            'result': result_val
        }

        self.channel.basic_publish(
            exchange='',
            routing_key='resultados_q',
            body=json.dumps(result_message),
            properties=pika.BasicProperties(delivery_mode=2)
        )

        print(f"[{self.worker_id}] Resultado #{scenario_id} publicado.")

    def callback_escenario(self, ch, method, properties, body):
        """Callback cuando recibe un escenario."""
        scenario = json.loads(body)
        scenario_id = scenario.get('id')

        print(f"[{self.worker_id}] Escenario #{scenario_id} recibido.")

        result = self.ejecutar_modelo(scenario)

        if result is not None:
            self.publicar_resultado(scenario_id, result)
        else:
            print(f"[{self.worker_id}] ERROR en escenario #{scenario_id}")

        ch.basic_ack(method.delivery_tag)

    def iniciar_consumo(self):
        """Consume escenarios después de cargar modelo."""
        print(f"[{self.worker_id}] Esperando modelo...")

        while not self.leer_modelo():
            time.sleep(3)

        print(f"[{self.worker_id}] Escuchando escenarios en 'escenarios_q'...")

        self.channel.basic_consume(
            queue='escenarios_q',
            on_message_callback=self.callback_escenario,
            auto_ack=False
        )

        self.channel.start_consuming()


if __name__ == '__main__':
    WORKER_ID = os.getpid()

    worker = ConsumidorWorker(
        worker_id=WORKER_ID,
        rabbitmq_host='localhost'   # <-- TU SERVIDOR RABBITMQ
    )

    try:
        worker.iniciar_consumo()
    except KeyboardInterrupt:
        print(f"\n[{WORKER_ID}] Worker detenido.")
        if worker.connection:
            worker.connection.close()