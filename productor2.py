# productor.py
import pika
import numpy as np
import json
import random
from math import exp, sqrt, log

class ProductorMontecarlo:
    def __init__(self,
                 rabbitmq_host='localhost',
                 rabbitmq_user='admin',
                 rabbitmq_pass='1234',
                 model_file='model.txt'):

        self.host = rabbitmq_host
        self.user = rabbitmq_user
        self.password = rabbitmq_pass
        self.model_file = model_file
        
        self.connection = None
        self.channel = None
        
        # Conexión usando usuario y contraseña
        self._connect()

        # Declaración de colas
        self.channel.queue_declare(queue='modelo_q', durable=True)
        self.channel.queue_declare(queue='escenarios_q', durable=True)

        print(f"[*] Conectado a RabbitMQ en {self.host} como usuario '{self.user}'")

    def _connect(self):
        """Conexión con usuario/contraseña exactamente igual que consumidor."""
        try:
            credentials = pika.PlainCredentials(self.user, self.password)
            params = pika.ConnectionParameters(
                host=self.host,
                port=5672,
                virtual_host='/',
                credentials=credentials
            )

            self.connection = pika.BlockingConnection(params)
            self.channel = self.connection.channel()

        except pika.exceptions.AMQPConnectionError as e:
            print(f"[ERROR] No se pudo conectar a RabbitMQ: {e}")
            exit(1)

    def _parse_model(self):
        model = {'FUNCTION': None, 'DISTRIBUTIONS': {}}
        try:
            with open(self.model_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line.startswith('#') or not line:
                        continue
                    if line.startswith('FUNCTION:'):
                        model['FUNCTION'] = line.split(':', 1)[1].strip()
                    else:
                        var, dist_str = line.split(':', 1)
                        model['DISTRIBUTIONS'][var.strip()] = dist_str.strip()
            return model
        except FileNotFoundError:
            print(f"[ERROR] Archivo '{self.model_file}' no encontrado.")
            exit(1)

    def publicar_modelo(self, model_data):
        self.channel.queue_delete(queue='modelo_q')
        self.channel.queue_declare(queue='modelo_q', durable=True)

        self.channel.basic_publish(
            exchange='',
            routing_key='modelo_q',
            body=json.dumps(model_data),
            properties=pika.BasicProperties(
                delivery_mode=2,
                expiration='600000'
            )
        )
        print("[->] Modelo publicado correctamente.")

    def _generate_value(self, dist_str):
        try:
            dist_name, params = dist_str.split("(", 1)
            params = params[:-1]  # remover ')'

            if dist_name == 'uniform':
                min_val = float(params.split('=')[1].split(',')[0])
                max_val = float(params.split('=')[2])
                return float(np.random.uniform(min_val, max_val))

            elif dist_name == 'normal':
                mu = float(params.split('=')[1].split(',')[0])
                sigma = float(params.split('=')[2])
                return float(np.random.normal(mu, sigma))

            elif dist_name == 'poisson':
                lambda_val = float(params.split('=')[1])
                return int(np.random.poisson(lambda_val))

            else:
                raise ValueError(f"Distribución desconocida: {dist_name}")

        except Exception as e:
            print(f"Error generando valor para '{dist_str}': {e}")
            return None

    def generar_y_publicar_escenario(self, model_data, total_escenarios):

        for i in range(total_escenarios):
            scenario = {var: self._generate_value(dist)
                        for var, dist in model_data['DISTRIBUTIONS'].items()}
            
            scenario['id'] = i + 1

            self.channel.basic_publish(
                exchange='',
                routing_key='escenarios_q',
                body=json.dumps(scenario),
                properties=pika.BasicProperties(delivery_mode=2)
            )

            print(f"[->] Escenario #{i+1} enviado")

    def iniciar_simulacion(self, n_escenarios):
        print("\n### Publicando Modelo ###")
        model_data = self._parse_model()
        self.publicar_modelo(model_data)

        print("\n### Publicando Escenarios ###")
        self.generar_y_publicar_escenario(model_data, n_escenarios)

        print(f"\n[FIN] {n_escenarios} escenarios publicados.")
        self.connection.close()


if __name__ == '__main__':
    productor = ProductorMontecarlo(
        rabbitmq_host='localhost',
        rabbitmq_user='admin',
        rabbitmq_pass='1234'
    )

    productor.iniciar_simulacion(1000)