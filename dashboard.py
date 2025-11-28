# dashboard.py
import pika
import json
import pandas as pd
import matplotlib.pyplot as plt
import threading
import time

# --- Clase para el Visualizador OO ---
class DashboardVisualizador:
    def __init__(self, rabbitmq_host='localhost'):
        self.host = rabbitmq_host
        self.connection = None
        self.channel = None
        
        # DataFrame para almacenar los resultados
        self.results_df = pd.DataFrame(columns=['worker_id', 'scenario_id', 'result'])
        self.results_lock = threading.Lock() # Para acceso concurrente al DF
        
        self._connect()
        self.declare_queues()
        print("[*] Dashboard conectado a RabbitMQ.")

    def _connect(self):
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
        except pika.exceptions.AMQPConnectionError as e:
            print(f"Error al conectar con RabbitMQ: {e}")
            exit(1)

    def declare_queues(self):
        """Declaración de la cola de resultados."""
        self.channel.queue_declare(queue='resultados_q', durable=True)
        # El dashboard consume continuamente, no necesita QoS.

    def callback_resultado(self, ch, method, properties, body):
        """Recibe el resultado y lo añade al DataFrame."""
        result_data = json.loads(body)
        
        new_row = pd.Series(result_data)
        
        with self.results_lock:
            # Usar pd.concat para añadir la nueva fila de forma segura
            self.results_df = pd.concat([self.results_df, new_row.to_frame().T], ignore_index=True)
            
        ch.basic_ack(method.delivery_tag)

    def update_plot(self):
        """
        Actualiza el gráfico con las estadísticas actuales.
        Se ejecuta en un thread separado.
        """
        plt.ion() # Modo interactivo para actualizar el gráfico
        fig, axes = plt.subplots(1, 2, figsize=(14, 6))
        
        while True:
            with self.results_lock:
                current_df = self.results_df.copy()

            if not current_df.empty:
                axes[0].clear()
                axes[1].clear()

                # --- Gráfico 1: Histograma de Resultados ---
                axes[0].hist(current_df['result'], bins=50, color='skyblue', edgecolor='black')
                axes[0].set_title(f'Distribución de Resultados (N={len(current_df)})')
                axes[0].set_xlabel('Resultado (Z)')
                axes[0].set_ylabel('Frecuencia')
                
                # Calcular y mostrar estadísticas (Media y Desviación Estándar)
                media = current_df['result'].mean()
                std = current_df['result'].std()
                axes[0].axvline(media, color='red', linestyle='dashed', linewidth=1)
                min_ylim, max_ylim = axes[0].get_ylim()
                axes[0].text(media*1.05, max_ylim*0.9, f'Media: {media:.2f}', color='red')
                
                # --- Gráfico 2: Tasa de Producción por Worker ---
                worker_counts = current_df.groupby('worker_id').size().sort_values(ascending=False)
                worker_counts.plot(kind='bar', ax=axes[1], color='lightcoral')
                axes[1].set_title('Simulaciones Completadas por Worker')
                axes[1].set_xlabel('ID del Worker')
                axes[1].set_ylabel('Conteo de Resultados')
                axes[1].tick_params(axis='x', rotation=45)

                fig.suptitle(f'Simulación Montecarlo Distribuida - Total: {len(current_df)} Resultados')
                
            plt.tight_layout()
            plt.draw()
            plt.pause(1) # Actualizar cada 1 segundo
            
            # Condición de salida (opcional, para simulaciones finitas)
            # if len(current_df) >= 1000: break

    def start_consuming_and_plotting(self):
        """Inicia los threads de consumo y visualización."""
        
        # Hilo 1: Consumo de Mensajes (RabbitMQ)
        def start_consumer_thread():
            print("[*] Esperando resultados en 'resultados_q'.")
            self.channel.basic_consume(
                queue='resultados_q',
                on_message_callback=self.callback_resultado,
                auto_ack=False
            )
            self.channel.start_consuming()
            
        consumer_thread = threading.Thread(target=start_consumer_thread)
        consumer_thread.start()
        
        # Hilo 2: Actualización del Gráfico (Matplotlib)
        try:
            self.update_plot()
        except KeyboardInterrupt:
            print("\n[FIN] Dashboard detenido por el usuario.")
        finally:
            if self.connection:
                self.connection.close()

if __name__ == '__main__':
    # Asegúrate de cambiar 'localhost' por la IP del servidor RabbitMQ si es necesario
    dashboard = DashboardVisualizador(rabbitmq_host='localhost')
    dashboard.start_consuming_and_plotting()