#Nombre: flight_searcher
#Autores:
# - Juan Diego Cardenas Mejia - 2416427
# - Samuel Banguero Ortega - 2418671
#Fecha de Creación: 27/09/2026
#Descripción: Clase que emplea la técnica de programación concurrente MapReduce para la búsqueda
# del vuelo óptimo al interior de un conjunto de vuelos.
#Curso: Infraestructuras Paralelas y Distribuidas
#Código: 750023C

import threading
import queue
import time
import random
from collections import defaultdict

# Dataset inventado de datos
# Trayecto: Cali - Cartagena
RAW_FLIGHTS = [
    {"flight_id": "JA5301", "airline": "JetSmart", "price": 559598, "stops": 1},
    {"flight_id": "JA5565", "airline": "JetSmart", "price": 436050, "stops": 1},
    {"flight_id": "JA5533", "airline": "JetSmart", "price": 302270, "stops": 0},
    {"flight_id": "JA5531", "airline": "JetSmart", "price": 274791, "stops": 0},
    {"flight_id": "LA4356", "airline": "Latam", "price": 1567000, "stops": 1},
    {"flight_id": "LA4356", "airline": "Latam", "price": 1567000, "stops": 1},
    {"flight_id": "LA4357", "airline": "Latam", "price": 1875000, "stops": 1},
    {"flight_id": "LA4060", "airline": "Latam", "price": 689430, "stops": 1},
    {"flight_id": "LA4062", "airline": "Latam", "price": 781890, "stops": 1},
    {"flight_id": "LA4163", "airline": "Latam", "price": 450574, "stops": 1},
    {"flight_id": "AV9366", "airline": "Avianca", "price": 349241, "stops": 1},
    {"flight_id": "AV9368", "airline": "Avianca", "price": 433241, "stops": 1},
    {"flight_id": "AV9369", "airline": "Avianca", "price": 322513, "stops": 1},
    {"flight_id": "AV9260", "airline": "Avianca", "price": 374588, "stops": 0},
    {"flight_id": "AV9269", "airline": "Avianca", "price": 360588, "stops": 0},
    {"flight_id": "AV9265", "airline": "Avianca", "price": 347861, "stops": 1}
]

random.shuffle(RAW_FLIGHTS) # Mezcla valores para tener un dataset realista

# --- Fase de mapeo ---
def map_worker(flight_chunk, results_queue):
    """
    Mapper: Obtiene una porción de los vuelos y los clasifica por aerolinea.
    Coloca los resultados en una cola de resultados
    """
    # Simular delay en obtención de datos
    time.sleep(random.uniform(0.1, 0.5))

    #Default dict de listas para organizar listas de vuelos por aerolineas
    mapped_flights = defaultdict(list)

    for flight in flight_chunk:
        airline = flight["airline"]
        mapped_flights[airline].append(flight)

    # Resultado de mapeo en cola para reduce
    results_queue.put(mapped_flights)


# --- Fase de shuffle ---
def shuffle_phase(results_queue):
    """
    Shuffler: Obtiene los resultados intermedios de los mappers
    y combina los resultados por llave de aerolínea. 
    """
    grouped_by_airline = defaultdict(list)
    
    while not results_queue.empty():
        mapper_output = results_queue.get()
        for airline, flights in mapper_output.items():
            grouped_by_airline[airline].extend(flights)
            
    return grouped_by_airline  

# --- Fase de reducción ---
def reduce_worker(airline, flights, results):
    """
    Reducer: Combina los diccionarios de resultados intermedios de mappers,
    para encontrar el vuelo más barato por aerolínea
    """
    cheapest = {"flight_id" : "", "price" : float('inf')}
    for flight in flights:
        price = flight["price"]
        if price < cheapest["price"]:
            cheapest = flight
                
    results[airline] = cheapest # Guardar resultado en lista

# --- Función principal ---
def run_flight_mapreduce(RAW_FLIGHTS, num_threads: int = 3):
    results_queue = queue.Queue()
    mapper_threads = []

    # Divide el dataset original de vuelos en chunks
    chunk_size = (len(RAW_FLIGHTS) + num_threads - 1) // num_threads
    chunks = [RAW_FLIGHTS[i:i + chunk_size] for i in range(0, len(RAW_FLIGHTS), chunk_size)]

    # Crea e inicia los hilos de mapeo
    print(f"--- Starting {len(chunks)} Map Threads ---")
    for i, chunk in enumerate(chunks):
        t = threading.Thread(target=map_worker, args=(chunk, results_queue))
        mapper_threads.append(t)
        t.start()

    # Join de sincronización
    for t in mapper_threads:
        t.join()

    # Shuffle para organizar por aerolineas
    shuffled_data = shuffle_phase(results_queue)

    # Reducers
    print("--- Running Reducers ---")
    reducer_threads = []
    results = {}

    for airline in shuffled_data.keys():
        t = threading.Thread(target=reduce_worker, args=(airline, shuffled_data[airline], results))
        reducer_threads.append(t)
        t.start()

    # Join de sincronización
    for t in reducer_threads:
        t.join()

    # Obtener el vuelo más barato a nivel global
    global_cheapest = sorted(results.values(), key=lambda d: d['price'])[0]

    return results, global_cheapest

if __name__ == "__main__":
    summary, cheapest = run_flight_mapreduce(RAW_FLIGHTS)

    print("\nVuelo más barato por aerolínea")
    for airline, flight in summary.items():
        print(f"  • {airline}: {flight['flight_id']} @ ${flight['price']} ({flight['stops']} parada(s))")

    print(f"\nÓptimo global: {cheapest['airline']} {cheapest['flight_id']} @ ${cheapest['price']} ({cheapest['stops']} parada(s))")