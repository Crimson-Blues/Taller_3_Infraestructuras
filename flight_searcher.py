import threading
import queue
import time
import random

# Raw mock flight catalog
RAW_FLIGHTS = [
    {"flight_id": "DL101", "airline": "Delta", "price": 320, "stops": 0},
    {"flight_id": "DL202", "airline": "Delta", "price": 280, "stops": 1},
    {"flight_id": "UA303", "airline": "United", "price": 410, "stops": 0},
    {"flight_id": "UA404", "airline": "United", "price": 250, "stops": 1},
    {"flight_id": "AA505", "airline": "American", "price": 300, "stops": 0},
    {"flight_id": "AA606", "airline": "American", "price": 220, "stops": 2},
    {"flight_id": "JB707", "airline": "JetBlue", "price": 290, "stops": 0},
    {"flight_id": "JB808", "airline": "JetBlue", "price": 190, "stops": 1},
]

# --- MAP STAGE ---
def map_worker(flight_chunk: list, results_queue: queue.Queue):
    """
    Map Worker Thread: Process a slice of flights.
    Finds the cheapest flight per airline within its assigned chunk.
    """
    # Simulate network/I/O delay per thread
    time.sleep(random.uniform(0.1, 0.5))
    
    local_cheapest = {}
    for flight in flight_chunk:
        airline = flight["airline"]
        price = flight["price"]
        
        # Local map aggregation
        if airline not in local_cheapest or price < local_cheapest[airline]["price"]:
            local_cheapest[airline] = flight

    # Push worker's intermediate results onto thread-safe queue
    results_queue.put(local_cheapest)

# --- REDUCE STAGE ---
def reduce_cheapest_by_airline(intermediate_results: list[dict]) -> dict:
    """
    Reducer: Merges intermediate dictionaries from all map threads 
    to find the overall cheapest flight for each airline.
    """
    final_cheapest = {}
    
    for partial_result in intermediate_results:
        for airline, flight in partial_result.items():
            if airline not in final_cheapest or flight["price"] < final_cheapest[airline]["price"]:
                final_cheapest[airline] = flight
                
    return final_cheapest

# --- MAIN CONTROLLER ---
def run_flight_mapreduce(num_threads: int = 3):
    results_queue = queue.Queue()
    threads = []

    # 1. Split workload into chunks for threads
    chunk_size = (len(RAW_FLIGHTS) + num_threads - 1) // num_threads
    chunks = [RAW_FLIGHTS[i:i + chunk_size] for i in range(0, len(RAW_FLIGHTS), chunk_size)]

    # 2. Spawn and start Map threads
    print(f"--- Starting {len(chunks)} Map Threads ---")
    for i, chunk in enumerate(chunks):
        t = threading.Thread(target=map_worker, args=(chunk, results_queue), name=f"MapThread-{i+1}")
        threads.append(t)
        t.start()

    # 3. Synchronize (Barrier / Join)
    for t in threads:
        t.join()

    # 4. Gather intermediate results from queue
    intermediate_results = []
    while not results_queue.empty():
        intermediate_results.append(results_queue.get())

    # 5. Run Reducer
    print("--- Running Reducer ---")
    final_summary = reduce_cheapest_by_airline(intermediate_results)
    
    return final_summary

if __name__ == "__main__":
    summary = run_flight_mapreduce(num_threads=3)
    
    print("\nCheapest Flight Per Airline:")
    for airline, flight in summary.items():
        print(f"  • {airline}: {flight['flight_id']} @ ${flight['price']} ({flight['stops']} stops)")