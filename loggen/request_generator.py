import requests
import random
import time
import json
from datetime import datetime
import threading

# API Server URL
API_BASE_URL = "http://api:8000"

# Endpoints to simulate requests to
ENDPOINTS = [
    {"path": "/", "method": "GET"},
    {"path": "/todos", "method": "GET"},
    {"path": "/users", "method": "GET"},
    {"path": "/products", "method": "GET"},
    {"path": "/todos/1", "method": "GET"},
    {"path": "/users/2", "method": "GET"},
    {"path": "/products/3", "method": "GET"},
    {"path": "/todos", "method": "POST", "payload": {"title": "New task", "completed": False}},
    {"path": "/users", "method": "POST", "payload": {"name": "New User", "email": "new@example.com"}},
    {"path": "/products", "method": "POST", "payload": {"name": "New Product", "price": 19.99, "in_stock": True}},
    {"path": "/todos/1", "method": "PUT", "payload": {"id": 1, "title": "Updated task", "completed": True}},
    {"path": "/simulate-error", "method": "GET"}
]

def send_request(endpoint):
    """Send a request to the specified endpoint"""
    url = f"{API_BASE_URL}{endpoint['path']}"
    method = endpoint['method']
    payload = endpoint.get('payload', None)
    
    try:
        if method == "GET":
            response = requests.get(url, timeout=5)
        elif method == "POST":
            response = requests.post(url, json=payload, timeout=5)
        elif method == "PUT":
            response = requests.put(url, json=payload, timeout=5)
        elif method == "DELETE":
            response = requests.delete(url, timeout=5)
        
        print(f"{datetime.now().isoformat()} - {method} {url} - Status: {response.status_code}")
        return response.status_code
    except requests.exceptions.RequestException as e:
        print(f"{datetime.now().isoformat()} - {method} {url} - Error: {str(e)}")
        return None

def simulate_traffic(intensity="medium"):
    """Simulate API traffic with specified intensity"""
    if intensity == "low":
        request_count = random.randint(5, 10)
        delay_range = (1.0, 3.0)
    elif intensity == "medium":
        request_count = random.randint(10, 30)
        delay_range = (0.5, 2.0)
    elif intensity == "high":
        request_count = random.randint(30, 50)
        delay_range = (0.1, 1.0)
    else:  # burst
        request_count = random.randint(50, 100)
        delay_range = (0.05, 0.5)
    
    print(f"Starting traffic simulation: {request_count} requests with {intensity} intensity")
    
    for _ in range(request_count):
        endpoint = random.choice(ENDPOINTS)
        send_request(endpoint)
        
        delay = random.uniform(*delay_range)
        time.sleep(delay)

def run_continuous_simulation():
    """Run continuous simulation with varying intensities"""
    while True:
        # Randomly select traffic intensity with some weighting
        intensity = random.choices(
            ["low", "medium", "high", "burst"],
            weights=[0.2, 0.5, 0.2, 0.1],
            k=1
        )[0]
        
        simulate_traffic(intensity)
        
        # Wait between simulation batches
        time.sleep(random.uniform(5, 15))

def run_scheduled_simulation():
    """Run simulations on a schedule to create realistic traffic patterns"""
    while True:
        current_hour = datetime.now().hour
        
        # Simulate "business hours" with higher traffic during the day
        if 9 <= current_hour < 18:  # 9 AM to 6 PM
            intensity = random.choices(
                ["medium", "high", "burst"],
                weights=[0.6, 0.3, 0.1],
                k=1
            )[0]
        else:  # "off hours" with lower traffic
            intensity = random.choices(
                ["low", "medium", "high"],
                weights=[0.7, 0.2, 0.1],
                k=1
            )[0]
        
        simulate_traffic(intensity)
        time.sleep(random.uniform(10, 20))

if __name__ == "__main__":
    print("Starting API request generator...")
    
    # Wait for API to be ready
    max_retries = 30
    retries = 0
    api_ready = False
    
    while not api_ready and retries < max_retries:
        try:
            response = requests.get(f"{API_BASE_URL}/health", timeout=2)
            if response.status_code == 200:
                api_ready = True
                print("API is ready. Starting traffic simulation...")
            else:
                retries += 1
                print(f"API not ready yet. Status code: {response.status_code}. Retrying in 5 seconds...")
                time.sleep(5)
        except requests.exceptions.RequestException:
            retries += 1
            print(f"Could not connect to API. Retrying in 5 seconds... ({retries}/{max_retries})")
            time.sleep(5)
    
    if not api_ready:
        print("Could not connect to API after maximum retries. Exiting.")
        exit(1)
    
    # Start simulation threads
    continuous_thread = threading.Thread(target=run_continuous_simulation)
    scheduled_thread = threading.Thread(target=run_scheduled_simulation)
    
    continuous_thread.daemon = True
    scheduled_thread.daemon = True
    
    continuous_thread.start()
    scheduled_thread.start()
    
    # Keep the main thread running
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("Stopping simulation...")