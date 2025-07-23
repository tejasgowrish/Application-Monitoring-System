import json
import time
import uuid
import random
from datetime import datetime
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from kafka import KafkaProducer

app = FastAPI(title="Monitoring Demo API")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Kafka producer configuration
try:
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda v: json.dumps(v).encode('utf-8'),
        # Add these settings for more robust connection
        api_version=(2, 0, 2),  # Try a specific version
        reconnect_backoff_ms=1000,
        reconnect_backoff_max_ms=10000,
        request_timeout_ms=30000
    )
    kafka_connected = True
    print("Successfully connected to Kafka")
except Exception as e:
    print(f"Failed to connect to Kafka: {e}")
    kafka_connected = False

# Sample data for demonstration
todos = [
    {"id": 1, "title": "Buy groceries", "completed": False},
    {"id": 2, "title": "Read a book", "completed": True},
    {"id": 3, "title": "Go for a walk", "completed": False},
]

users = [
    {"id": 1, "name": "Alice Smith", "email": "alice@example.com"},
    {"id": 2, "name": "Bob Jones", "email": "bob@example.com"},
    {"id": 3, "name": "Charlie Brown", "email": "charlie@example.com"},
]

products = [
    {"id": 1, "name": "Laptop", "price": 999.99, "in_stock": True},
    {"id": 2, "name": "Smartphone", "price": 499.99, "in_stock": True},
    {"id": 3, "name": "Headphones", "price": 99.99, "in_stock": False},
]

# Models
class Todo(BaseModel):
    id: int
    title: str
    completed: bool

class User(BaseModel):
    id: int
    name: str
    email: str

class Product(BaseModel):
    id: int
    name: str
    price: float
    in_stock: bool

class CreateTodo(BaseModel):
    title: str
    completed: bool = False

class CreateUser(BaseModel):
    name: str
    email: str

class CreateProduct(BaseModel):
    name: str
    price: float
    in_stock: bool = True

# Middleware to log requests and responses
@app.middleware("http")
async def log_requests(request: Request, call_next):
    request_id = str(uuid.uuid4())
    start_time = time.time()
    path = request.url.path
    method = request.method
    
    # Process request
    try:
        response = await call_next(request)
        status_code = response.status_code
        success = status_code < 400
    except Exception as e:
        status_code = 500
        success = False
        response = JSONResponse(
            status_code=status_code,
            content={"detail": str(e)},
        )

    # Calculate processing time
    process_time = time.time() - start_time
    
    # Randomly introduce delays and errors for demonstration
    if random.random() < 0.1:  # 10% chance of delay
        process_time += random.uniform(0.5, 2.0)
    
    # Log data
    log_data = {
        "timestamp": datetime.now().isoformat(),
        "request_id": request_id,
        "method": method,
        "path": path,
        "status_code": status_code,
        "success": success,
        "response_time": process_time,
        "client_ip": request.client.host if request.client else None,
    }

    print(f"Request log: {json.dumps(log_data)}")
    
    # Send log to Kafka if connected
    if kafka_connected:
        try:
            producer.send(
                topic="api-logs",
                key={"request_id": request_id},
                value=log_data
            )
        except Exception as e:
            print(f"Failed to send log to Kafka: {e}")
    
    return response

# API Endpoints
@app.get("/")
async def root():
    return {"message": "Welcome to the Monitoring Demo API"}

# TODOs Endpoints
@app.get("/todos", response_model=List[Todo])
async def get_todos():
    return todos

@app.get("/todos/{todo_id}", response_model=Todo)
async def get_todo(todo_id: int):
    for todo in todos:
        if todo["id"] == todo_id:
            return todo
    raise HTTPException(status_code=404, detail="Todo not found")

@app.post("/todos", response_model=Todo, status_code=status.HTTP_201_CREATED)
async def create_todo(todo: CreateTodo):
    new_id = max(t["id"] for t in todos) + 1 if todos else 1
    new_todo = {"id": new_id, "title": todo.title, "completed": todo.completed}
    todos.append(new_todo)
    return new_todo

@app.put("/todos/{todo_id}", response_model=Todo)
async def update_todo(todo_id: int, todo: Todo):
    for i, t in enumerate(todos):
        if t["id"] == todo_id:
            todos[i] = {"id": todo_id, "title": todo.title, "completed": todo.completed}
            return todos[i]
    raise HTTPException(status_code=404, detail="Todo not found")

@app.delete("/todos/{todo_id}")
async def delete_todo(todo_id: int):
    for i, t in enumerate(todos):
        if t["id"] == todo_id:
            del todos[i]
            return {"message": "Todo deleted successfully"}
    raise HTTPException(status_code=404, detail="Todo not found")

# Users Endpoints
@app.get("/users", response_model=List[User])
async def get_users():
    return users

@app.get("/users/{user_id}", response_model=User)
async def get_user(user_id: int):
    for user in users:
        if user["id"] == user_id:
            return user
    raise HTTPException(status_code=404, detail="User not found")

@app.post("/users", response_model=User, status_code=status.HTTP_201_CREATED)
async def create_user(user: CreateUser):
    new_id = max(u["id"] for u in users) + 1 if users else 1
    new_user = {"id": new_id, "name": user.name, "email": user.email}
    users.append(new_user)
    return new_user

# Products Endpoints
@app.get("/products", response_model=List[Product])
async def get_products():
    return products

@app.get("/products/{product_id}", response_model=Product)
async def get_product(product_id: int):
    for product in products:
        if product["id"] == product_id:
            return product
    raise HTTPException(status_code=404, detail="Product not found")

@app.post("/products", response_model=Product, status_code=status.HTTP_201_CREATED)
async def create_product(product: CreateProduct):
    new_id = max(p["id"] for p in products) + 1 if products else 1
    new_product = {"id": new_id, "name": product.name, "price": product.price, "in_stock": product.in_stock}
    products.append(new_product)
    return new_product

# Error simulation endpoint
@app.get("/simulate-error")
async def simulate_error():
    error_types = ["database_connection", "timeout", "validation", "authentication", "authorization"]
    error_type = random.choice(error_types)
    
    error_log = {
        "timestamp": datetime.now().isoformat(),
        "error_type": error_type,
        "message": f"Simulated error: {error_type}",
        "severity": random.choice(["warning", "error", "critical"]),
    }
    
    # Send error log to Kafka if connected
    if kafka_connected:
        try:
            producer.send(
                topic="error-logs",
                value=error_log
            )
        except Exception as e:
            print(f"Failed to send error log to Kafka: {e}")
    
    raise HTTPException(
        status_code=random.choice([400, 401, 403, 500, 503]),
        detail=error_log["message"]
    )

# Health check endpoint
@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "kafka_connected": kafka_connected,
        "timestamp": datetime.now().isoformat()
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)