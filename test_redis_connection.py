import os
from redis import Redis
import time

redis_host = os.getenv('REDIS_HOST', 'redis_broker')
redis_port = int(os.getenv('REDIS_PORT', 6379))

print(f"Attempting to connect to Redis at {redis_host}:{redis_port}")

try:
    r = Redis(host=redis_host, port=redis_port, socket_connect_timeout=5)
    r.ping()
    print(f"Successfully connected to Redis at {redis_host}:{redis_port}")
except Exception as e:
    print(f"Failed to connect to Redis at {redis_host}:{redis_port}: {e}")

# Keep the container alive for inspection
while True:
    time.sleep(1)
