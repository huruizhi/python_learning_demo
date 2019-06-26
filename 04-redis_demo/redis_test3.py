import  redis

pool = redis.ConnectionPool(host='192.168.0.169', port=6379, password='my_redis')
conn = redis.Redis(connection_pool=pool)
conn.set("uv1", "huruizhi")
conn.set("uv1", "dengl")
users = conn.get("uv1")
print(users)
