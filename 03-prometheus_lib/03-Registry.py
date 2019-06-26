import random
from prometheus_client import Counter, Gauge
import prometheus_client
from prometheus_client.core import CollectorRegistry
from flask import Response, Flask

app = Flask(__name__)

REGISTRY = CollectorRegistry(auto_describe=False)

requests_total = Counter("request_count", "Total request cout of the host", registry=REGISTRY)
random_value = Gauge("random_value", "Random value of the request", registry=REGISTRY)


@app.route("/metrics")
def ApiResponse():
    requests_total.inc()
    random_value.set(random.randint(-10, 10))
    return Response(prometheus_client.generate_latest(REGISTRY),mimetype="text/plain")


@app.route('/')
def index():
    requests_total.inc()
    return "Hello World"


if __name__ == "__main__":
    app.run(host="0.0.0.0")
