import random
import prometheus_client
from prometheus_client import Gauge
from flask import Response, Flask

app = Flask(__name__)

random_value = Gauge("random_value", "Random value of the request")


@app.route("/metrics")
def r_value():
    random_value.set(random.randint(-10, 10))
    return Response(prometheus_client.generate_latest(random_value),
                    mimetype="text/plain")


if __name__ == "__main__":
    app.run(host="0.0.0.0")
