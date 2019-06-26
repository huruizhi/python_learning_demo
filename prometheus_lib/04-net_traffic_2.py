import prometheus_client
from prometheus_client import Counter
from prometheus_client import Gauge
from prometheus_client.core import CollectorRegistry
import psutil
import time
import datetime
import requests
import socket
import threading
from flask import Response, Flask

app = Flask(__name__)


class GetNetRate:
    def __init__(self, interval_time):
        self.interval = interval_time
        self.hostname = socket.gethostname()  # 主机名
        self.registry = CollectorRegistry(auto_describe=False)  # prometheus仓库
        self.key_info = dict()
        self.old_recv = dict()
        self.old_sent = dict()

    @staticmethod
    def get_key():
        key_info = psutil.net_io_counters(pernic=True).keys()
        recv = dict()
        sent = dict()

        for k in key_info:
            recv.setdefault(k, psutil.net_io_counters(pernic=True).get(k).bytes_recv)
            sent.setdefault(k, psutil.net_io_counters(pernic=True).get(k).bytes_sent)

        return key_info, recv, sent

    def get_rate(self):
        while True:
            if not self.key_info:
                self.key_info, self.old_recv, self.old_sent = self.get_key()
            t = time.time()
            time.sleep(self.interval - (t % self.interval))
            print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'))
            self.key_info, now_recv, now_sent = self.get_key()
            net_in = dict()
            net_out = dict()

            for k in self.key_info:
                net_in.setdefault(k, float('%.2f' % ((now_recv.get(k) - self.old_recv.get(k))/(1024 * self.interval))))
                net_out.setdefault(k, float('%.2f' % ((now_sent.get(k) - self.old_sent.get(k))/(1024 * self.interval))))

            self.old_recv = now_recv
            self.old_sent = now_sent
            yield self.key_info, net_in, net_out

    @staticmethod
    def net_address():
        dic = psutil.net_if_addrs()
        net_dic = dict()
        net_dic['no_ip'] = []  # 无ip的网卡列表
        for adapter in dic:
            snicList = dic[adapter]
            mac = '无 mac 地址'
            ipv4 = '无 ipv4 地址'
            ipv6 = '无 ipv6 地址'
            for snic in snicList:
                if snic.family.name in {'AF_LINK', 'AF_PACKET'}:
                    mac = snic.address
                elif snic.family.name == 'AF_INET':
                    ipv4 = snic.address
                elif snic.family.name == 'AF_INET6':
                    ipv6 = snic.address
            # print('%s, %s, %s, %s' % (adapter, mac, ipv4, ipv6))

            # 判断网卡名不在net_dic中时,并且网卡不是lo
            if adapter not in net_dic and adapter != 'lo':
                if not ipv4.startswith("无"):  # 判断ip地址不是以无开头
                    net_dic[adapter] = ipv4  # 增加键值对
                else:
                    net_dic['no_ip'].append(adapter)  # 无ip的网卡

        return net_dic

    def insert_registry(self):
        # 流入
        net_input = Gauge("network_traffic_input", self.hostname, ['adapter_name', 'unit', 'ip', 'instance'],
                          registry=self.registry)
        # 流出
        net_output = Gauge("network_traffic_output", self.hostname, ['adapter_name', 'unit', 'ip', 'instance'],
                           registry=self.registry)

        for key_info, net_in, net_out in self.get_rate():
            for key in key_info:
                net_addr = self.net_address()
                # 判断网卡不是lo(回环网卡)以及 不是无ip的网卡
                if 'lo' not in key and key not in net_addr['no_ip'] and key in net_addr:
                    # 流入和流出
                    net_input.labels(ip=net_addr[key], adapter_name=key, unit="KByte/s", instance=self.hostname).\
                        set(net_in.get(key))
                    net_output.labels(ip=net_addr[key], adapter_name=key, unit="KByte/s", instance=self.hostname).\
                        set(net_out.get(key))

            try:
                requests.post("http://prometheus-gateway.kube-ops.svc.cluster.local:9091/metrics/job/network_traffic",
                              data=prometheus_client.generate_latest(self.registry))
                print("发送了一次网卡流量数据")
            except Exception as e:
                print(e)

    def run(self):
        t = threading.Thread(target=self.insert_registry)
        t.start()


interval = 5
o = GetNetRate(interval)
o.run()


@app.route("/metrics")
def api_response():
    return Response(prometheus_client.generate_latest(o.registry), mimetype="text/plain")


@app.route('/')
def index():
    return "Hello World"


if __name__ == '__main__':
    app.run(host="0.0.0.0")
