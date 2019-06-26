import redis
import multiprocessing
import time


def getvalue1():
    conn = redis.Redis(host='192.168.0.155', port=6379, password='my_redis')
    dic1 = conn.zrangebyscore("py_fund_non_monetary_net_trading_chabu|nav", 1.0, 1.01)
    print(len(dic1))


def getvalue2():
    conn = redis.Redis(host='192.168.0.155', port=6379, password='my_redis')
    dic1 = conn.zrangebyscore("py_fund_non_monetary_net_trading_chabu|adjusted_nav", 1.0, 1.01)
    print(len(dic1))


if __name__=='__main__':
    tb = time.time()
    m = multiprocessing.Manager()

    multiprocessing.freeze_support()
    tb = time.time()
    t1 = multiprocessing.Process(target=getvalue1)
    t2 = multiprocessing.Process(target=getvalue2)

    t1.start()
    t2.start()

    t1.join()
    t2.join()
    te1 = time.time()
    print(te1 - tb)
