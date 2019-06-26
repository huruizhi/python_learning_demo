import time
import random
# 不带参数的装饰器


def get_exec_time(func):
    def wrapper():
        begin_time = time.time()
        func()
        end_time = time.time()
        use_time = end_time-begin_time
        print(use_time)
    return wrapper


@get_exec_time
def func1():
    sleep_time = random.randint(1, 5)
    print(sleep_time)
    time.sleep(sleep_time)


func1()
