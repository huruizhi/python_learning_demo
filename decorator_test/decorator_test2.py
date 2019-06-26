import time
import random
# 带参数的装饰器


def get_exec_time(func):
    def wrapper(a, b):
        begin_time = time.time()
        func(a, b)
        end_time = time.time()
        use_time = end_time-begin_time
        print(use_time)
    return wrapper


@get_exec_time
def func1(a, b):
    sleep_time = random.randint(a, b)
    print(sleep_time)
    time.sleep(sleep_time)


func1(3, 4)
