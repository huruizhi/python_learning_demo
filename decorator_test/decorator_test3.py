import time
import random
# 带参数的装饰器


def get_exec_time(arg=True):
    if arg:
        def _get_exec_time(func):
            def wrapper(*args, **kwargs):
                begin_time = time.time()
                func(*args, **kwargs)
                end_time = time.time()
                use_time = end_time-begin_time
                print(use_time)
            return  wrapper
    else:
        def _get_exec_time(func):
            def wrapper(*args, **kwargs):
                func(*args, **kwargs)
            return wrapper
    return _get_exec_time


@get_exec_time(False)
def func1(a, b):
    sleep_time = random.randint(a, b)
    print(sleep_time)
    time.sleep(sleep_time)


func1(3, 4)
