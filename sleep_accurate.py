import time
import datetime


def func(t_s):
    while True:
        t = time.time()
        time.sleep(t_s - (t % t_s))
        print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'))


if __name__ == '__main__':
    t_sleep = 0.5 # 休眠时间（秒）
    func(t_sleep)
