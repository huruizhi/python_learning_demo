from redis_test import mysql_to_redis
import multiprocessing

pool = multiprocessing.Pool(processes=3)


def main():
    year_s = 2016
    month_s = 1
    time_string_start = '%i-%02i-01' % (year_s, month_s)
    while True:
        global pool
        month_e = month_s+1
        if month_e == 13:
            year_e = year_s+1
            month_e = 1
        else:
            year_e = year_s
        time_string_end = '%i-%02i-01' % (year_e, month_e)
        sr = mysql_to_redis.SqlRedis()
        pool.apply_async(sr.sql_to_redis, (time_string_start, time_string_end))
        if year_e >= 2017 and month_e > 11:
            break
        else:
            month_s = month_e
            year_s = year_e
            time_string_start = time_string_end


if __name__ == '__main__':
    main()
    pool.close()
    pool.join()

    print('done')

