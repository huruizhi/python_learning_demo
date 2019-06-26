import redis
import pymysql
import pandas as pd


class SqlRedis:

    def sql_to_redis(self, time_string_start, time_string_end):
        rds_pool = redis.ConnectionPool(host='192.168.0.155', port=6379, password='my_redis')
        redis_conn = redis.Redis(connection_pool=rds_pool)
        pipe = redis_conn.pipeline(transaction=True)
        print(time_string_start, "--", time_string_end)
        chabu = self.read_sql(time_string_start, time_string_end)
        for index in chabu.index:
            list_a = chabu.loc[index]
            self.to_redis(pipe, *list_a)
        pipe.execute()
        print(time_string_start, "--", time_string_end, " -----end")

    @staticmethod
    def read_sql(begin, end):
        mysql_conn = pymysql.connect("192.168.0.151", "pycf", "1qaz@WSXabc", "py_fund")
        sql_string = "select concat(fund_code,'_',trading_date) as key_value,nav,accumulative_nav,adjusted_nav," \
                    "accumulative_nav_growth," \
                    "adjusted_nav_growth,nav_adjust_factor from py_fund_non_monetary_net_trading_chabu " \
                    "where  trading_date>='%s' and trading_date<'%s' " % (begin, end)
        chabu = pd.read_sql(sql_string, mysql_conn)
        mysql_conn.close()
        return chabu

    @staticmethod
    def to_redis(pipe, key_value, nav, accumulative_nav, adjusted_nav, accumulative_nav_growth, adjusted_nav_growth,
                 nav_adjust_factor):
        pipe.zadd("py_fund_non_monetary_net_trading_chabu|nav", key_value, nav)
        pipe.zadd("py_fund_non_monetary_net_trading_chabu|accumulative_nav", key_value, accumulative_nav)
        pipe.zadd("py_fund_non_monetary_net_trading_chabu|adjusted_nav", key_value, adjusted_nav)
        pipe.zadd("py_fund_non_monetary_net_trading_chabu|accumulative_nav_growth", key_value,
                  accumulative_nav_growth)
        pipe.zadd("py_fund_non_monetary_net_trading_chabu|adjusted_nav_growth", key_value,
                  adjusted_nav_growth)
        pipe.zadd("py_fund_non_monetary_net_trading_chabu|nav_adjust_factor", key_value, nav_adjust_factor)


