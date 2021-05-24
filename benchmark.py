import redis_benchmarking as rb
import memcached_benchmarking as mb
from matplotlib import pyplot as plt
import time
import config

def set_maxmemory(r, m, limit):
    rb.set_maxmemory(r, str(limit)+'mb')
    mb.set_memlimit(m, limit)




def factorial_test(n):
    memory = 100
    redis_factorial_client = rb.create_server()
    redis_factorial_client.flushall()
    mem_fac = mb.memcached_connection()
    mem_fac.flush_all()
    set_maxmemory(redis_factorial_client, mem_fac, memory)
    start_r = time.process_time()
    r_f = rb.redis_factorial(redis_factorial_client, n)
    end_r = time.process_time()
    start_m = time.process_time()
    m_f = mb.memcache_factorial(mem_fac, n)
    end_m = time.process_time()
    assert r_f == m_f
    print("Redis factorial time: {}".format(end_r-start_r))
    print("memcached factorial time: {}".format(end_m-start_m))




def operations_test(n, ratio):
    rb.test_time(n, ratio)
    mb.time_test(n, ratio)



def API_test(n, path, params):
    rb.API_time_test(n, path, params)
    mb.API_time_test(n, path, params)



API_test(100, config.coin_desk_path, config.coin_desk_params)



