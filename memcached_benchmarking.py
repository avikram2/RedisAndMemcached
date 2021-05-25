from pymemcache.client.base import Client
import time
import numpy as np
import requests
import json
import config
'''
memcached_connection():
Establish connnection with Memcached server with some provided arguments
'''
def memcached_connection(hostname = 'localhost', portnum = 11211, connect_timeout_ = None, timeout_ = None):
    mem = Client((hostname, portnum), connect_timeout = connect_timeout_, timeout = timeout_)
    # the server(hostname) parameter can be passed a host string, a host:port string, or a (host, port) 2-tuple. 
    # The host part may be a domain name, an IPv4 address, or an IPv6 address. 
    # The port may be omitted, in which case it will default to 11211.
    if mem is None:
        raise Exception("Invalid Connection")
    mem.flush_all()
    return mem

'''
set_memlimit(): set the memory limit of the cache
limit – int, the number of megabytes to set as the new cache memory limit
returns True if no exceptions raised
'''
def set_memlimit(mem, limit):
    val = mem.cache_memlimit(limit)
    return val

'''
set_key():
Set a key unconditionally
expiry – optional int, number of seconds until the item is expired from the cache, or zero for no expiry (the default).
'''
def set_key(mem, key, value, expiry = 0):
    mem.set(key, value, expire = expiry)



'''test setting the memory limit directive'''
def test_memorylimit(lim):
    m = memcached_connection()
    assert set_memlimit(m, lim) == True


'''
get_value: returns the value associated with a single key
'''
def get_value(mem, key):
    mem.get(key)

'''time_set():
'''
def time_set(mem, n):
    sum = 0
    for x in range(0, n):
        start_set = time.process_time()
        mem.set(str(x), str(x))
        end_set = time.process_time()
        sum += (end_set - start_set)
    average = sum/n
    print("\nTotal Time for {} SET operations (in seconds) {}".format(n, sum))
    print("Average time for 1 SET Operation (in seconds) {}".format(average))
    return average

def time_get(mem, n):
    sum = 0
    for x in range(0,n):
        start_set = time.process_time()
        val = mem.get(str(x))
        assert val is not None
        end_set = time.process_time()
        sum += (end_set - start_set)
    average = sum/n
    print("\nTotal Time for {} GET operations (for items in memcache) (in seconds) {}".format(n, sum))
    print("Average time for 1 GET Operation (for an item in memcache) (in seconds) {}".format(average))
    return average


def time_miss(mem, n):
    sum = 0
    for x in range(n+1,2*n):
        start_set = time.process_time()
        val = mem.get(str(x))
        assert val is None
        end_set = time.process_time()
        sum += (end_set - start_set)
    length = len(range(n+1, 2*n))
    average = sum/length
    print("\nTotal Time for {} GET operations (for items NOT in memcache) (in seconds) {}".format(length, sum))
    print("Average time for 1 GET Operation (for an item NOT in memcache) (in seconds) {}".format(average))
    return average



def time_half_miss(mem, n):
    sum = 0
    for x in range(n//2,3*n//2):
        start_set = time.process_time()
        mem.get(str(x))
        end_set = time.process_time()
        sum += (end_set - start_set)
    length = len(range(n//2,3*n//2))
    average = sum/length
    print("\nTotal Time for {} GET operations (half miss rate) (in seconds) {}".format(length, sum))
    print("Average time for 1 GET Operation (half miss rate) (in seconds) {}".format(average))
    return average



def time_ratio_miss(mem, ratio, n):
    #ratio is the ratio of the probability of hits to misses, for ex. ratio of 1/3 means 1/3 probability of a hit
    sum = 0
    end_val = (1/ratio)*n
    for x in range(n):
        num = np.random.randint(0, end_val)
        start_time = time.process_time()
        mem.get(str(x))
        end_time = time.process_time()
        sum += (end_time - start_time)
    average = sum/n
    print("\nTotal Time for {} GET operations with probability {} of being a hit is (in seconds) {}".format(n, ratio, sum))
    print("Average time for 1 GET Operation with probability {} of being a hit is (in seconds) {}".format(ratio, average))
    return average


'''time_mem_incr():
measure the time taken by the increment operation
'''
def time_mem_incr(mem, n, amt = 1):
    sum = 0
    for x in range(n):
        start = time.process_time()
        val = mem.incr(str(x), amt)
        assert val is not None
        end = time.process_time()
        sum += (end-start)
    average = sum/n
    print("\nmemcached: Total time for {} incr operations by {} amount is: {}".format(n, amt, sum))
    print("memcached: Average time for 1 incr operation by {} amount is: {}".format(amt, average))
    return average



def time_test(n, ratio):
    mem = memcached_connection()
    mem.flush_all()
    time_set(mem, n)
    time_get(mem, n)
    time_miss(mem, n)
    time_half_miss(mem, n)
    time_ratio_miss(mem, ratio, n)
    time_mem_incr(mem, n)




'''
naive_loop:
makes n number of API get request calls to url at path with parameters in params
returns the average time for one such call
'''

def naive_loop_API_get(n, path, params):
    sum = 0
    for x in range(n):
        start = time.process_time()
        response = requests.get(url = path, params= params)
        if response.status_code >= 400:
            raise Exception("API Error")
        response_json = response.json()
        end = time.process_time()
        sum += (end-start)
    return sum/n



'''
memcached_API_loop():
makes n API GET Request calls to url at path with parameters in params
returns average time for one such call
'''

def memcached_API_loop(n, path, params):
    sum = 0
    mem = memcached_connection()
    for x in range(n):
        start = time.process_time()
        if (mem.get(path) is not None):
            json.loads(mem.get(path))
        else:
            response = requests.get(url = path, params= params)
            if response.status_code >= 400:
                raise Exception("API Error")
            response_json = response.json()
            mem.set(path, json.dumps(response_json))
        end = time.process_time()
        sum += (end - start)
    return sum/n



def API_time_test(n, path, params):
    print("Naive API calls average time: {}".format(naive_loop_API_get(n, path, params)))
    print("memcached API Loop average time: {}".format(memcached_API_loop(n, path, params)))

#API_time_test(1000, config.coin_desk_path, config.coin_desk_params)


def memcache_factorial(mem_factorial_client, n):
    if n<=1:
        return 1
    if mem_factorial_client.get(str(n)) is not None:
        return int(mem_factorial_client.get(str(n)))
    else:
        val = n * memcache_factorial(mem_factorial_client, n-1)
        mem_factorial_client.set(str(n), str(val))
        return val
