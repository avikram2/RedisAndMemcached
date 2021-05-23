import redis
import time
import json
import requests
import numpy as np
from datetime import timedelta

'''setup_connection:
sets up the redis server and initiates the connection
'''
def setup_connection(hostname = 'localhost', port_number = 6379, db_num = 0, pass_word = None, socket_timeout_ = None):
    #hostname = IP of host, local by default
    #port_number: TCP port number , 6379 by default
    #db_num: database number, can run upto 16
    #pass_word: password, default set to None
    #socket_timeout_ : connection timeout
    r = redis.Redis(host = hostname, port = port_number, db = db_num, password = pass_word, socket_timeout = socket_timeout_)

    return r if r.ping() == True else None


'''set_maxmemory:
manually set the maxmemory directive that is used in order to limit the memory usage to a fixed amount.
The maxmemory configuration directive is used in order to configure Redis to use a specified amount of memory for the data set. It is possible to set the configuration directive using the redis.conf file, or later using the CONFIG SET command at runtime.
For example in order to configure a memory limit of 100 megabytes, the following directive can be used inside the redis.conf file.
maxmemory 100mb
Setting maxmemory to zero results into no memory limits. This is the default behavior for 64 bit systems, while 32 bit systems use an implicit memory limit of 3GB.
When the specified amount of memory is reached, it is possible to select among different behaviors, called policies. Redis can just return errors for commands that could result in more memory being used, or it can evict some old data in order to return back to the specified limit every time new data is added.
'''
def set_maxmemory(r_conn, memory_arg):
    r_conn.config_set('maxmemory', memory_arg)






'''
A note on eviction policy, taken from the documentation:
The exact behavior Redis follows when the maxmemory limit is reached is configured using the maxmemory-policy configuration directive.
The following policies are available:
noeviction: return errors when the memory limit was reached and the client is trying to execute commands that could result in more memory to be used (most write commands, but DEL and a few more exceptions).
allkeys-lru: evict keys by trying to remove the less recently used (LRU) keys first, in order to make space for the new data added.
volatile-lru: evict keys by trying to remove the less recently used (LRU) keys first, but only among keys that have an expire set, in order to make space for the new data added.
allkeys-random: evict keys randomly in order to make space for the new data added.
volatile-random: evict keys randomly in order to make space for the new data added, but only evict keys with an expire set.
volatile-ttl: evict keys with an expire set, and try to evict keys with a shorter time to live (TTL) first, in order to make space for the new data added.
The policies volatile-lru, volatile-random and volatile-ttl behave like noeviction if there are no keys to evict matching the prerequisites.
Picking the right eviction policy is important depending on the access pattern of your application, however you can reconfigure the policy at runtime while the application is running, and monitor the number of cache misses and hits using the Redis INFO output in order to tune your setup.
In general as a rule of thumb:
Use the allkeys-lru policy when you expect a power-law distribution in the popularity of your requests, that is, you expect that a subset of elements will be accessed far more often than the rest. This is a good pick if you are unsure.
Use the allkeys-random if you have a cyclic access where all the keys are scanned continuously, or when you expect the distribution to be uniform (all elements likely accessed with the same probability).
Use the volatile-ttl if you want to be able to provide hints to Redis about what are good candidate for expiration by using different TTL values when you create your cache objects.
The volatile-lru and volatile-random policies are mainly useful when you want to use a single instance for both caching and to have a set of persistent keys. However it is usually a better idea to run two Redis instances to solve such a problem.
It is also worth noting that setting an expire to a key costs memory, so using a policy like allkeys-lru is more memory efficient since there is no need to set an expire for the key to be evicted under memory pressure.
'''



'''
set_evictionpolicy()
sets the maxmemory-policy configuration directive with user provided argument
return: None
'''
def set_evictionpolicy(r_conn, eviction_policy):
    r_conn.config_set('maxmemory-policy', eviction_policy)





'''
Information about approximation of LRU algorithm used by Redis taken from the documentation:
Redis LRU algorithm is not an exact implementation. This means that Redis is not able to pick the best candidate for eviction, that is, the access that was accessed the most in the past. Instead it will try to run an approximation of the LRU algorithm, by sampling a small number of keys, and evicting the one that is the best (with the oldest access time) among the sampled keys.
However since Redis 3.0 the algorithm was improved to also take a pool of good candidates for eviction. This improved the performance of the algorithm, making it able to approximate more closely the behavior of a real LRU algorithm.
What is important about the Redis LRU algorithm is that you are able to tune the precision of the algorithm by changing the number of samples to check for every eviction. This parameter is controlled by the following configuration directive:
maxmemory-samples 5
The reason why Redis does not use a true LRU implementation is because it costs more memory. However the approximation is virtually equivalent for the application using Redis. 
'''


'''
set_maxmemory_samples():
manually set maxmemory-samples directive to tune the precision of the algorithm by changing the number of samples to check for every eviction
return: None
'''
def set_maxmemory_samples(r_conn, sample_num):
    r_conn.config_set('maxmemory-samples', sample_num)




'''
create_server():
master function/wrapper to create the connection and server and set the desired memory limits and eviction policies and the sample 
'''

def create_server(hostname_ = 'localhost', port_number_ = 6379, db_num_ = 0, pass_word_= None, _socket_timeout_ = None, memory_arg_ = 0, eviction_policy_ = 'allkeys-lru', sample_num_ = 10):
    r = setup_connection(hostname = hostname_, port_number= port_number_, db_num = db_num_, pass_word=pass_word_, socket_timeout_= _socket_timeout_)
    if r is None:
        raise Exception("Redis Server Connection Not Established")
    else:
        print("Connection Established")

        #tuning for the eviction policy and memory limits
        set_maxmemory(r, memory_arg_)
        set_evictionpolicy(r, eviction_policy_)
        set_maxmemory_samples(r, sample_num_)
        print("Memory limits and eviction policy tuned")
        return r





'''
set_string():
fill the Redis cache with string key,value pair without a fixed expiry time
'''
def set_string(r_conn, key_, value_):
    r_conn.set(key_, value_)

'''
set_string_expiry()
add a string key,value pair with expiry date
'''
def set_string_expiry(r_conn, key_, expiry_time_, value_,):
    r_conn.setex(key_, expiry_time_, value_)


'''
get_string():
get the string value associated with a key
'''
def get_string_value(r_conn, key_):
    return_val = None
    try:
        return_val = r_conn.get(key_).decode('utf-8')
    except:
        return_val =  r_conn.get(key_)
    finally:
        return return_val

'''
time_set_str():
measure the total time and average time taken to SET n number of key,value numerical (integer) pairs into the Redis cache
'''
def time_set_str(r_conn, n):
    sum = 0
    for x in range(0,n):
        start_set = time.process_time()
        set_string(r_conn, x, x)
        end_set = time.process_time()
        sum += (end_set - start_set)
    average = sum/n
    print("\nTotal Time for {} SET operations (in seconds) {}".format(n, sum))
    print("Average time for 1 SET Operation (in seconds) {}".format(average))
    return average
'''
time_get_str():
measure the total time and average time taken to GET n number of key,value pairs that all exists within the Redis cache
'''
def time_get_str(r_conn, n):
    sum = 0
    for x in range(0,n):
        start_set = time.process_time()
        get_string_value(r_conn, x)
        end_set = time.process_time()
        sum += (end_set - start_set)
    average = sum/n
    print("\nTotal Time for {} GET operations (for items in Redis) (in seconds) {}".format(n, sum))
    print("Average time for 1 GET Operation (for an item in Redis) (in seconds) {}".format(average))
    return average
'''
time_str_miss():
meaure the total and average time taken to GET n number of key,value pairs that all do not exists in Redis
'''
def time_str_miss(r_conn, n):
    sum = 0
    for x in range(n+1,2*n):
        start_set = time.process_time()
        get_string_value(r_conn, x)
        end_set = time.process_time()
        sum += (end_set - start_set)
    length = len(range(n+1, 2*n))
    average = sum/length
    print("\nTotal Time for {} GET operations (for items NOT in Redis) (in seconds) {}".format(length, sum))
    print("Average time for 1 GET Operation (for an item NOT in Redis) (in seconds) {}".format(average))
    return average
'''
time_half_miss():
measure the total and average time taken to GET n number of key,value pairs such that 1/2 of them are hits and other half misses
'''
def time_half_miss(r_conn, n):
    sum = 0
    for x in range(n//2,3*n//2):
        start_set = time.process_time()
        get_string_value(r_conn, x)
        end_set = time.process_time()
        sum += (end_set - start_set)
    length = len(range(n//2,3*n//2))
    average = sum/length
    print("\nTotal Time for {} GET operations (half miss rate) (in seconds) {}".format(length, sum))
    print("Average time for 1 GET Operation (half miss rate) (in seconds) {}".format(average))
    return average

'''
time_ratio_miss():
calculate the average and total time for n GET operations where ratio is the probability of a hit. for example if ratio is 1/3, then the probability of a hit is 1/3 and that of a miss is 2/3.
'''
def time_ratio_miss(r_conn, ratio, n):
    #ratio is the ratio of the probability of hits to misses, for ex. ratio of 1/3 means 1/3 probability of a hit
    sum = 0
    end_val = (1/ratio)*n
    for x in range(n):
        num = np.random.randint(0, end_val)
        start_time = time.process_time()
        get_string_value(r_conn, num)
        end_time = time.process_time()
        sum += (end_time - start_time)
    average = sum/n
    print("\nTotal Time for {} GET operations with probability {} of being a hit is (in seconds) {}".format(n, ratio, sum))
    print("Average time for 1 GET Operation with probability {} of being a hit is (in seconds) {}".format(ratio, average))
    return average


'''test_set_get_str
test that the custom get string value is working and returns the value for a given key
'''
def test_set_get_str():
    r = create_server()
    set_string(r, 'ayush', 'vikram')
    assert get_string_value(r, 'ayush') == 'vikram'


'''
test_expiry_string():
testing of the set_string_expiry function, which sets a key,value pair with a timedelta object as the expiry
after the expiry time runs out (10 seconds) the key,value pair should not exist anymore on Redis
'''
def test_expiry_string():
    r = create_server()
    set_string_expiry(r, 'pencil', timedelta(seconds = 10), 'pen')
    assert get_string_value(r, 'pencil') == 'pen'
    time.sleep(10)
    assert get_string_value(r, 'pencil') is None



'''
test_time
wrapper function to call the functions that measure the time taken for various scearios of reddis string key, value pairs
'''
def test_time(n, ratio):
    r = create_server()
    time_set_str(r, n)
    time_get_str(r, n)
    time_str_miss(r, n)
    time_half_miss(r, n)
    time_ratio_miss(r, ratio, n)





test_time(100000, 2/3)





# def naive_factorial(n):
#     if n <= 1:
#         return 1
#     return n * naive_factorial(n-1)

# def memo_factorial(hash, n):
#     if n <= 1:
#         return 1
#     if n in hash:
#         return hash[n]
#     else:
#         val = n * memo_factorial(hash, n-1)
#         hash[n] = val
#         return val

# redis_factorial_client = create_server()
# redis_factorial_client.hmset('factorial', {0:1})
# def redis_factorial(n):
#     if n<=1:
#         return 1
#     fac_map = redis_factorial_client.hgetall('factorial')
#     if n in fac_map:
#         return int(fac_map[n])
#     else:
#         val = n * redis_factorial(n-1)
#         fac_map[n] = val
#         redis_factorial_client.hset('factorial', n, val)
#         return val

# def test_time_factorial(n):
#     hash = dict()
#     start_naive = time.process_time()
#     naive_val = naive_factorial(n)
#     end_naive = time.process_time()

#     start_memo = time.process_time()
#     memo_val = memo_factorial(hash, n)
#     end_memo = time.process_time()

#     start_redis = time.process_time()
#     redis_fac = redis_factorial(n)
#     end_redis = time.process_time()
#     assert naive_val == redis_fac == memo_val
#     print("Time taken to compute {} factorial with naive method: {}".format(n, (end_naive-start_naive)))
#     print("Time taken to compute {} factorial with Redis: {}".format(n, (end_redis-start_redis)))
#     print("Time taken to compute {} factorial with Memo: {}".format(n, (end_memo-start_memo)))

# test_time_factorial(40)


