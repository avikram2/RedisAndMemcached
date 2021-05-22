import redis
import time
import numpy as np
import json
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


def time_set_str(r_conn, n):
    sum = 0
    for x in range(0,n):
        start_set = time.process_time()
        set_string(r_conn, x, x)
        end_set = time.process_time()
        sum += (end_set - start_set)
    average = sum/n
    print("Total Time for {} SET operations (in seconds) {}".format(n, sum))
    print("Average time for 1 SET Operation (in seconds) {}".format(average))

def time_get_str(r_conn, n):
    sum = 0
    for x in range(0,n):
        start_set = time.process_time()
        get_string_value(r_conn, x)
        end_set = time.process_time()
        sum += (end_set - start_set)
    average = sum/n
    print("Total Time for {} GET operations (for items in Redis) (in seconds) {}".format(n, sum))
    print("Average time for 1 GET Operation (for an item in Redis) (in seconds) {}".format(average))


def time_str_miss(r_conn, n):
    sum = 0
    for x in range(n+1,2*n):
        start_set = time.process_time()
        get_string_value(r_conn, x)
        end_set = time.process_time()
        sum += (end_set - start_set)
    length = len(range(n+1, 2*n))
    average = sum/length
    print("Total Time for {} GET operations (for items NOT in Redis) (in seconds) {}".format(length, sum))
    print("Average time for 1 GET Operation (for an item NOT in Redis) (in seconds) {}".format(average))

def time_half_miss(r_conn, n):
    sum = 0
    for x in range(n//2,3*n//2):
        start_set = time.process_time()
        get_string_value(r_conn, x)
        end_set = time.process_time()
        sum += (end_set - start_set)
    length = len(range(n//2,3*n//2))
    average = sum/length
    print("Total Time for {} GET operations (half miss rate) (in seconds) {}".format(length, sum))
    print("Average time for 1 GET Operation (half miss rate) (in seconds) {}".format(average))

def test_set_get_str():
    r = create_server()
    set_string(r, 'ayush', 'vikram')
    assert get_string_value(r, 'ayush') == 'vikram'

def test_expiry_string():
    r = create_server()
    set_string_expiry(r, 'pencil', timedelta(seconds = 10), 'pen')
    assert get_string_value(r, 'pencil') == 'pen'
    time.sleep(10)
    assert get_string_value(r, 'pencil') is None


def test_time():
    r = create_server()
    n = 10000
    time_set_str(r, n)
    time_get_str(r, n)
    time_str_miss(r, n)
    time_half_miss(r, n)




test_time()