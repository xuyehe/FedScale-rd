import logging
import redis
import pickle
import subprocess
import time
from threading import Thread
from functools import wraps


def async_write(func):
    """Async wrapper for write operations. Return value of func would be ignored.

        Args:
            func (((...) -> object)): Function object to be asynchronously executed.

        Returns:
            ((...) -> object): Wrapped function object.

    """
    @wraps(func)
    def decorated(*args, **kwargs):
        tr = Thread(target=func, args=args, kwargs=kwargs)
        tr.start()
    return decorated


def bytes_serialize(data):
    """Serialize input data into bytes.

    Args:
        data (Any): Input to be serialized.

    Returns:
        bytes: Serialized data in bytes.
    """
    return pickle.dumps(data)


def bytes_deserialize(data_bytes):
    """Deserialize bytes.

    Args:
        data_bytes (bytes): Input to be deserialized.

    Returns:
        Any: Original data.
    """
    return pickle.loads(data_bytes)


def start_redis_server(
    executable,
    fedscale_home,
    host='127.0.0.1',
    port=6379,
    password='',
):
    """Start the Redis server with specifications as a subprocess.

    Args:
        executable (string): Absolute path to the Redis executable.
        fedscale_home (string): Absolute path to Fedscale working directory.
        host (string, optional): IP address of the Redis server. Defaults to '127.0.0.1'.
        port (int, optional): Port of the Redis server. Defaults to 6379.
        password (string, optional): Password for server side authentication. Defaults to None.

    Raises:
        ValueError: Raised if password contains space characters.
    """
    command = [executable]
    # Hard code data storing position for Redis server
    working_dir = fedscale_home + '/redisdata'
    command += ['--dir', working_dir]
    pidfile = working_dir + '/redis_' + str(port) + '.pid'
    command += ['--pidfile', pidfile]
    logfile = working_dir + '/redis_' + str(port) + '.log'
    command += ['--logfile', logfile]
    # Other configs
    command += ['--bind', host]
    command += ['--port', str(port), '--loglevel', 'warning']
    if password != '':
        if ' ' in password:
            raise ValueError('Spaces not permitted in redis password.')
        logging.info("Password set")
        command += ['--requirepass', password]
    else:
        logging.info("Disabled protected mode since no password is set")
        command += ['--protected-mode no'] # Not safe on public internet
    # We want to avoid any periodical save.
    # Instead, we want to save a snapshot of each round's data upon round completion.
    command += ['--save \"\"']
    # start Redis Server as a subprocess
    logging.info(f'Starting Redis server at at {host}:{port}')
    subprocess.Popen(command)


def is_redis_server_online(
    host='127.0.0.1',
    port=6379,
    password='',
    retry=10
):
    """Test if Redis server is online.

    Args:
        host (string, optional): IP address of the Redis server. Defaults to '127.0.0.1'.
        port (int, optional): Port of the Redis server. Defaults to 6379.
        password (string, optional): Password for server side authentication. Defaults to None.
        retry (int, optional): Number of retry times when connection to server fails. Defaults to 10.

    Returns:
        bool: True if Redis server is online, False otherwise.
    """
    client = redis.Redis(host=host, port=port, password=password)
    for i in range(retry):
        try:
            # try a redis command to check connection
            client.randomkey()
        except Exception as e:
            time.sleep(0.1)
        else:
            logging.info(f'Connected to Redis server at {host}:{port} in {i + 1} attempts')
            return True
    else:
        logging.info(f'Failed to reach Redis server at {host}:{port} after {retry} retries')
        return False


def start_redis_server_until_success(
    executable,
    fedscale_home,
    host='127.0.0.1',
    port=6379,
    password='',
):
    """Retry until successfully starts the redis server.

    Args:
        executable (string): Absolute path to the Redis executable.
        fedscale_home (string): Absolute path to Fedscale working directory.
        host (string, optional): IP address of the Redis server. Defaults to '127.0.0.1'.
        port (int, optional): Port of the Redis server. Defaults to 6379.
        password (string, optional): Password for server side authentication. Defaults to None.

    """
    while not is_redis_server_online(host, port, password):
        start_redis_server(executable, fedscale_home, host, port, password)
        time.sleep(1) # wait for server to go online


def shutdown_server(
    host='127.0.0.1',
    port=6379,
    password='',
    nosave=False
):
    """Shutdown the Redis server.

    Args:
        host (string, optional): IP address of the Redis server. Defaults to '127.0.0.1'.
        port (int, optional): Port of the Redis server. Defaults to 6379.
        password (string, optional): Password for server side authentication. Defaults to None.
        nosave (bool, optional): If true, prevent DB from saving to disk before shutdown.
    """
    client = redis.Redis(host=host, port=port, password=password)
    try:
        client.shutdown(nosave=nosave)
        logging.info(f'Successfully shutdown Redis server at {host}:{port}')
    except Exception:
        logging.info(f'Failed to shutdown Redis server at {host}:{port}')
        pass


def clear_all_keys(
    host='127.0.0.1',
    port=6379,
    password='',
):
    """Delete all keys in the Redis server.

    Args:
        host (string, optional): IP address of the Redis server. Defaults to '127.0.0.1'.
        port (int, optional): Port of the Redis server. Defaults to 6379.
        password (string, optional): Password for server side authentication. Defaults to None.
    """
    client = redis.Redis(host=host, port=port, password=password)
    try:
        client.flushall()
        logging.info(f'Successfully cleared all keys')
    except Exception:
        pass


class Redis_client():
    '''Create a redis client connected to specified server.'''

    def __init__(self, host='localhost', port=6379, password='', tag=''):
        """Initialize the redis client.

        Args:
            host (string, optional): IP address of the Redis server. Defaults to '127.0.0.1'.
            port (int, optional): Port of the Redis server. Defaults to 6379.
            password (string, optional): Password for server side authentication. Defaults to None.
            retry (int, optional): Number of retry times when connection to server fails. Defaults to 10.
            tag (string, optional): Tag attached to the key to distinguish between jobs.
        """
        # Use tag after key to distinguish between jobs
        self.tag = tag
        retry = 100
        while not is_redis_server_online(host, port, password, retry):
            logging.info('Client waiting for redis server to get online')
            time.sleep(1) # wait until server is online
        # Set decode_responses=False to get bytes response,
        # now all values get from redis (including TYPE command) are bytes
        self.r = redis.Redis(host=host, port=port, password=password, decode_responses=False)
        logging.info('Successfully created client object')

    def __quit__(self):
        self.r.quit()

    def set_val(self, key, val, bytes=False):
        """Save the value with specified key into Redis server.

        Args:
            key (string): Key for referencing value.
            val (Any): The value to be saved.
            bytes (bool, optional): Set to True if input needs to be serialized, otherwise set to False.
                                    Defaults to False.

        Returns:
            bool | None: Status of the set operation.
        """
        tagged_key = key + self.tag
        if not bytes:
            return self.r.set(tagged_key, val)
        else:
            return self.r.set(tagged_key, bytes_serialize(val))

    @async_write
    def async_set_val(self, key, val, bytes=False):
        """Asynchronously save the value with specified key into Redis server.

        Args:
            key (string): Key for referencing value.
            val (Any): The value to be saved.
            bytes (bool, optional): Set to True if input needs to be serialized, otherwise set to False.
                                    Defaults to False.
        """
        self.set_val(key, val, bytes)


    def get_val(self, key, type=str):
        """Get the value specified by key and decode it.

        Args:
            key (string): Key for referencing value.
            type (Type): Python type for casting value.

        Raises:
            ValueError: Raised if type is not one of four options: string | int | float | bytes.

        Returns:
            string | int | float | Any: Decoded value.
        """
        raw_val = self.get_val_raw(key)
        if raw_val is None:
            return None
        if type in [bytes]:
            return bytes_deserialize(raw_val)
        else:
            ret_val = raw_val.decode('utf-8')
            if type in [int, float, str]:
                return type(ret_val)
            else:
                raise ValueError(f'Unrecognized type: {type}')

    def get_val_raw(self, key):
        """Get the raw value specified by key, i.e. undecoded response.

        Args:
            key (string): Key for referencing value.

        Returns:
            bytes | None: Undecoded value.
        """
        tagged_key = key + self.tag
        return self.r.get(tagged_key)

    def delete_key(self, key):
        """Delete the value specified by key.

        Args:
            key (string): Key for referencing value.

        Returns:
            int: Status of the delete operation.
        """
        tagged_key = key + self.tag
        return self.r.delete(tagged_key)

    def dump_to_disk(self):
        """Save the current in-memory database into disk.
        """
        self.r.bgsave()

    def update_list(self, key, lst: list, bytes=False):
        """Update list with specified key in Redis server.

        Args:
            key (string): Key for referencing the list.
            lst (list of any): List to be saved.
            bytes (bool, optional): Set to True if input needs to be serialized, otherwise set to False.
                                    Defaults to False.

        Raises:
            ValueError: Raised if key does not refer to a list or empty value in Redis.

        Returns:
            int: Status of the push operation.
        """
        tagged_key = key + self.tag
        if self.r.type(tagged_key) not in [b'list', b'none']:
            raise ValueError(f'Key {key} is not a list')
        if len(lst) == 0:
            # update to an empty list
            self.r.delete(tagged_key)
            return 1
        if not bytes:
            self.r.delete(tagged_key)
            return self.r.rpush(tagged_key, *lst)
        else:
            self.r.delete(tagged_key)
            return self.r.rpush(tagged_key, [bytes_serialize(s) for s in lst])

    @async_write
    def async_update_list(self, key, lst: list, bytes=False):
        """Asynchronously update list with specified key in Redis server.

        Args:
            key (string): Key for referencing the list.
            lst (list of any): List to be saved.
            bytes (bool, optional): Set to True if input needs to be serialized, otherwise set to False.
                                    Defaults to False.

        Raises:
            ValueError: Raised if key does not refer to a list or empty value in Redis.
        """
        self.update_list(key, lst, bytes)

    def get_list(self, key, type=str):
        """Get the list with specified value, and decode each element in it.

        Args:
            key (string): Key for referencing the list.
            type (Type): Python type for casting value.

        Raises:
            ValueError: Raised if key refers to an non-empty value which is not a list.
            ValueError: Raised if type is not one of four options: str | int | float | bytes.

        Returns:
            list of string | list of int | list of float | list of Any: Decoded list.
        """
        tagged_key = key + self.tag
        valtype = self.r.type(tagged_key)
        if valtype in [b'none']:
            # return empty list if list length is 0
            return []
        elif valtype not in [b'list']:
            raise ValueError(f'Key {key} is not a list')
        if type in [bytes]:
            return [bytes_deserialize(s) for s in self.r.lrange(tagged_key, 0, -1)]
        else:
            ret_list = [s.decode('utf-8') for s in self.r.lrange(tagged_key, 0, -1)]
            if type in [str]:
                return ret_list
            elif type in [int, float]:
                return [type(i) for i in ret_list]
            else:
                raise ValueError(f'Unrecognized type: {type}')

    def get_list_raw(self, key):
        """Get the undecoded list specified by key.

        Args:
            key (string): Key for referencing the list.

        Raises:
            ValueError: Raised if key does not refer to a list or empty value in Redis.

        Returns:
            list of bytes: Undecoded list.
        """
        tagged_key = key + self.tag
        if self.r.type(tagged_key) not in [b'list', b'none']:
            raise ValueError(f'Key {key} is not a list')
        return self.r.lrange(tagged_key, 0, -1)

    def rpush(self, key, val, bytes=False):
        """Push the value to the right of the list.

        Args:
            key (string): Key for referencing the list.
            val (Any): The value to be pushed into the list.
            bytes (bool, optional): Set to True if input needs to be serialized, otherwise set to False.
                                    Defaults to False.

        Raises:
            ValueError: Raised if key does not refer to a list or empty value in Redis.

        Returns:
            int: Status of the push operation.
        """
        tagged_key = key + self.tag
        if self.r.type(tagged_key) not in [b'list', b'none']:
            raise ValueError(f'Key {key} is not a list')
        if not bytes:
            return self.r.rpush(tagged_key, val)
        else:
            return self.r.rpush(tagged_key, bytes_serialize(val))

    @async_write
    def async_rpush(self, key, val, bytes=False):
        """Asynchronously push the value to the right of the list.

        Args:
            key (string): Key for referencing the list.
            val (Any): The value to be pushed into the list.
            bytes (bool, optional): Set to True if input needs to be serialized, otherwise set to False.
                                    Defaults to False.

        Raises:
            ValueError: Raised if key does not refer to a list or empty value in Redis.
        """
        self.rpush(key, val, bytes)


    def list_len(self, key):
        """Get the length of the list specified by key.

        Args:
            key (string): Key for referencing the list.

        Raises:
            ValueError: Raised if key refers to an non-empty value which is not a list.

        Returns:
            int: Number of elements in the list.
        """
        tagged_key = key + self.tag
        valtype = self.r.type(tagged_key)
        if valtype in [b'none']:
            # return 0 if list length is 0, i.e. no list/empty list exists
            return 0
        elif valtype not in [b'list']:
            raise ValueError(f'Key {key} is not a list')
        return self.r.llen(tagged_key)

    def exists_key(self, key):
        """Test if key exists in the database.

        Args:
            key (string): Key to be tested.

        Returns:
            int: 1 if key exists, 0 otherwise.
        """
        tagged_key = key + self.tag
        return self.r.exists(tagged_key)

    def get_type(self, key):
        """Get the type of the key in string.

        Args:
            key (string): Key for referencing the value.

        Returns:
            string: Type of the queried key.
        """
        tagged_key = key + self.tag
        return self.r.type(tagged_key).decode('utf-8')

    def get_client(self):
        """Get a reference to the redis client object.

        Returns:
            Redis: Redis client object.
        """
        return self.r
