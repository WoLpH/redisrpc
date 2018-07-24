# Copyright (C) 2018.  Rick van Hattem <wolph@wol.ph>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


import sys
import json
import redis
import pprint
import pickle
import random
import string
import timeit
import logging
import threading
import functools
import traceback
import collections
import prometheus_client
from datetime import datetime
from datetime import timedelta


__all__ = [
    'Client',
    'Server',
    'RemoteException',
    'TimeoutException'
]


def json_default(value):
    if isinstance(value, FromNameMixin):
        return value.__dict__
    elif isinstance(value, logging.Logger):
        return value.name
    elif isinstance(value, datetime):
        return value.isoformat()
    elif isinstance(value, timedelta):
        return str(value)
    elif isinstance(value, Response):
        return value.__dict__
    elif isinstance(value, RemoteException):
        return value.__dict__
    elif hasattr(value, 'info') and hasattr(value, 'id'):
        return dict(result=dict(id=value.id, info=value.info))

    try:
        import bson
        if isinstance(value, bson.objectid.ObjectId):
            return str(value)
        elif isinstance(value, bson.timestamp.Timestamp):
            return default(value.as_datetime())
        elif isinstance(value, bson.binary.Binary):
            return repr(value)
    except ImportError:
        pass

    try:
        import pymongo
        if isinstance(value, pymongo.cursor.Cursor):
            return list(value)
    except ImportError:
        pass


logger = logging.getLogger(__name__)
redisrpc_duration = prometheus_client.Histogram(
    'redisrpc_duration', 'Duration of redisrpc call',
    ['method'],
)
redisrpc_exception_duration = prometheus_client.Histogram(
    'redisrpc_exception_duration', 'Duration of redisrpc call',
    ['method', 'exception'],
)

TIMEOUT = 60
SLEEP = 5
RESULTS_EXPIRE = 300


if sys.version_info.major == 2:
    range = xrange  # NOQA
else:
    unicode = str
    basestring = str, bytes


def truncated_repr(response):
    response_repr = dict()
    repr_keys = set(('return_value', 'response', 'exception'))
    for k, v in response.items():
        if isinstance(v, (list, set)) and v:
            if len(v) > 10:
                vs = v[:5] + ['...'] + v[-5:]
            else:
                vs = v

            for i, v in enumerate(vs):
                v = repr(v)
                if len(v) > 100:
                    v = v[:50] + '...' + v[-50:]

                vs[i] = v

            k += '_repr'
            v = repr(vs)

        elif isinstance(v, dict) and v:
            ks = sorted(v)
            if len(ks) > 10:
                ks = ks[:5] + ['...'] + ks[-5:]

            vs = {k2: v.get(k2, '...') for k2 in ks}
            for k2, v in vs.items():
                v = repr(v)

                if len(v) > 100:
                    v = v[:50] + '...' + v[-50:]

                vs[k2] = v

            k += '_repr'
            v = repr(vs)

        elif k in repr_keys and v:
            v_repr = repr(v)
            k += '_repr'
            if len(v_repr) > 100:
                v_repr = v_repr[:50] + '...' + v_repr[-50:]

        response_repr[k] = v

    return response_repr


def random_string(size=8, chars=string.ascii_uppercase + string.digits):
    '''Ref: http://stackoverflow.com/questions/2257441'''
    return ''.join(random.choice(chars) for x in range(size))


class curry:
    '''functools.partial with proper repr()'''

    def __init__(self, function, *args, **kwargs):
        self.function = function
        self.pending = args[:]
        self.kwargs = kwargs.copy()

    def __repr__(self):
        args = []
        if self.pending:
            args.append(repr(self.pending))
        if self.kwargs:
            args.append(repr(self.kwargs))

        return '<%s %s>' % (self.function.__name__, ' '.join(args))

    def __call__(self, *args, **kwargs):
        if kwargs and self.kwargs:
            kw = self.kwargs.copy()
            kw.update(kwargs)
        else:
            kw = kwargs or self.kwargs
            return self.function(*(self.pending + args), **kw)


class FunctionCall(dict):
    '''Encapsulates a function call as a Python dictionary.'''

    @staticmethod
    def from_dict(dictionary):
        '''Return a new FunctionCall from a Python dictionary.'''
        name = dictionary.get('name')
        args = dictionary.get('args')
        kwargs = dictionary.get('kwargs')
        return FunctionCall(name, args, kwargs)

    def __init__(self, name, args=None, kwargs=None):
        '''Create a new FunctionCall from a method name, an optional argument
        tuple, and an optional keyword argument dictionary.'''
        self['name'] = name
        self['args'] = args or []
        self['kwargs'] = kwargs or {}

    def as_python_code(self):
        '''Return a string representation of this object that can be evaled to
        execute the function call.'''
        args = []
        for arg in self['args']:
            if isinstance(arg, unicode) and sys.version_info.major == 2:
                arg = arg.encode('utf-8')
            else:
                arg = str(arg)

            args.append(arg)

        argstring = '' if 'args' not in self else ','.join(args)
        kwargstring = '' if 'kwargs' not in self else ','.join(
            '%s=%s' %
            (key, val) for (
                key, val) in list(
                self['kwargs'].items()))
        if len(argstring) == 0:
            params = kwargstring
        elif len(kwargstring) == 0:
            params = argstring
        else:
            params = ','.join([argstring, kwargstring])
        return '%s(%s)' % (self['name'], params)

    def __str__(self):
        return self.as_python_code()


def decode_message(message):
    '''Returns a (transport, decoded_message) pair.'''
    # Try JSON, then try Python pickle, then fail.
    try:
        if hasattr(message, 'decode'):
            message = message.decode()
        return JSONTransport.create(), json.loads(message)
    except Exception:
        raise
        pass
    return PickleTransport.create(), pickle.loads(message)


class JSONTransport(object):
    '''Cross platform transport.'''
    _singleton = None

    @classmethod
    def create(cls):
        if cls._singleton is None:
            cls._singleton = JSONTransport()
        return cls._singleton

    def dumps(self, obj):
        return json.dumps(obj, default=json_default)

    def loads(self, obj):
        return json.loads(obj)


class PickleTransport(object):
    '''Only works with Python clients and servers.'''
    _singleton = None

    @classmethod
    def create(cls):
        if cls._singleton is None:
            cls._singleton = PickleTransport()
        return cls._singleton

    def dumps(self, obj):
        # Version 2 works for Python 2.3 and later
        return pickle.dumps(obj, protocol=2)

    def loads(self, obj):
        return pickle.loads(obj)


class RedisBase(object):

    def __init__(self, redis_args=None):
        self.redis_args = redis_args or dict()
        self.redis_args['decode_responses'] = True
        logger.debug('RPC Redis args: %s' % self.redis_args)

        self._redis_server = None

    def __del__(self):
        self._redis_server = None

    def get_redis_server(self):
        if not self._redis_server:
            self._redis_server = redis.StrictRedis(**self.redis_args)

        return self._redis_server

    @classmethod
    def get_running_key(cls, queue):
        return '%s:running' % queue

    @classmethod
    def get_queue_key(cls, queue):
        return '%s:queue' % queue

    @classmethod
    def get_activity_key(cls, queue):
        return '%s:activity' % queue

    @classmethod
    def get_rpc_key(cls, queue):
        return '%s:rpc:%s' % (queue, random_string())

    redis_server = property(get_redis_server)


class Client(RedisBase):
    '''Calls remote functions using Redis as a message queue.'''

    def __init__(
            self,
            name,
            redis_args=None,
            timeout=TIMEOUT,
            transport='json'):
        self._redis_server = None
        self.name = name
        self.timeout = timeout

        if transport == 'json':
            self.transport = JSONTransport()
        elif transport == 'pickle':
            self.transport = PickleTransport()
        else:
            raise Exception('invalid transport {0}'.format(transport))

        RedisBase.__init__(self, redis_args)

    def has_subscribers(self, queue):
        return self.redis_server.exists(self.get_activity_key(queue))

    def call(self, method_name, *args, **kwargs):
        function_call = FunctionCall(method_name, args, kwargs)
        response_queue = self.get_rpc_key(self.name)
        rpc_request = dict(
            function_call=function_call,
            response_queue=response_queue,
        )
        message = self.transport.dumps(rpc_request)
        logger.debug('RPC Request: %s' % message)

        if not self.has_subscribers(self.name):
            raise NoServerAvailableException(
                'No servers available for queue %s' % self.name)

        start = datetime.now()
        queue_key = self.get_queue_key(self.name)
        queue_length = self.redis_server.lpush(queue_key, message)

        response = self.redis_server.brpop(response_queue, timeout=self.timeout)
        if response is None:
            if self.has_subscribers(queue_key):
                raise TimeoutException(
                    'No response within %s seconds while waiting for %r' % (
                        self.timeout, method_name), rpc_request)
            else:
                raise ServerDiedException(
                    'Server died after waiting %s seconds for %r' % (
                        i, method_name), rpc_request)
        else:
            response = response[1]

        logger.debug('RPC Response: %s' % response)

        rpc_response = self.transport.loads(response)

        response_repr = truncated_repr(rpc_response)

        duration = datetime.now() - start
        response_repr['duration'] = str(duration)
        response_repr['duration_ms'] = duration.total_seconds() * 1000
        response_repr['call'] = str(function_call)

        logger.info('' % function_call, dict(rpc_responses=[response_repr]))
        if 'return_value' in rpc_response:
            if rpc_response.get('return_type'):
                Class_ = Response.from_name(rpc_response.get('return_type'))
            else:
                Class_ = Response

            response = Class_(rpc_response['return_value'])
        else:
            response = None

        if 'exception' in rpc_response:
            if rpc_response.get('exception_type'):
                Exception = RemoteException.from_name(
                    rpc_response['exception_type'])
                exception_name = rpc_response['exception_type']
            else:
                Exception = RemoteException
                exception_name = 'RemoteException'

            exception = Exception(rpc_response['exception'])
            exception.response = response
            logger.exception(repr(exception))

            redisrpc_exception_duration.labels(
                method=method_name,
                exception=exception_name,
            ).observe(duration.total_seconds())
            raise exception
        else:
            redisrpc_duration.labels(method=method_name).observe(
                duration.total_seconds())
            return response

    def __getattr__(self, name):
        '''Treat missing attributes as remote method call invocations.'''
        return curry(self.call, name)


class Server(RedisBase):
    '''Executes function calls received from a Redis queue.'''

    def __init__(self, local_objects=None, redis_args=None):
        if local_objects is None:
            local_objects = dict()
        self.local_objects = local_objects
        self.last_update = 0
        self.queue_keys = []
        self.activity_keys = []

        RedisBase.__init__(self, redis_args)

    def add_local_object(self, *args, **kwargs):
        if args:
            kwargs[args[0]] = args[1]

        for key, value in kwargs.items():
            # queue = self.queue_key(key)
            # channel, subscribers = self.redis_server.pubsub_numsub(queue)[0]
            # assert not subscribers, 'Someone is already subscribed to %r' % \
            #     channel

            self.local_objects[key] = value
            # self.pubsub.subscribe(queue)

        self.queue_keys = [self.get_queue_key(key)
                           for key in self.local_objects]
        self.activity_keys = [self.get_activity_key(key)
                              for key in self.local_objects]

    def set_active(self):
        now = timeit.default_timer()
        if now - self.last_update < SLEEP:
            return

        with self.redis_server.pipeline() as pipe:
            for key in self.activity_keys:
                # Add 5 seconds margin to be safe
                pipe.setex(key, TIMEOUT + SLEEP + 5, now)

            pipe.execute()
        self.last_update = now

    def run(self):
        # Initialize everything and start the loop
        self.add_local_object()
        self.set_active()
        try:
            self.loop()
        finally:
            self.redis_server.delete(*self.activity_keys)

    def loop(self):
        while True:
            message = self.redis_server.brpop(self.queue_keys, timeout=SLEEP)
            self.set_active()

            if message is not None:
                name, message = message
                name = name.rsplit(':', 1)[0]
                self.execute(name, message)

    @classmethod
    def get_type_name(cls, object_):
        type_ = type(object_).__name__
        type_ = reverse_classes.get(type_, type_)
        return type_

    def execute(self, name, message):
        logger.debug('RPC Request: %s' % message)
        transport, rpc_request = decode_message(message)
        response_queue = rpc_request['response_queue']

        function_call = FunctionCall.from_dict(rpc_request['function_call'])
        start = datetime.now()

        try:
            local_object = self.local_objects[name]
            method = getattr(local_object, function_call['name'])
            with redisrpc_duration.labels(method=function_call['name']).time():
                response = method(
                    *function_call['args'],
                    **function_call['kwargs'])

            rpc_response = dict(
                return_type=self.get_type_name(response),
                return_value=response,
            )
            log = logger.info
        except Exception as e:
            log = logger.error
            logger.exception('Error while executing %r: %r' % (
                function_call, e))
            rpc_response = dict(
                exception=str(e),
                exception_type=self.get_type_name(e),
            )
            duration = datetime.now() - start
            redisrpc_exception_duration.labels(
                method=function_call['name'],
                exception=type(e).__name__,
            ).observe(duration.total_seconds())
        message = transport.dumps(rpc_response)
        log('RPC Response: %s' % message)

        with self.redis_server.pipeline() as pipe:
            pipe.lpush(response_queue, message)
            pipe.expire(response_queue, RESULTS_EXPIRE)
            pipe.execute()


class AsyncServer(Server):

    def loop(self):
        import asyncio
        from concurrent import futures
        self.event_loop = loop = asyncio.get_event_loop()

        self.executor = futures.ThreadPoolExecutor(256)

        loop.run_until_complete(Server.loop(self))

    def execute(self, worker_name, message):
        if worker_name == 'manager':
            Server.execute(self, worker_name, message)
        else:
            import asyncio
            asyncio.ensure_future(self.event_loop.run_in_executor(
                self.executor, Server.execute, self, worker_name, message))


def native(value):
    return value


default_classes = {
    'array': native,
    'boolean': bool,
    'NULL': lambda data: None,
    'string': lambda s: s,
    'tuple': tuple,
    'dict': dict,
    'OrderedDict': collections.OrderedDict,
}
reverse_classes = {
    'NoneType': 'NULL',
    'bool': 'boolean',
    'str': 'string',
    'OrderedDict': 'dict',
}


class FromNameMixin(object):
    # Needs to be initialized in the inheriting classes. Otherwise the classes
    # are shared (and mixing exceptions and other types is a bad idea)
    classes = None

    def __init__(self, data=None):
        if data:
            if isinstance(data, dict):
                self.__dict__.update(data)
            else:
                logger.error(
                    'Unexpected data for %s: %r', self.__class__, data)

    def get(self, key, default=None):
        return getattr(self, str(key), default)

    def __getitem__(self, key):
        return self.get(key)

    def __repr__(self):
        return json.dumps(self.__dict__, default=json_default)

    @classmethod
    def from_name(cls, key, *keys):
        key = tuple(str(key).replace('\\', '.').split('.'))

        # Make sure to combine the keys if needed
        keys = key[1:] + keys
        key = key[0]

        if key not in cls.classes:
            cls.classes[key] = type(key, (cls,), dict(parent=cls))

        Class_ = cls.classes[key]
        if keys:
            return Class_.from_name(*keys)
        else:
            return Class_


class Response(FromNameMixin):
    '''Returned by the RPC client, through `Response.classes[name]` the return
    type can be overridden'''
    classes = default_classes.copy()


class RedisRPCException(Exception):
    pass


class RemoteException(FromNameMixin, RedisRPCException):
    '''Raised by an RPC client when an exception occurs on the RPC server.'''
    classes = default_classes.copy()

    def __init__(self, message=None):
        if message:
            Exception.__init__(self, message)
        FromNameMixin.__init__(self)


class LocalException(RedisRPCException):
    pass


class TimeoutException(LocalException):
    '''Raised by an RPC client when a timeout occurs.'''
    pass


class NoServerAvailableException(LocalException):
    pass


class ServerDiedException(LocalException):
    pass
