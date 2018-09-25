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
import pprint
import pickle

import aioredis
import random
import string
import timeit
import inspect
import logging
import asyncio
import traceback
import collections
import prometheus_client
from datetime import datetime
from datetime import timedelta

import redis
import threading

__all__ = [
    'Client',
    'Server',
    'RemoteException',
    'TimeoutException'
]


assert pprint


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
    elif hasattr(value, 'httpResponse'):
        return value.httpResponse['data']
    elif hasattr(value, '_response'):
        return value.__dict__
    elif hasattr(value, 'data'):
        return value.__dict__


    try:
        import bson
        if isinstance(value, bson.objectid.ObjectId):
            return str(value)
        elif isinstance(value, bson.timestamp.Timestamp):
            return json_default(value.as_datetime())
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

TIMEOUT = 300
SLEEP = 15
RESULTS_EXPIRE = 300


def truncated_repr(response):
    response_repr = dict()
    repr_keys = set(('return_value', 'response', 'exception'))
    for k, v in response.items():
        if k in repr_keys and v:
            v_repr = repr(v)
            k += '_repr'
            if len(v_repr) > 100:
                v_repr = v_repr[:50] + '...' + v_repr[-50:]

            v = v_repr

        elif isinstance(v, (list, set)) and v:
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
            if isinstance(arg, str) and sys.version_info.major == 2:
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

    def __init__(self, redis_pool: aioredis.ConnectionsPool=None, redis_name: str=None, redis_sentinel: aioredis.RedisSentinel=None):
        self.redis_pool = redis_pool
        self.redis_name = redis_name
        self.redis_sentinel = redis_sentinel

        assert redis_pool or (redis_name and redis_sentinel), 'Need to specify either a redis pool or a sentinal'

    def get_redis_slave(self) -> aioredis.Redis:
        if self.redis_pool:
            return self.redis_pool
        else:
            return self.redis_sentinel.slave_for(self.redis_name)

    def get_redis_master(self) -> aioredis.Redis:
        if self.redis_pool:
            return self.redis_pool
        else:
            return self.redis_sentinel.master_for(self.redis_name)

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

    redis_slave = property(get_redis_slave)
    redis_master = property(get_redis_master)
    redis_server = property(get_redis_master)


class Client(RedisBase):
    '''Calls remote functions using Redis as a message queue.'''

    def __init__(
            self,
            name,
            redis_name,
            redis_sentinel,
            timeout=TIMEOUT,
            transport='json'):
        self.name = name
        self.timeout = timeout

        if transport == 'json':
            self.transport = JSONTransport()
        elif transport == 'pickle':
            self.transport = PickleTransport()
        else:
            raise Exception('invalid transport {0}'.format(transport))

        RedisBase.__init__(self, redis_name=redis_name, redis_sentinel=redis_sentinel)

    def has_subscribers(self, queue):
        return self.redis_slave.exists(self.get_activity_key(queue))

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
        self.redis_master.lpush(queue_key, message)

        for i in range(self.timeout):
            response = self.redis_master.brpop(response_queue, timeout=i)
            if response:
                response = response[1]
                break
            elif not self.has_subscribers(queue_key):
                self.redis_master.rpop(queue_key)

                raise ServerDiedException(
                    'Server died after waiting %s seconds for %r' % (
                        i, method_name), rpc_request)
        else:
            self.redis_master.rpop(queue_key)

            raise TimeoutException(
                'No response within %s seconds while waiting for %r' % (
                    self.timeout, method_name), rpc_request)

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

    def __init__(self, redis_pool=None, redis_name=None, redis_sentinel=None, local_objects=None):
        self.stopped = False
        self.replaced = False
        if local_objects is None:
            local_objects = dict()
        self.local_objects = local_objects
        self.last_update = 0
        self.futures: list[asyncio.Future] = []
        self.queue_keys = []
        self.activity_keys = []

        RedisBase.__init__(self, redis_pool=redis_pool, redis_name=redis_name, redis_sentinel=redis_sentinel)

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

    async def set_active(self, sleep=5):
        while not self.stopped:
            # if self.activity_keys[0] != 'manager':
            #     thread_id = threading.get_ident()
            #     redis_thread_id = await self.redis_slave.get(self.activity_keys[0])
            #     if redis_thread_id and redis_thread_id != thread_id:
            #         Some other worker is also servicing this user, that should not happen
                    # self.replaced = True
                    # self.stopped = True
                    # logger.warning('Got unexpected thread id %r instead of %r for %r',
                    #                redis_thread_id, thread_id, self.activity_keys)
                    # break

            # with self.redis_server.pipeline() as pipe:
            pipe = self.redis_master.pipeline()

            now = timeit.default_timer()
            self.last_update = now

            for key in self.activity_keys:
                # Add 5 seconds margin to be safe
                pipe.setex(key, sleep + 5, now)

            # asyncio.run_coroutine_threadsafe(
            #     pipe.execute(),
            #     self.redis_sentinel._pool._loop,
            # )
            await pipe.execute()
            await asyncio.sleep(sleep)

    async def run(self, loop):
        # Initialize everything and start the loop
        self.add_local_object()
        self.futures.append(asyncio.ensure_future(self.set_active(), loop=loop))
        try:
            # asyncio.ensure_future(self.loop(loop), loop=loop)
            # loop.run_forever()
            await self.loop(loop)
        finally:
            if not self.replaced:
                await self.redis_master.delete(*self.activity_keys)
                self.stop()
                await loop.shutdown_asyncgens()

    def stop(self):
        self.stopped = True
        for future in self.futures:
            future.cancel()

    def drain(self):
        # print('waiting for futures')
        # done, not_done = futures.wait(
        #     tuple(self.futures),
        #     timeout=0.1,
        #     return_when=futures.FIRST_COMPLETED)
        #
        # print('got', done, not_done)
        # for future in done:
        #     logger.info(future.result())
        # self.futures = list(not_done)
        #
        for future in self.futures:
            if future.done():
                self.futures.remove(future)

    async def loop(self, loop):
        while not self.stopped:
            await asyncio.sleep(0)
            logger.debug('waiting for message in %s', ','.join(self.queue_keys))
            message = await self.redis_master.brpop(*self.queue_keys, timeout=SLEEP)

            await asyncio.sleep(0)
            self.drain()
            await asyncio.sleep(0)

            if message is not None:
                name, message = message
                # We were replaced, requeue the message
                if self.replaced:
                    self.redis_master.lpush(name, message)
                    return

                name = name.rsplit(':', 1)[0]
                self.futures.append(asyncio.ensure_future(self.execute(name, message), loop=loop))
                await asyncio.sleep(0)
            else:
                logger.debug('no message')

    @classmethod
    def get_type_name(cls, object_):
        type_ = type(object_).__name__
        type_ = reverse_classes.get(type_, type_)
        return type_

    # def execute(self, name, message):
    #     try:
    #         loop = asyncio.get_event_loop()
    #     except RuntimeError:
    #         loop = asyncio.new_event_loop()
    #         asyncio.set_event_loop(loop)
    #
    #     loop.run_until_complete(self._execute(name, message))

    async def execute(self, name, message):
        log = dict()
        logger.debug('RPC Request: %s' % message)
        transport, rpc_request = decode_message(message)
        response_queue = rpc_request['response_queue']

        log['request'] = message

        function_call = FunctionCall.from_dict(rpc_request['function_call'])
        start = datetime.now()

        try:
            local_object = self.local_objects[name]
            method = getattr(local_object, function_call['name'])
            with redisrpc_duration.labels(method=function_call['name']).time():
                if inspect.iscoroutinefunction(method):
                    log['async'] = True
                    response = await method(
                        *function_call['args'],
                        **function_call['kwargs'])
                else:
                    logger.warning('%r is not an async method', method)
                    log['async'] = False
                    response = method(
                        *function_call['args'],
                        **function_call['kwargs'])

                await asyncio.sleep(0)
                # if inspect.iscoroutine(response):
                #     log['async_response'] = True
                #     response = await response

            rpc_response = dict(
                return_type=self.get_type_name(response),
                return_value=response,
                duration=datetime.now() - start,
            )
            if hasattr(response, 'status'):
                rpc_response['return_status'] = response.status

            logfunc = logger.info
        except Exception as e:
            logfunc = logger.error
            logger.exception('Error while executing %r: %r' % (
                function_call, e))

            trace = traceback.format_exc()
            if len(trace) > 65535:
                trace = trace[:32765] + '...' + trace[-32765:]

            rpc_response = dict(
                exception=str(e),
                exception_type=self.get_type_name(e),
                exception_trace=trace,
            )
            if hasattr(e, 'getResponse'):
                response = e.getResponse()
                if hasattr(response, 'status'):
                    rpc_response['return_status'] = response.status
                rpc_response['return_value'] = response
                rpc_response['return_type'] = self.get_type_name(response)

            duration = datetime.now() - start
            redisrpc_exception_duration.labels(
                method=function_call['name'],
                exception=type(e).__name__,
            ).observe(duration.total_seconds())
        message = transport.dumps(rpc_response)
        response_repr = truncated_repr(transport.loads(message))
        log['response'] = response_repr
        logfunc('RPC: %s :: %s', log['request'], log['response'])
        # logfunc('RPC Response: %s' % response_repr)

        pipe = self.redis_master.pipeline()
        pipe.lpush(response_queue, message)
        pipe.expire(response_queue, RESULTS_EXPIRE)
        await pipe.execute()


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
    'GenericResponse': 'Response',
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
