# Copyright (C) 2017.  Rick van Hattem <wolph@wol.ph>
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


import json
import logging
import pickle
import random
import string
import sys
import redis
import collections
from datetime import datetime


__all__ = [
    'Client',
    'Server',
    'RemoteException',
    'TimeoutException'
]


logger = logging.getLogger('redisrpc')


if sys.version_info < (3,):
    range = xrange


def random_string(size=8, chars=string.ascii_uppercase + string.digits):
    '''Ref: http://stackoverflow.com/questions/2257441'''
    return ''.join(random.choice(chars) for x in range(size))


class curry:
    '''Ref: https://jonathanharrington.wordpress.com/2007/11/01/currying-and-python-a-practical-example/'''

    def __init__(self, fun, *args, **kwargs):
        self.fun = fun
        self.pending = args[:]
        self.kwargs = kwargs.copy()

    def __call__(self, *args, **kwargs):
        if kwargs and self.kwargs:
            kw = self.kwargs.copy()
            kw.update(kwargs)
        else:
            kw = kwargs or self.kwargs
            return self.fun(*(self.pending + args), **kw)


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
        argstring = '' if 'args' not in self else \
            ','.join(str(arg) for arg in self['args'])
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


def decode_message(message):
    '''Returns a (transport, decoded_message) pair.'''
    # Try JSON, then try Python pickle, then fail.
    try:
        return JSONTransport.create(), json.loads(message.decode())
    except Exception:
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
        return json.dumps(obj)

    def loads(self, obj):
        return json.loads(obj.decode())


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
        logger.debug('RPC Redis args: %s', self.redis_args)

        self.pubsub = None
        self.redis_server = None
        self.redis_pubsub_server = None

    def __del__(self):
        if self.pubsub:
            self.pubsub.close()

        self.pubsub = None
        self.redis_server = None
        self.redis_pubsub_server = None

    def get_pubsub(self):
        if not self.pubsub:
            self.redis_pubsub_server = redis.StrictRedis(**self.redis_args)
            self.pubsub = self.redis_pubsub_server.pubsub(
                ignore_subscribe_messages=True)

        return self.pubsub

    def get_redis_server(self):
        if not self.redis_server:
            self.redis_server = redis.StrictRedis(**self.redis_args)

        return self.redis_server


class Client(RedisBase):
    '''Calls remote functions using Redis as a message queue.'''

    def __init__(
            self,
            message_queue,
            redis_args=None,
            timeout=60,
            transport='json'):
        self.message_queue = message_queue
        self.timeout = timeout

        if transport == 'json':
            self.transport = JSONTransport()
        elif transport == 'pickle':
            self.transport = PickleTransport()
        else:
            raise Exception('invalid transport {0}'.format(transport))

        RedisBase.__init__(self, redis_args)

    def call(self, method_name, *args, **kwargs):
        function_call = FunctionCall(method_name, args, kwargs)
        response_queue = self.message_queue + ':rpc:' + random_string()
        rpc_request = dict(
            function_call=function_call,
            response_queue=response_queue,
        )
        message = self.transport.dumps(rpc_request)
        logger.debug('RPC Request: %s' % message)
        redis_server = self.get_redis_server()

        subscribers = dict(redis_server.pubsub_numsub(self.message_queue))
        if int(subscribers[self.message_queue]) == 0:
            raise NoServerAvailableException(
                'No servers available for queue %s' % self.message_queue)

        pubsub = self.get_pubsub()
        pubsub.subscribe(response_queue)
        start = datetime.now()
        redis_server.publish(self.message_queue, message)

        while pubsub.subscribed:
            if self.timeout:
                message = pubsub.parse_response(
                    block=False, timeout=self.timeout)
            else:
                message = pubsub.parse_response(block=True)

            if message is None:
                raise TimeoutException(
                    'No response within %s seconds while waiting for %r' % (
                        self.timeout, rpc_request))

            message = pubsub.handle_message(message)
            if message and message['type'] == 'message':
                assert message['channel'] == response_queue
                response = message
                pubsub.unsubscribe(response_queue)
                pubsub.close()
                break
        else:
            raise TimeoutException(
                'No response within %s seconds while waiting for %r' % (
                    self.timeout, self.rpc_request))

        assert response['channel'] == response_queue

        logger.debug('RPC Response: %s', response['data'])

        rpc_response = self.transport.loads(response['data'])

        response_repr = dict()
        for k, v in rpc_response.items():
            if isinstance(v, (dict, list, set)):
                v = repr(v)
                if len(v) > 2000:
                    v = v[:2000] + '...'

            response_repr[k] = v
        response_repr['duration'] = str(datetime.now() - start)
        logger.info('', dict(rpc_responses=[response_repr]))

        if 'return_value' in rpc_response:
            if rpc_response.get('return_type'):
                Class_ = Response.from_name(rpc_response.get('return_type'))
            else:
                Class_ = Response

            response = Class_(rpc_response['return_value'])
        else:
            logger.warn('No return value in: %r' % rpc_response)
            response = None

        if 'exception' in rpc_response:
            if rpc_response.get('exception_type'):
                Exception = RemoteException.from_name(
                    rpc_response['exception_type'])
            else:
                Exception = RemoteException

            exception = Exception(rpc_response['exception'])
            exception.response = response
            logger.exception(rpc_response)

            raise exception
        else:
            return response

    def __getattr__(self, name):
        '''Treat missing attributes as remote method call invocations.'''
        return curry(self.call, name)


class Server(RedisBase):
    '''Executes function calls received from a Redis queue.'''

    def __init__(self, local_objects, redis_args=None):
        self.local_objects = local_objects

        RedisBase.__init__(self, redis_args)

    def run(self):
        # Flush the message queue.
        pubsub = self.get_pubsub()
        pubsub.subscribe(self.local_objects)

        for message in pubsub.listen():
            if message['type'] != 'message':
                continue

            logger.debug('RPC Request: %s' % message['data'])
            transport, rpc_request = decode_message(message['data'])
            response_queue = rpc_request['response_queue']

            function_call = FunctionCall.from_dict(
                rpc_request['function_call'])
            try:
                local_object = self.local_objects[message['channel']]
                method = getattr(local_object, function_call['name'])
                response = method(
                    *function_call['args'],
                    **function_call['kwargs'])

                rpc_response = dict(
                    return_type=type(response).__name__,
                    return_value=response,
                )
            except Exception as e:
                rpc_response = dict(
                    exception=str(e),
                    exception_type=type(e).__name__,
                )
            message = transport.dumps(rpc_response)
            logger.debug('RPC Response: %s' % message)

            self.get_redis_server().publish(response_queue, message)


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
        return json.dumps(self.__dict__)

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


class RemoteException(FromNameMixin, Exception):
    '''Raised by an RPC client when an exception occurs on the RPC server.'''
    classes = default_classes.copy()

    def __init__(self, message=None):
        if message:
            Exception.__init__(self, message)
        FromNameMixin.__init__(self)


class TimeoutException(Exception):
    '''Raised by an RPC client when a timeout occurs.'''
    pass


class NoServerAvailableException(Exception):
    pass
