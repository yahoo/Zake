# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2013 Yahoo! Inc. All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import collections
import functools
import logging
import time

from concurrent import futures
import six

from kazoo import exceptions as k_exceptions
from kazoo.handlers import threading as k_threading
from kazoo.protocol import paths as k_paths
from kazoo.protocol import states as k_states
from kazoo.recipe import watchers as k_watchers

from zake import fake_storage as fs
from zake import utils
from zake import version

LOG = logging.getLogger(__name__)

# We provide a basic txn support (not as functional as zookeeper) and this
# was added in 3.4.0 so we will say we are 3.4.0 compat (until proven
# differently).
SERVER_VERSION = (3, 4, 0)

_NO_ACL_MSG = "ACLs not currently supported"


def _make_cb(func, args, type=''):
    return k_states.Callback(type=type, func=func, args=args)


class SequentialThreadingHandler(k_threading.SequentialThreadingHandler):
    def __init__(self, spawn_workers):
        super(SequentialThreadingHandler, self).__init__()
        self._spawner = futures.ThreadPoolExecutor(max_workers=spawn_workers)

    def spawn(self, func, *args, **kwargs):
        return self._spawner.submit(func, *args, **kwargs)


class FakeClient(object):
    """A fake mostly functional/good enough kazoo compat. client

    It can have its underlying storage mocked out (as well as exposes the
    listeners that are currently active and the watches that are currently
    active) so that said functionality can be examined & introspected by
    testing frameworks (while in use and after the fact).
    """

    def __init__(self, handler=None,
                 storage=None, server_version=None,
                 handler_spawn_workers=1):
        self._listeners = set()
        self._child_watches = collections.defaultdict(list)
        self._data_watches = collections.defaultdict(list)
        if handler:
            self.handler = handler
            self._stop_handler = False
        else:
            self.handler = SequentialThreadingHandler(handler_spawn_workers)
            self._stop_handler = True
        if storage is not None:
            self.storage = storage
        else:
            self.storage = fs.FakeStorage(lock=self.handler.rlock_object())
        self._lock = self.handler.rlock_object()
        self._child_watches_lock = self.handler.rlock_object()
        self._data_watches_lock = self.handler.rlock_object()
        self._connected = False
        if server_version is None:
            self._server_version = SERVER_VERSION
        else:
            self._server_version = tuple(server_version)
            if not len(self._server_version):
                raise ValueError("Non-empty server version expected")
        self.expired = False
        self.logger = LOG
        # Helper objects that makes these easier to create.
        self.ChildrenWatch = functools.partial(k_watchers.ChildrenWatch, self)
        self.DataWatch = functools.partial(k_watchers.DataWatch, self)

    def command(self, cmd=b'ruok'):
        self.verify()
        if cmd == 'ruok':
            return 'imok'
        if cmd == 'stat':
            server_version = ".".join([str(s) for s in self._server_version])
            return "\n".join(['Zake the fake version: %s' % (version.VERSION),
                              'Mimicked version: %s' % (server_version),
                              'Mode: standalone'])
        if cmd == "kill":
            self.stop()
        return ''

    def verify(self):
        if not self.connected:
            raise k_exceptions.ConnectionClosedError("Connection has been"
                                                     " closed")
        if self.expired:
            raise k_exceptions.SessionExpiredError("Expired")

    @property
    def timeout_exception(self):
        return IOError

    @property
    def child_watches(self):
        return self._child_watches

    @property
    def data_watches(self):
        return self._data_watches

    @property
    def listeners(self):
        return self._listeners

    @property
    def connected(self):
        return self._connected

    def sync(self, path):
        self.verify()
        if not isinstance(path, six.string_types):
            raise TypeError("path must be a string")

    def server_version(self):
        self.verify()
        return self._server_version

    def flush(self):
        self.verify()

        # This puts an item into the callback queue, and waits until it gets
        # called, this is a cheap way of knowing that the queue has been
        # cycled over (as this item goes in on the bottom) and only when the
        # items ahead of this callback are finished will this get called.
        wait_for = self.handler.event_object()
        fired = False

        def flip():
            wait_for.set()

        while not wait_for.is_set():
            if not fired:
                self.handler.dispatch_callback(_make_cb(flip, []))
                fired = True
            time.sleep(0.001)

    def create(self, path, value=b"", acl=None,
               ephemeral=False, sequence=False, makepath=False):
        self.verify()
        if not isinstance(path, six.string_types):
            raise TypeError("path must be a string")
        if not isinstance(value, six.binary_type):
            raise TypeError("value must be a byte string")
        if acl:
            raise NotImplementedError(_NO_ACL_MSG)

        with self.storage.lock:
            path = k_paths.normpath(path)
            if makepath:
                for p in utils.partition_path(path)[0:-1]:
                    if not self.exists(p):
                        self.create(p)
            created, parents, path = self.storage.create(path,
                                                         value=value,
                                                         sequence=sequence)

        # Fire off child notifications that this node was created.
        if parents:
            event = k_states.WatchedEvent(type=k_states.EventType.CHILD,
                                          state=k_states.KeeperState.CONNECTED,
                                          path=path)
            self._fire_watches(parents, event,
                               self._child_watches,
                               self._child_watches_lock)
        # Fire off data notifications that this node was created.
        if created:
            event = k_states.WatchedEvent(type=k_states.EventType.CREATED,
                                          state=k_states.KeeperState.CONNECTED,
                                          path=path)
            self._fire_watches([path], event,
                               self._data_watches,
                               self._data_watches_lock)
        return path

    def create_async(self, path, value=b"", acl=None,
                     ephemeral=False, sequence=False, makepath=False):
        return self._generate_async(self.create, path, value=value,
                                    acl=acl, ephemeral=ephemeral,
                                    sequence=sequence, makepath=makepath)

    def get(self, path, watch=None):
        self.verify()
        if not isinstance(path, six.string_types):
            raise TypeError("path must be a string")
        path = k_paths.normpath(path)
        try:
            (data, znode) = self.storage.get(path)
        except KeyError:
            raise k_exceptions.NoNodeError("No path %s" % (path))
        if watch:
            with self._data_watches_lock:
                self._data_watches[path].append(watch)
        return (data, znode)

    def set_acls(self, path, acls, version=-1):
        raise NotImplementedError(_NO_ACL_MSG)

    def set_acls_async(self, path, acls, version=-1):
        raise NotImplementedError(_NO_ACL_MSG)

    def get_acls_async(self, path):
        raise NotImplementedError(_NO_ACL_MSG)

    def get_acls(self, path):
        raise NotImplementedError(_NO_ACL_MSG)

    def get_async(self, path, watch=None):
        return self._generate_async(self.get, path, watch=watch)

    def start(self, timeout=None):
        with self._lock:
            if not self._connected:
                self.handler.start()
                self._connected = True
                self._fire_state_change(k_states.KazooState.CONNECTED)

    def restart(self):
        self.stop()
        self.start()

    def _fire_state_change(self, state):
        with self._lock:
            listeners = list(self._listeners)
        for func in listeners:
            self.handler.dispatch_callback(_make_cb(func, [state]))

    def _generate_async(self, func, *args, **kwargs):
        async_result = self.handler.async_result()

        def call(func, args, kwargs):
            try:
                result = func(*args, **kwargs)
                async_result.set(result)
                return result
            except Exception as exc:
                async_result.set_exception(exc)

        cb = _make_cb(call, [func, args, kwargs], type='async')
        self.handler.dispatch_callback(cb)
        return async_result

    def exists(self, path, watch=None):
        self.verify()
        if not isinstance(path, six.string_types):
            raise TypeError("path must be a string")
        path = k_paths.normpath(path)
        try:
            exists = bool(self.get(path)[1])
        except k_exceptions.NoNodeError:
            exists = None
        if watch:
            with self._data_watches_lock:
                self._data_watches[path].append(watch)
        return exists

    def exists_async(self, path, watch=None):
        return self._generate_async(self.exists, path, watch=watch)

    def set(self, path, value, version=-1):
        self.verify()
        if not isinstance(path, six.string_types):
            raise TypeError("path must be a string")
        if not isinstance(value, six.binary_type):
            raise TypeError("value must be a byte string")
        if not isinstance(version, int):
            raise TypeError("version must be an int")

        path = k_paths.normpath(path)
        try:
            stat = self.storage.set(path, value, version=version)
        except KeyError:
            raise k_exceptions.NoNodeError("No path %s" % (path))

        # Fire any attached watches.
        event = k_states.WatchedEvent(type=k_states.EventType.CHANGED,
                                      state=k_states.KeeperState.CONNECTED,
                                      path=path)
        self._fire_watches([path], event,
                           self._data_watches,
                           self._data_watches_lock)
        return stat

    def set_async(self, path, value, version=-1):
        return self._generate_async(self.set, path, value, version=version)

    def get_children(self, path, watch=None, include_data=False):
        self.verify()
        if not isinstance(path, six.string_types):
            raise TypeError("path must be a string")

        def _clean(p):
            return p.strip("/")

        path = k_paths.normpath(path)
        paths = self.storage.get_children(path)
        if watch:
            with self._child_watches_lock:
                self._child_watches[path].append(watch)
        if include_data:
            children_with_data = []
            for (p, data) in six.iteritems(paths):
                children_with_data.append(_clean(p[len(path):]), data)
            return children_with_data
        else:
            children = []
            for p in list(six.iterkeys(paths)):
                children.append(_clean(p[len(path):]))
            return children

    def get_children_async(self, path, watch=None, include_data=False):
        return self._generate_async(self.get_children, path,
                                    watch=watch, include_data=include_data)

    def stop(self):
        with self._lock:
            if not self._connected:
                return
            self._fire_state_change(k_states.KazooState.LOST)
            if self._stop_handler:
                self.handler.stop()
            self._listeners.clear()
            self._child_watches.clear()
            self._data_watches.clear()
            self._connected = False

    def delete(self, path, recursive=False):
        self.verify()
        if not isinstance(path, six.string_types):
            raise TypeError("path must be a string")

        path = k_paths.normpath(path)
        with self.storage.lock:
            if path not in self.storage:
                raise k_exceptions.NoNodeError("No path %s" % (path))
            if recursive:
                paths = [path]
                children = self.storage.get_children(path, only_direct=False)
                for p in six.iterkeys(children):
                    paths.append(p)
            else:
                children = self.storage.get_children(path, only_direct=False)
                if children:
                    raise k_exceptions.NotEmptyError("Path %s is not-empty"
                                                     " (%s children exist)"
                                                     % (path, len(children)))
                paths = [path]
            paths = list(reversed(sorted(set(paths))))
            for p in paths:
                self.storage.pop(p)
            parents = []
            for p in paths:
                parents.extend(self.storage.get_parents(p))
            parents = list(reversed(sorted(set(parents))))
            # Fire off parent notifications that these paths were removed.
            for p in parents:
                event = k_states.WatchedEvent(
                    type=k_states.EventType.DELETED,
                    state=k_states.KeeperState.CONNECTED,
                    path=p)
                self._fire_watches([p], event,
                                   self._child_watches,
                                   self._child_watches_lock)
            # Fire off data notifications that these paths were removed.
            for p in paths:
                event = k_states.WatchedEvent(
                    type=k_states.EventType.DELETED,
                    state=k_states.KeeperState.CONNECTED,
                    path=p)
                self._fire_watches([p], event,
                                   self._data_watches,
                                   self._data_watches_lock)
            return True

    def delete_async(self, path, recursive=False):
        return self._generate_async(self.delete, path, recursive=recursive)

    def add_listener(self, listener):
        with self._lock:
            self._listeners.add(listener)

    def retry(self, func, *args, **kwargs):
        self.verify()
        return func(*args, **kwargs)

    def remove_listener(self, listener):
        with self._lock:
            self._listeners.discard(listener)

    def _fire_watches(self, paths, event, watch_source, watch_mutate_lock):
        for p in reversed(sorted(set(paths))):
            with watch_mutate_lock:
                watches = list(watch_source.pop(p, []))
            for w in watches:
                self.handler.dispatch_callback(_make_cb(w, [event]))

    def transaction(self):
        return FakeTransactionRequest(self)

    def ensure_path(self, path):
        self.verify()
        if not isinstance(path, six.string_types):
            raise TypeError("path must be a string")

        path = k_paths.normpath(path)
        path_pieces = utils.partition_path(path)
        for p in path_pieces:
            try:
                self.create(p)
            except k_exceptions.NodeExistsError:
                pass

    def ensure_path_async(self, path):
        return self._generate_async(self.ensure_path, path)

    def close(self):
        self._connected = False


class StopTransaction(Exception):
    pass


class FakeTransactionRequest(object):
    def __init__(self, client):
        self.client = client
        self.operations = []
        self.committed = False

    def delete(self, path, version=-1):
        self.operations.append((self.client.delete, [path, version]))

    def check(self, path, version):
        if not isinstance(path, six.string_types):
            raise TypeError("path must be a string")
        if not isinstance(version, int):
            raise TypeError("version must be an int")

        def apply_check(path, version):
            try:
                data = self.client.storage[path]
                if data['version'] != version:
                    raise StopTransaction()
            except KeyError:
                raise StopTransaction()

        self.operations.append((apply_check, [path, version]))

    def set_data(self, path, value, version=-1):
        self.operations.append((self.client.set, [path, value, version]))

    def create(self, path, value=b"", acl=None, ephemeral=False,
               sequence=False):
        self.operations.append((self.client.create,
                                [path, value, acl, ephemeral, sequence]))

    def commit(self):
        if self.committed:
            raise ValueError('Transaction already committed')
        with self.client.storage.lock:
            self.client.verify()
            results = []
            failed = False
            # Currently we aren't undoing things, that's ok for now, but it
            # can leave the storage in a bad state when things go wrong...
            for (f, args) in self.operations:
                try:
                    results.append(f(*args))
                except StopTransaction:
                    failed = True
                    break
            if failed:
                self.committed = False
                return []
            else:
                self.committed = True
                return results

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        if not any((type, value, tb)):
            if not self.committed:
                self.commit()
