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

import threading

import six

from zake import utils


class FakeStorage(object):
    """A place too place fake zookeeper paths + data."""

    def __init__(self, lock=None, paths=None):
        if paths:
            self._paths = dict(paths)
        else:
            self._paths = {}
        if lock is None:
            lock = threading.RLock()
        self.lock = lock

    @property
    def paths(self):
        with self.lock:
            return dict(self._paths)

    def pop(self, path):
        with self.lock:
            self._paths.pop(path)

    def __setitem__(self, path, value):
        with self.lock:
            self._paths[path] = value

    def __getitem__(self, path):
        with self.lock:
            return self._paths[path]

    def __contains__(self, path):
        with self.lock:
            return path in self._paths

    def get_children(self, path, only_direct=True):
        paths = {}
        with self.lock:
            for (k, v) in list(six.iteritems(self._paths)):
                if utils.is_child_path(path, k, only_direct=only_direct):
                    paths[k] = v
        return paths

    def get_parents(self, path):
        paths = {}
        with self.lock:
            for (k, v) in list(six.iteritems(self._paths)):
                if utils.is_child_path(k, path, only_direct=False):
                    paths[k] = v
        return paths
