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

import os
import time

from kazoo.protocol import paths as k_paths
from kazoo.protocol import states as k_states


def millitime():
    """Converts the current time to milliseconds."""
    return int(round(time.time() * 1000.0))


def normpath(path):
    """Really normalize the path by adding a missing leading slash."""
    path = k_paths.normpath(path)
    if not path.startswith('/'):
        return '/' + path
    return path


def make_cb(func, args=None, type=''):
    if not args:
        args = []
    return k_states.Callback(type=type, func=func, args=args)


def partition_path(path):
    path_pieces = [path]
    cur_path = path
    while True:
        (tmp_path, _ext) = os.path.split(cur_path)
        if tmp_path == cur_path:
            path_pieces.append(tmp_path)
            break
        else:
            path_pieces.append(tmp_path)
            cur_path = tmp_path
    return sorted(set(path_pieces))


def is_child_path(parent_path, child_path, only_direct=True):
    parent_pieces = [p for p in parent_path.split("/") if p]
    child_pieces = [p for p in child_path.split("/") if p]
    if len(child_pieces) <= len(parent_pieces):
        return False
    shared_pieces = child_pieces[0:len(parent_pieces)]
    if tuple(parent_pieces) != tuple(shared_pieces):
        return False
    if only_direct:
        return len(child_pieces) == len(parent_pieces) + 1
    return True
