# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2014 Yahoo! Inc. All Rights Reserved.
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
import contextlib

from kazoo import exceptions as k_exceptions
from kazoo.recipe import watchers as k_watchers

from zake import fake_client
from zake import test


@contextlib.contextmanager
def start_close(client):
    client.start()
    try:
        yield client
    finally:
        client.close()


class TestClient(test.Test):
    def setUp(self):
        super(TestClient, self).setUp()
        self.client = fake_client.FakeClient()

    def test_connected(self):
        self.assertFalse(self.client.connected)
        with start_close(self.client) as c:
            self.assertTrue(c.connected)

    def test_command(self):
        with start_close(self.client) as c:
            self.assertTrue(c.connected)
            self.assertEqual("imok", c.command('ruok'))
            self.client.command('kill')
            self.assertFalse(c.connected)

    def test_root(self):
        with start_close(self.client) as c:
            self.assertTrue(c.exists("/"))

    def test_version(self):
        with start_close(self.client) as c:
            self.assertTrue(len(c.server_version()) > 0)
            self.assertEqual(fake_client.SERVER_VERSION,
                             c.server_version())

    def test_sequence(self):
        with start_close(self.client) as c:
            path = c.create("/", sequence=True)
            self.assertEqual("/0000000000", path)
            path = c.create("/", sequence=True)
            self.assertEqual("/0000000001", path)
            children = c.get_children("/")
            self.assertEqual(2, len(children))
            seqs = c.storage.sequences
            self.assertEqual(1, len(seqs))
            self.assertEqual(2, seqs['/'])

    def test_command_no_connect(self):
        self.assertRaises(k_exceptions.KazooException, self.client.sync, '/')

    def test_create(self):
        with start_close(self.client) as c:
            c.ensure_path("/b")
            c.create('/b/c', b'abc')
            paths = c.storage.paths
            self.assertEqual(3, len(paths))
            self.assertEqual(1, len(c.storage.get_children("/b")))
            self.assertEqual(1, len(c.storage.get_parents("/b")))
            data, znode = c.get("/b/c")
            self.assertEqual(b'abc', data)
            self.assertEqual(0, znode.version)
            c.set("/b/c", b"efg")
            data, znode = c.get("/b/c")
            self.assertEqual(b"efg", data)
            self.assertEqual(1, znode.version)

    def test_delete(self):
        with start_close(self.client) as c:
            self.assertRaises(k_exceptions.NoNodeError, c.delete, "/b")
            c.ensure_path("/")
            c.create("/b", b'b')
            c.delete("/b")
            self.assertRaises(k_exceptions.NoNodeError, c.delete, "/b")
            self.assertFalse(c.exists("/b"))

    def test_get_children(self):
        with start_close(self.client) as c:
            c.ensure_path("/a/b")
            c.ensure_path("/a/c")
            c.ensure_path("/a/d")
            self.assertEqual(3, len(c.get_children("/a")))

    def test_exists(self):
        with start_close(self.client) as c:
            c.ensure_path("/a")
            self.assertTrue(c.exists("/"))
            self.assertTrue(c.exists("/a"))

    def test_sync(self):
        with start_close(self.client) as c:
            c.sync("/")

    def test_transaction(self):
        with start_close(self.client) as c:
            c.ensure_path("/b")
            with c.transaction() as txn:
                txn.create("/b/c")
                txn.set_data("/b/c", b'e')
            data, znode = c.get("/b/c")
            self.assertEqual(b'e', data)
            self.assertTrue(txn.committed)

    def test_data_watch(self):
        updates = collections.deque()

        def _notify_me(data, stat):
            updates.append((data, stat))

        with start_close(self.client) as c:
            c.ensure_path("/b")
            k_watchers.DataWatch(c, "/b", func=_notify_me)
            c.set("/b", b"1")
            c.flush()
            c.set("/b", b"2")
            c.flush()

        self.assertEqual(3, len(updates))
