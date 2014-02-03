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

from kazoo import exceptions as k_exceptions
import testtools

from zake import fake_client


class TestClient(testtools.TestCase):
    def test_connected(self):
        c = fake_client.FakeClient()
        self.assertFalse(c.connected)
        c.start()
        self.assertTrue(c.connected)

    def test_command(self):
        c = fake_client.FakeClient()
        c.start()
        self.assertTrue(c.connected)
        self.assertEqual("imok", c.command('ruok'))
        c.command('kill')
        self.assertFalse(c.connected)

    def test_version(self):
        c = fake_client.FakeClient()
        c.start()
        self.assertTrue(len(c.server_version()) > 0)
        self.assertEqual(fake_client.SERVER_VERSION,
                         c.server_version())

    def test_command_no_connect(self):
        c = fake_client.FakeClient()
        self.assertRaises(k_exceptions.KazooException, c.sync, '/')

    def test_create(self):
        c = fake_client.FakeClient()
        c.start()
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
        c = fake_client.FakeClient()
        c.start()
        self.assertRaises(k_exceptions.NoNodeError, c.delete, "/b")
        c.ensure_path("/")
        c.create("/b", b'b')
        c.delete("/b")
        self.assertRaises(k_exceptions.NoNodeError, c.delete, "/b")
        self.assertFalse(c.exists("/b"))

    def test_get_children(self):
        c = fake_client.FakeClient()
        c.start()
        c.ensure_path("/a/b")
        c.ensure_path("/a/c")
        c.ensure_path("/a/d")
        self.assertEqual(3, len(c.get_children("/a")))

    def test_exists(self):
        c = fake_client.FakeClient()
        c.start()
        c.ensure_path("/a")
        self.assertTrue(c.exists("/"))
        self.assertTrue(c.exists("/a"))

    def test_sync(self):
        c = fake_client.FakeClient()
        c.start()
        c.sync("/")

    def test_transaction(self):
        c = fake_client.FakeClient()
        c.start()
        c.ensure_path("/b")
        with c.transaction() as txn:
            txn.create("/b/c")
            txn.set_data("/b/c", b'e')
        data, znode = c.get("/b/c")
        self.assertEqual(b'e', data)
