# -*- coding: utf-8 -*-

import os
import sys
import signal
import tempfile
from mock import patch
import testing.elasticsearch
from elasticsearch import Elasticsearch
from time import sleep
from shutil import rmtree

if sys.version_info < (2, 7):
    import unittest2 as unittest
else:
    import unittest


class TestElasticsearch(unittest.TestCase):
    def test_basic(self):
        try:
            # start elasticsearch server
            es = testing.elasticsearch.Elasticsearch()
            self.assertIsNotNone(es)
            params = es.dsn()
            self.assertEqual(['127.0.0.1:%d' % es.elasticsearch_yaml['http.port']], params['hosts'])

            # connect to elasticsearch (w/ elasticsearch-py)
            elasticsearch = Elasticsearch(**es.dsn())
            self.assertIsNotNone(elasticsearch)
            self.assertRegexpMatches(es.read_bootlog(), '\[INFO \]\[node                     \] \[.*?\] started')
        finally:
            # shutting down
            pid = es.server_pid
            self.assertTrue(es.is_alive())

            es.stop()
            sleep(1)

            self.assertFalse(es.is_alive())
            with self.assertRaises(OSError):
                os.kill(pid, 0)  # process is down

    def test_stop(self):
        # start elasticsearch server
        es = testing.elasticsearch.Elasticsearch()
        self.assertTrue(os.path.exists(es.base_dir))
        self.assertTrue(es.is_alive())

        # call stop()
        es.stop()
        self.assertFalse(os.path.exists(es.base_dir))
        self.assertFalse(es.is_alive())

        # call stop() again
        es.stop()
        self.assertFalse(os.path.exists(es.base_dir))
        self.assertFalse(es.is_alive())

        # delete elasticsearch object after stop()
        del es

    def test_dsn_and_url(self):
        es = testing.elasticsearch.Elasticsearch(port=12345, auto_start=0)
        self.assertEqual({'hosts': ['127.0.0.1:12345']}, es.dsn())

    def test_with_statement(self):
        with testing.elasticsearch.Elasticsearch() as es:
            self.assertIsNotNone(es)

            # connect to elasticsearch
            elasticsearch = Elasticsearch(**es.dsn())
            self.assertIsNotNone(elasticsearch)

            self.assertTrue(es.is_alive())

        self.assertFalse(es.is_alive())

    def test_multiple_elasticsearch(self):
        es1 = testing.elasticsearch.Elasticsearch()
        es2 = testing.elasticsearch.Elasticsearch()
        self.assertNotEqual(es1.server_pid, es2.server_pid)

        self.assertTrue(es1.is_alive())
        self.assertTrue(es2.is_alive())

    @patch('testing.elasticsearch.find_elasticsearch_home')
    def test_elasticsearch_is_not_found(self, find_elasticsearch_home):
        find_elasticsearch_home.side_effect = RuntimeError

        with self.assertRaises(RuntimeError):
            testing.elasticsearch.Elasticsearch()

    def test_fork(self):
        es = testing.elasticsearch.Elasticsearch()
        if os.fork() == 0:
            del es
            es = None
            os.kill(os.getpid(), signal.SIGTERM)  # exit tests FORCELY
        else:
            os.wait()
            sleep(1)
            self.assertTrue(es.is_alive())  # process is alive (delete es obj in child does not effect)

    def test_stop_on_child_process(self):
        es = testing.elasticsearch.Elasticsearch()
        if os.fork() == 0:
            es.stop()
            os.kill(es.server_pid, 0)  # process is alive (calling stop() is ignored)
            os.kill(os.getpid(), signal.SIGTERM)  # exit tests FORCELY
        else:
            os.wait()
            sleep(1)
            self.assertTrue(es.is_alive())  # process is alive (calling stop() in child is ignored)

    def test_copy_data_from(self):
        try:
            tmpdir = tempfile.mkdtemp()

            # create new database
            with testing.elasticsearch.Elasticsearch(base_dir=tmpdir) as es1:
                elasticsearch1 = Elasticsearch(**es1.dsn())
                elasticsearch1.index(index='greetings',
                                     doc_type='message',
                                     id=1,
                                     body={"Hello": "world"})

            data_dir = os.path.join(tmpdir, 'data')
            with testing.elasticsearch.Elasticsearch(copy_data_from=data_dir) as es2:
                elasticsearch2 = Elasticsearch(**es2.dsn())
                response = elasticsearch2.get(index='greetings', doc_type='message', id=1)
                self.assertEqual({"Hello": "world"}, response['_source'])
        finally:
            rmtree(tmpdir)

    def test_skipIfNotInstalled_found(self):
        @testing.elasticsearch.skipIfNotInstalled
        def testcase():
            pass

        self.assertEqual(False, hasattr(testcase, '__unittest_skip__'))
        self.assertEqual(False, hasattr(testcase, '__unittest_skip_why__'))

    @patch('testing.elasticsearch.find_elasticsearch_home')
    def test_skipIfNotInstalled_notfound(self, find_elasticsearch_home):
        find_elasticsearch_home.side_effect = RuntimeError

        @testing.elasticsearch.skipIfNotInstalled
        def testcase():
            pass

        self.assertEqual(True, hasattr(testcase, '__unittest_skip__'))
        self.assertEqual(True, hasattr(testcase, '__unittest_skip_why__'))
        self.assertEqual(True, testcase.__unittest_skip__)
        self.assertEqual("Elasticsearch not found", testcase.__unittest_skip_why__)

    def test_skipIfNotInstalled_with_args_found(self):
        path = testing.elasticsearch.find_elasticsearch_home()

        @testing.elasticsearch.skipIfNotInstalled(path)
        def testcase():
            pass

        self.assertEqual(False, hasattr(testcase, '__unittest_skip__'))
        self.assertEqual(False, hasattr(testcase, '__unittest_skip_why__'))

    def test_skipIfNotInstalled_with_args_notfound(self):
        @testing.elasticsearch.skipIfNotInstalled("/path/to/anywhere")
        def testcase():
            pass

        self.assertEqual(True, hasattr(testcase, '__unittest_skip__'))
        self.assertEqual(True, hasattr(testcase, '__unittest_skip_why__'))
        self.assertEqual(True, testcase.__unittest_skip__)
        self.assertEqual("Elasticsearch not found", testcase.__unittest_skip_why__)

    def test_skipIfNotFound_found(self):
        @testing.elasticsearch.skipIfNotFound
        def testcase():
            pass

        self.assertEqual(False, hasattr(testcase, '__unittest_skip__'))
        self.assertEqual(False, hasattr(testcase, '__unittest_skip_why__'))

    @patch('testing.elasticsearch.find_elasticsearch_home')
    def test_skipIfNotFound_notfound(self, find_elasticsearch_home):
        find_elasticsearch_home.side_effect = RuntimeError

        @testing.elasticsearch.skipIfNotFound
        def testcase():
            pass

        self.assertEqual(True, hasattr(testcase, '__unittest_skip__'))
        self.assertEqual(True, hasattr(testcase, '__unittest_skip_why__'))
        self.assertEqual(True, testcase.__unittest_skip__)
        self.assertEqual("Elasticsearch not found", testcase.__unittest_skip_why__)
