import unittest
import tempfile
import shutil

from mediaire_toolbox.queue.daemon import QueueDaemon
from mediaire_toolbox.queue.redis_wq import RedisWQ
from mediaire_toolbox.queue.tasks import Task


class FooDaemon(QueueDaemon):

    def process_task(self, _):
        self.processed = True


class FooFailingDaemon(QueueDaemon):

    def process_task(self, _):
        raise Exception("I fail")


class MockQueue(RedisWQ):

    def __init__(self):
        super().__init__("", None)
        self.serialized_task = Task(t_id=1,
                                    tag='tag',
                                    data={'t1': 'foo', 't2': 'bar', 'out': 'foo'}).to_bytes()
        self.completed = False
        self.error_msg = None

    def lease(self, lease_secs=5, block=True, timeout=None):
        return self.serialized_task

    def error(self, value, msg=None):
        self.error_msg = msg

    def put(self, item):
        self.put_item = item

    def complete(self, item):
        if item == self.serialized_task:
            self.completed = True


class TestDaemon(unittest.TestCase):

    def setUp(self):
        self.data_dir = tempfile.mkdtemp(suffix='_test_daemon_')
        self.input_queue = MockQueue()
        self.result_queue = self.input_queue
        self.foo_daemon = FooDaemon(self.input_queue,
                                    self.result_queue,
                                    60 * 30,
                                    'foo',
                                    {'data_dir': self.data_dir})

    def tearDown(self):
        shutil.rmtree(self.data_dir)

    def test_daemon_process_ok(self):
        self.foo_daemon.run_once()

        self.assertTrue(self.input_queue.completed)
        self.assertTrue(self.foo_daemon.processed)
        self.assertTrue(not self.input_queue.error_msg)

    def test_daemon_deserialization_error(self):
        self.input_queue.serialized_task = "whatever".encode('utf-8')
        self.foo_daemon.run_once()

        self.assertFalse(self.input_queue.completed)
        self.assertTrue(self.input_queue.error_msg)

    def test_daemon_processing_error_with_t_id(self):
        self.foo_daemon = FooFailingDaemon(self.input_queue,
                                           self.result_queue,
                                           60 * 30,
                                           'foo',
                                           {'data_dir': self.data_dir})

        self.transaction_failed = False
        self.foo_daemon.run_once()

        self.assertFalse(self.input_queue.completed)
        self.assertFalse(self.input_queue.error_msg)
        self.assertTrue(self.result_queue.put_item and
                        Task().read_bytes(self.result_queue.put_item).error)
