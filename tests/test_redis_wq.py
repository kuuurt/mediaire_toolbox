import unittest
from unittest import mock
from unittest.mock import patch

from mediaire_toolbox.queue.redis_wq import RedisWQ
class MockRedis():
    def __init__(self):
        self.hashmap = {}
        self.expiremap = {}

    def incr(self, key):
        if key in self.hashmap:
            self.hashmap[key] = self.hashmap[key] + 1
        else:
            self.hashmap[key] = 1
        return [self.hashmap[key]]

    def get(self, key):
        if key in self.hashmap:
            return [self.hashmap[key]]
        else:
            return None

    def expire(self, key, time):
        self.expiremap[key] = time

    def execute(self):
        pass

    def pipeline(self):
        return self

    def rpoplpush(self, src, dst):
        value = self.hashmap[src].pop()
        self.hashmap[dst].append(value)
        return value

    def setex(self, *args):
        pass


class TestRedisWQ(unittest.TestCase):
    def test_limit_rate(self):
        """Test that the limit rate function limits the rate"""
        time = 0
        get_time = lambda: time
        mock_redis = MockRedis()
        r_wq = RedisWQ(name='mock_limit_rate', db=mock_redis)
        # test when no limit, leasing object
        self.assertTrue(r_wq._limit_rate(None))
        with mock.patch.object(RedisWQ, '_get_timestamp') as mock_get_timestamp:
            mock_get_timestamp.return_value = get_time()
            for i in range(5):
                self.assertTrue(r_wq._limit_rate(5))
            self.assertFalse(r_wq._limit_rate(5))
            self.assertTrue(len(mock_redis.expiremap.items()) == 1)

    def test_limit_rate_reset(self):
        """Test that the limit counter refreshes in the next time bucket"""
        mock_redis = MockRedis()
        r_wq = RedisWQ(name='mock_limit_rate', db=mock_redis)
        with mock.patch.object(RedisWQ, '_get_timestamp') as mock_get_timestamp:
            mock_get_timestamp.side_effect = [0, 1]
            self.assertTrue(r_wq._limit_rate(1))
            self.assertTrue(r_wq._limit_rate(1))
            self.assertTrue(len(mock_redis.expiremap.items()) == 2)

    def test_lease(self):
        mock_redis = MockRedis()
        r_wq = RedisWQ(name='mock_limit_rate', db=mock_redis)
        mock_redis.hashmap[r_wq._main_q_key] = [1, 2]
        mock_redis.hashmap[r_wq._processing_q_key] = []
        with mock.patch('time.sleep') as mock_sleep, \
             mock.patch.object(RedisWQ, '_get_timestamp') as mock_get_timestamp, \
             mock.patch.object(RedisWQ, '_itemkey') as  mock_item_key:
            mock_sleep.return_value = lambda: None
            mock_get_timestamp.side_effect = [0, 0, 1]
            mock_item_key = lambda: "mock"
            self.assertTrue(r_wq.lease(block=False, limit=1) == 2)
            self.assertTrue(r_wq.lease(block=False, limit=1) == 1)
            self.assertTrue(mock_sleep.call_count == 1)
