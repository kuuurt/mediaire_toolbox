"""Work queue based on redis.
   Adapted from https://kubernetes.io/docs/tasks/job/fine-parallel-processing-work-queue/rediswq.py  # noqa
"""

import redis
import uuid
import hashlib
import logging
import time

logger = logging.getLogger(__name__)


class RedisWQ(object):
    """Simple Finite Work Queue with Redis Backend

    This work queue is finite: as long as no more work is added
    after workers start, the workers can detect when the queue
    is completely empty.

    The items in the work queue are assumed to have unique values.

    This object is not intended to be used by multiple threads
    concurrently.
    """
    def __init__(self, name, db=None, **redis_kwargs):
        """The default connection parameters are:
        host='localhost', port=6379, db=0

        The work queue is identified by "name".  The library may create other
        keys with "name" as a prefix.
        """
        if db is None:
            self._db = redis.StrictRedis(**redis_kwargs)
        else:
            self._db = db
        # The session ID will uniquely identify this "worker".
        self._session = str(uuid.uuid4())
        # Work queue is implemented as two queues: main, and processing.
        # Work is initially in main, and moved to processing when a client
        # picks it up.
        self._main_q_key = name
        self._processing_q_key = name + ":processing"
        self._error_q_key = name + ":errors"
        self._error_messages_q_key = name + ":error_messages"
        self._lease_key_prefix = name + ":leased_by_session:"

    def sessionID(self):
        """Return the ID for this session."""
        return self._session

    def _main_qsize(self):
        """Return the size of the main queue."""
        return self._db.llen(self._main_q_key)

    def _processing_qsize(self):
        """Return the size of the processing queue."""
        return self._db.llen(self._processing_q_key)

    def empty(self):
        """Return True if the queue is empty, including work being done, False
        otherwise.

        False does not necessarily mean that there is work available to work
         on right now,
        """
        return self._main_qsize() == 0 and self._processing_qsize() == 0

# TODO: implement this
#    def check_expired_leases(self):
#        """Return to the work queueReturn True if the queue is empty,
    # False otherwise."""
#        # Processing list should not be _too_ long since it is approximately
    # as long
#        # as the number of active and recently active workers.
#        processing = self._db.lrange(self._processing_q_key, 0, -1)
#        for item in processing:
#          # If the lease key is not present for an item (it expired or was 
#          # never created because the client crashed before creating it)
#          # then move the item back to the main queue so others can work on
           # it.
#          if not self._lease_exists(item):
#            TODO: transactionally move the key from processing queue to
#            to main queue, while detecting if a new lease is created
#            or if either queue is modified.

    def _itemkey(self, item):
        """Returns a string that uniquely identifies an item (bytes)."""
        return hashlib.sha224(item).hexdigest()

    def _lease_exists(self, item):
        """True if a lease on 'item' exists."""
        return self._db.exists(self._lease_key_prefix + self._itemkey(item))

    def put(self, item):
        self._db.lpush(self._main_q_key, item)

    @staticmethod
    def _get_timestamp():
        return time.gmtime(time.time()).tm_min

    def _limit_rate(self, limit):
        """
        Limits the rate of items leased in the queue. Counts the leases
        at each minute, after the limit is reached, no more leases are allowed.

        Parameters
        ----------
        limit: int
            Maximum leases per minute, None if there is no limit.

        Returns
        -------
        bool
            True if limit not reached, False if lease hit maximum rate.
        """
        if not limit:
            return True
        minute = self._get_timestamp()
        rate_key = "{}:limit:{}".format(self._session, str(minute))
        result = self._db.get(rate_key)
        if result:
            if result[0] >= limit:
                return False
        self._db.get(rate_key)
        # atomic operation
        pipe = self._db.pipeline()
        pipe.incr(rate_key)
        pipe.expire(rate_key, 60)
        pipe.execute()
        return True

    # for now `lease_secs` is useless!
    def lease(self, lease_secs=5, block=True, timeout=None, limit=None):
        """Begin working on an item the work queue.

        Lease the item for lease_secs.  After that time, other
        workers may consider this client to have crashed or stalled
        and pick up the item instead.

        If optional args block is true and timeout is None (the default), block
        if necessary until an item is available."""
        if block:
            item = self._db.brpoplpush(self._main_q_key,
                                       self._processing_q_key, timeout=timeout)
        else:
            item = self._db.rpoplpush(self._main_q_key, self._processing_q_key)
        if item:
            # Record that we (this session id) are working on a key.  Expire
            # that
            # note after the lease timeout.
            # Note: if we crash at this line of the program, then GC will
            # see no lease
            # for this item a later return it to the main queue.
            while True:
                if self._limit_rate(limit):
                    break
                time.sleep(10)
            itemkey = self._itemkey(item)
            logger.info('{} -> {}'.format(self._lease_key_prefix + itemkey,
                                          self._session))
            self._db.setex(self._lease_key_prefix + itemkey, lease_secs,
                           self._session)
        return item

    def error(self, value, msg=None):
        """Handle the case when processing of the item with 'value' failed.

        Optionally provide error message `msg`.
        """
        itemkey = self._itemkey(value)
        if msg is None:
            msg = 'unknown error'
        logger.info("{}: Trying to move '{}' to '{}'".format(msg, itemkey,
                                                             self._error_q_key)
                    )
        exit_code = self._db.lrem(self._processing_q_key, 0, value)
        logger.debug("exit code: {}".format(exit_code))
        if exit_code == 0:
            logger.error("Could not find '{}' in '{}'".format(
                itemkey, self._processing_q_key))
        else:
            len_errors = self._db.lpush(self._error_q_key, value)
            len_msgs = self._db.lpush(self._error_messages_q_key,
                                      msg.encode('utf-8'))
            logger.debug("Moved '{}' to '{}'".format(itemkey,
                                                     self._error_q_key))
            assert len_errors == len_msgs

    def complete(self, value):
        """Complete working on the item with 'value'.

        If the lease expired, the item may not have completed, and some
        other worker may have picked it up.  There is no indication
        of what happened.
        """
        self._db.lrem(self._processing_q_key, 0, value)
        # If we crash here, then the GC code will try to move the value,
        # but it will
        # not be here, which is fine.  So this does not need to be a
        # transaction.
        itemkey = self._itemkey(value)
        self._db.delete(self._lease_key_prefix + itemkey, self._session)

    @staticmethod
    def get_all_queues_from_config(appconfig: dict, redis_args: dict):
        queues = {
            q_identifier: RedisWQ(q_key, **redis_args)
            for q_identifier, q_key in appconfig['shared']['queues'].items()
        }
        return queues


# TODO: add functions to clean up all keys associated with "name" when
# processing is complete.

# TODO(etune): finish code to GC expired leases, and call periodically
#  e.g. each time lease times out.
# edit: each leased item generates a leased_key dicom_folders:leased_
# by_session:item_key
# with the value of the session. One could periodically check if all items in
# the processing q
# have a leased_key. If not, these are expired leases and should be put back
# into the main q

# TODO: error handling, introduce error queue: when a known error
# during processing occurs, put item in
# error queue and remove it form processing queue (`complete()`).
# Manually check error queue
