import time
import json

from copy import deepcopy


class Task(object):
    """Defines task objects that can be handled by the task manager."""

    def __init__(self, t_id=None, tag=None, data=None,
                 timestamp=None, update_timestamp=None, error=None):
        """Initializes the Task object.

        Parameters
        ----------
        t_id: int
            transaction id this task belongs to
        tag: str
            String specifying the task. Unique for each task.
        data: dict
        timestamp: float
            Timestamp of task creation from`time.time()`
        update_timestamp: float
            Timestamp of task update (via `create_child()`) from `time.time()`
        error: str
            a serialized error string in case the task failed while executing
        """
        self.t_id = t_id
        self.tag = tag
        self.timestamp = timestamp or int(time.time())
        self.update_timestamp = update_timestamp
        self.data = data
        self.error = error
        # self.update = None

    def to_dict(self):
        return {'tag': self.tag,
                'timestamp': self.timestamp,
                'update_timestamp': self.update_timestamp,
                'data': self.data,
                't_id': self.t_id,
                'error': self.error}

    def to_json(self):
        return json.dumps(self.to_dict())

    def to_bytes(self):
        return self.to_json().encode('utf-8')

    def read_dict(self, d):
        tag = d['tag']
        timestamp = d['timestamp']
        t_id = d.get('t_id', None)
        update_timestamp = d.get('update_timestamp', None)
        data = d.get('data', None)
        error = d.get('error', None)
        self.__init__(t_id=t_id, tag=tag, data=data,
                      timestamp=timestamp, update_timestamp=update_timestamp,
                      error=error)
        return self

    def read_bytes(self, bytestring):
        d = json.loads(bytestring.decode('utf-8'))
        self.read_dict(d)
        return self

    def read_json(self, json_path):
        with open(json_path, 'r') as f:
            d = json.load(f)
        self.read_dict(d)
        return self

    def create_child(self, tag=None):
        """Creates and returns a follow up task object."""
        if tag is None:
            tag = self.tag + '__child'
        child_task = deepcopy(self)
        child_task.tag = tag
        child_task.update_timestamp = int(time.time())
        return child_task

    def get_subject_name(self):
        """Get the subject name of the dicom_header"""
        try:
            t1_header = self.data['dicom_info']['t1']['header']
            return t1_header['PatientName']
        except ValueError:
            return None


    def __str__(self):
        return str(self.to_dict())

    def __repr__(self):
        return str(self.to_dict())