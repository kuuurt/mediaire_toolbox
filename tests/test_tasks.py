import unittest
from copy import deepcopy

from mediaire_toolbox.queue.tasks import Task


class TestTask(unittest.TestCase):

    def setUp(self):
        self.task = Task(tag='tag',
                         error="an error")

    def test_to_dict(self):
        d = self.task.to_dict()
        self.assertIn('tag', d)
        self.assertIn('input', d)
        self.assertEqual(d['input']['t1'], 'foo')
        self.assertEqual(d['output']['out'], 'foo')
        self.assertEqual(d['tag'], 'tag')
        self.assertEqual(d['error'], 'an error')

    def test_from_and_to_bytes(self):
        bytes_ = self.task.to_bytes()
        task_from_bytes = Task().read_bytes(bytes_)
        self.assertEqual(task_from_bytes.__dict__, self.task.__dict__)

    def test_create_child(self):
        new_tag = 'child_task'
        child_task = self.task.create_child(new_tag)
        self.assertEqual(child_task.tag, new_tag)
        self.assertEqual(child_task.timestamp, self.task.timestamp)
        self.assertNotEqual(child_task.update_timestamp,
                            self.task.update_timestamp)
        self.assertGreaterEqual(child_task.update_timestamp,
                                self.task.timestamp)

    def test_child_does_not_influence_parent(self):
        new_tag = 'child_task'
        parent_task_output = deepcopy(self.task.output)
        child_task = self.task.create_child(new_tag)
        # change input of `child_task`
        child_task.input['out'] = 'bar'
        # this should not change output of parent task
        self.assertEqual(self.task.output, parent_task_output)


class TestTask(unittest.TestCase):

    def setUp(self):
        self.task_d = {"t_id": 1,
                       "tag": "spm_lesion",
                       "output": {"foo": "bar"},
                       "timestamp": 1530368396,
                       "data": {"dicom_info":
                                    {"t1": {"path": "path",
                                            "header": {"PatientName": "Max"}
                                            }
                                     }
                                }
                       }

    def test_read_dict(self):
        task = Task().read_dict(self.task_d)
        self.assertEqual(task.data['dicom_info']['t1']['path'], "path")

    def test_get_subject_name(self):
        task = Task().read_dict(self.task_d)
        name = task.get_subject_name()
        self.assertEqual(name, 'Max')

    def test_create_child(self):
        task = Task().read_dict(self.task_d)
        new_tag = 'child_task'
        child_task = task.create_child(new_tag)
        self.assertEqual(child_task.t_id, task.t_id)
        self.assertEqual(child_task.tag, new_tag)
        self.assertEqual(child_task.timestamp, task.timestamp)
        self.assertNotEqual(child_task.update_timestamp,
                            task.update_timestamp)
        self.assertGreaterEqual(child_task.update_timestamp,
                                task.timestamp)

    def test_child_does_not_influence_parent(self):
        task = Task().read_dict(self.task_d)
        new_tag = 'child_task'
        parent_task_data = deepcopy(task.data)
        child_task = task.create_child(new_tag)
        # change input of `child_task`
        child_task.data['out'] = 'bar'
        # this should not change output of parent task
        self.assertEqual(task.data, parent_task_data)

    def test_child_get_subject_name(self):
        task = Task().read_dict(self.task_d)
        new_tag = 'child_task'
        child_task = task.create_child(new_tag)
        name = child_task.get_subject_name()
        self.assertEqual(name, 'Max')

