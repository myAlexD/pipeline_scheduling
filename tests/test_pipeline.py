import unittest
from common.utils import read_pipeline, get_min_execution_time, _check_for_undefined_tasks, _validate_pipeline

class TestUtils(unittest.TestCase):

    def test_cyclic_dependency(self):
        with self.assertRaises(TypeError):
            read_pipeline("tests/example_files/cyclic_dependency.txt")

    def test_unbound_task(self):
        with self.assertRaises(ValueError):
            task_dict = read_pipeline("tests/example_files/unbound_task.txt")
            _check_for_undefined_tasks(task_dict)

    def test_default_input(self):
        task_dict = read_pipeline("tests/example_files/default_input.txt")
        min_execution_time, _ = get_min_execution_time(task_dict, 2)
        self.assertEqual(min_execution_time, 4)

    def test_non_existent_file(self):
        with self.assertRaises(FileNotFoundError):
            task_dict = read_pipeline("tests/example_files/non_existent.txt")
    
    def test_invalid_file(self):
        valid, _ = _validate_pipeline("tests/example_files/invalid_file.txt")
        self.assertFalse(valid)

if __name__ == "__main__":
    unittest.main()
