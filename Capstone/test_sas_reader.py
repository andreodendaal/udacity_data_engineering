import unittest
import sas_reader


class MyPositiveTests(unittest.TestCase):
    def test_headers(self):
        self.assertIsNotNone(sas_reader.proc_sasref('i94cit'))
        self.assertIsNotNone(sas_reader.proc_sasref('i94port'))
        self.assertIsNotNone(sas_reader.proc_sasref('i94mode'))
        self.assertIsNotNone(sas_reader.proc_sasref('i94addr'))
        self.assertIsNotNone(sas_reader.proc_sasref('i94visa'))
        self.assertIsNotNone(sas_reader.proc_sasref('i94res'))

    def test_type(self):
        self.assertIsInstance((sas_reader.proc_sasref('i94cit')), dict)


class MyNegativeTests(unittest.TestCase):
    def test_none(self):
        self.assertIsNone(sas_reader.proc_sasref('xxxx'))
        self.assertIsNone(sas_reader.proc_sasref(''))

if __name__ == '__main__':
    unittest.main()
