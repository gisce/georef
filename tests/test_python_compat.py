# -*- coding: utf-8 -*-
import csv
import os
import tempfile
import unittest

from georef import csv_row, open_csv
from georef.loop import List


class PythonCompatibilityTest(unittest.TestCase):

    def test_list_supports_iterator_protocol(self):
        values = List(None, objects=['one', 'two'], model='test.model')
        self.assertEqual(list(values), ['one', 'two'])

    def test_csv_helpers_write_unicode(self):
        handle, filename = tempfile.mkstemp()
        os.close(handle)
        try:
            output = open_csv(filename)
            writer = csv.writer(output, delimiter=';')
            writer.writerow(csv_row([u'Girona', u'àrea']))
            output.close()
            with open(filename, 'rb') as result:
                self.assertIn('Girona;'.encode('utf-8'), result.read())
        finally:
            os.unlink(filename)


if __name__ == '__main__':
    unittest.main()
