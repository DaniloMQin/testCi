from nose2.compat import unittest
import time

class Test11(unittest.TestCase):
    def setup(self):
        print "Hello World"
    def test1(self):
        print "TEST1 starts"
        time.sleep(2)
        print "TEST1 ends"
    def test2(self):
        print "TEST2 starts"
        time.sleep(3)
        print "TEST2 ends"
    def teardown(self):
        print "Good Bye World"

