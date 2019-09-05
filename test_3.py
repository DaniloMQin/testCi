from nose2.compat import unittest
import time

class Test3(unittest.TestCase):
    def setup(self):
        print "Hello World"
    def test31(self):
        print "TEST31 starts"
        time.sleep(2)
        print "TEST31 ends"
    def test32(self):
        print "TEST32 starts"
        time.sleep(4)
        assert False
        print "TEST32 ends"
    def teardown(self):
        print "Good Bye World"

