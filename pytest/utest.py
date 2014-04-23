#! python3
import unittest

def square(x):
    return x*2

class TestCase1(unittest.TestCase):
    def testA(self):
        self.assertEqual(square('abc'), 'abcabc')
    def testB(self):
        self.assertEqual(square([1,3]), [1,3,1,3])
    def testC(self):
        self.assertEqual(square(('3',4)), ('3',4,'3',4))

if __name__ == '__main__':
    unittest.main()
