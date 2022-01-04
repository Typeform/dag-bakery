import unittest
from dag_bakery.dummy import potato


class TestDummy(unittest.TestCase):
    def test_potato_tomato(self):
        self.assertEqual(potato(), "tomato")
