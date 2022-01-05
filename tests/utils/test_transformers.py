import unittest

from dag_bakery.utils.transformers import clean_key


class TestTransformers(unittest.TestCase):
    def test_clean_key(self):
        keys = [("potato", "potato"), ("Tomato", "tomato"), ("To Ma To", "to_ma_to"), ("2Po+Ta.Toes", "2po_ta_toes")]

        for key, expected_key in keys:
            self.assertEqual(clean_key(key), expected_key)
