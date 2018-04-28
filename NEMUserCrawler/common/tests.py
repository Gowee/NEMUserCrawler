#!/usr/bin/env python3
import unittest
import random
import time
from io import BytesIO
from math import ceil

from sparse_presence_table import SparsePresenceTable
import nem_crypto


class NEMAPICryptoTest(unittest.TestCase):
    def test_square_multiply(self):
        x, e, n = random.randint(
            1000000, 9999999), nem_crypto.EXPONENT, nem_crypto.MODULUS
        self.assertEqual(x ** e % n, nem_crypto.square_multiply(x, e, n))

    def test_encrypt(self):
        cipher_data = nem_crypto.encrypt(
            '{"csrf_token":""}', key=b"erfMaqdJPvByr7Xl")
        self.assertEqual(
            cipher_data['params'], "7KvkKBOcrvCW43XAV0rLbJHixeL5hnPJ6ndHWAxY4qGvaXk7v3Vt9+VWQr4JDhV3")
        self.assertEqual(cipher_data['encSecKey'], "59ba25f5a3e0b29a9c3580c003565fa128e9e7624c6fbbd47321206ff00d07b1d7d340f773df588fe1dae991642d9fdd8095ca2b04137424a31b4d58eeb7a52e50366da3ce6501f4e3f19a62f77e585927afa0ef8b3c111b3a664bf328b723701fe626f23369aacdc36377bc2a9c7d8e7945ed1db8ceb1c63c9d9a9cf7ae4fcf")


class SparsePresenceTableTest(unittest.TestCase):
    def reset_time(self):
        self.start_time = time.time()

    def elapsed_time(self):
        return time.time() - self.start_time

    def print_duration(self, task_name=None, count=1):
        if task_name is None:
            template = "{id}: {duration} secs"
        else:
            template = "{id}: {task_name} took {duration} secs"
        if count != 1:
            template += " ({average} secs per)"
        print(template.format(id=self.id(),
                              duration=self.elapsed_time(),
                              task_name=task_name,
                              average=self.elapsed_time() / count))

    def test_preallocation(self):
        for nth_test in range(5):
            with self.subTest(nth_test=nth_test):
                preallocated = set(random.randint(0, 99)
                                   for _ in range(random.randint(10, 20)))
                table = SparsePresenceTable(10, 2, preallocated)
                self.assertEqual(table._block_amount, 100)
                self.assertEqual(len(table._table), 100)

                for i, block in enumerate(table._table):
                    if i in preallocated:
                        self.assertIsNotNone(block)
                    else:
                        self.assertIsNone(block)
                # print(__import__("sys").getsizeof(table._table[list(preallocated)[0]])) # 57 bytes overhead per

    def test_set_present(self):
        for nth_test in range(5):
            with self.subTest(nth_test=nth_test):

                self.reset_time()
                table = SparsePresenceTable(10, 2)
                self.print_duration("create 1 table")
                self.reset_time()
                for _ in range(1000):
                    no = random.randint(0, 10**10 - 1)
                    table.set_present(no)
                    self.assertIn(no, table)
                self.print_duration(
                    "`set_present` and `is_present` 1000 times", 1000)

    def test_present(self):
        for nth_test in range(5):
            with self.subTest(nth_test=nth_test):
                table = SparsePresenceTable(10, 2)
                self.reset_time()
                nos = list(set(random.randint(0, 10**10 - 1) for _ in range(1000)))
                for no in nos:
                    self.assertFalse(table.present(no))
                random.shuffle(nos)
                for no in nos:
                    self.assertIn(no, table)
                for no in list(random.randint(0, 10**10 - 1) for _ in range(10000)):
                    if no in nos:
                        continue
                    self.assertNotIn(no, table)
                self.print_duration(
                    "`present` 1000 (hit) + roughly 10000 (miss) times", 1000 + 10000)
        

    def test_serilization(self):
        for nth_test in range(5):
            with self.subTest(nth_test=nth_test):
                table = SparsePresenceTable(8, 2)
                present = [random.randint(0, 10**8 - 1) for _ in range(1000)]
                for no in present:
                    table.set_present(no)
                empty_block_amount = table._table.count(None)
                file = BytesIO()
                byte_count = table.dump_to_file(file)
                #self.assertGreater(len(file.getbuffer()), byte_count)
                #self.assertGreater(len(file.getbuffer()), ceil(10**6 / 8) * 100)
                # TODO: test file size accurately
                file.seek(0)
                table = SparsePresenceTable(8, 2) # create new empty table
                table.load_from_file(file)
                self.assertEqual(empty_block_amount, table._table.count(None))
                for no in present:
                    self.assertIn(no, table)
            