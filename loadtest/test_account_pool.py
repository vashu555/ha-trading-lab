import concurrent.futures
import unittest
from account_pool import AccountPool


class ExclusiveAccounts(unittest.TestCase):
    def test_100_users_on_two_workers_have_100_distinct_accounts(self):
        pool = AccountPool()
        owners = [(f'worker-{i%2}',f'user-{i}') for i in range(100)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            accounts = list(executor.map(pool.acquire,owners))
        self.assertEqual(len(set(accounts)),100)
        self.assertEqual(set(accounts),{f'trader-{i:03d}' for i in range(41,141)})

    def test_duplicate_request_is_idempotent(self):
        pool=AccountPool()
        self.assertEqual(pool.acquire(('w','u')),pool.acquire(('w','u')))
        self.assertEqual(pool.snapshot()['assigned'],1)

    def test_accounts_are_exclusive_until_released(self):
        pool=AccountPool(41,42)
        pool.acquire(('w1','u1'))
        pool.acquire(('w2','u2'))
        with self.assertRaises(RuntimeError): pool.acquire(('w3','u3'))
        pool.release(('w1','u1'))
        pool.release(('w1','u1'))
        self.assertEqual(pool.acquire(('w3','u3')),'trader-041')
        pool.release(('w1','u1'))  # An old owner cannot release a reused account.
        with self.assertRaises(RuntimeError): pool.acquire(('w4','u4'))


if __name__=='__main__': unittest.main()
