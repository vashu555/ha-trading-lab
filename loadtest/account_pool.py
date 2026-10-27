"""Exclusive account leases shared by all workers through the Locust master."""
import heapq
import threading


class AccountPool:
    def __init__(self, first=41, last=200):
        self.capacity = last - first + 1
        self._free = list(range(first, last + 1))
        heapq.heapify(self._free)
        self._leases = {}
        self._lock = threading.Lock()

    def acquire(self, owner):
        with self._lock:
            if owner not in self._leases:
                if not self._free:
                    raise RuntimeError(f"All {self.capacity} unique Locust accounts are in use")
                self._leases[owner] = heapq.heappop(self._free)
            return f"trader-{self._leases[owner]:03d}"

    def release(self, owner):
        with self._lock:
            number = self._leases.pop(owner, None)
            if number is not None:
                heapq.heappush(self._free, number)

    def snapshot(self):
        with self._lock:
            accounts = sorted(({'account': f'trader-{n:03d}', 'worker': owner[0]}
                               for owner, n in self._leases.items()), key=lambda a: a['account'])
            return {'capacity': self.capacity, 'assigned': len(accounts), 'available': len(self._free),
                    'accounts': accounts}
