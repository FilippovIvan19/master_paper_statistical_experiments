from multiprocessing import Pool
from contextlib import contextmanager


class Parallelizer:
    def __init__(self, processes: int = 1, storage: list = None):
        self.processes = processes
        self.storage = storage

        if processes != 1:
            self.pool = Pool(processes)

    def apply_async(self, func, args=()):
        if self.processes == 1:
            res = func(*args)
        else:
            # print('apply_async')
            res = self.pool.apply_async(func, args)
        if self.storage is not None:
            self.storage.append(res)

    def __close__(self):
        if self.processes != 1:
            self.pool.close()
            self.pool.join()
            if self.storage is not None:
                for i in range(len(self.storage)):
                    self.storage[i] = self.storage[i].get()


@contextmanager
def parallelize(processes: int = 1, storage: list = None) -> Parallelizer:
    parallelizer = Parallelizer(processes, storage)
    try:
        yield parallelizer
    finally:
        parallelizer.__close__()
