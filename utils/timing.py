from contextlib import contextmanager
import time


@contextmanager
def time_measure(name: str, is_active: bool = True) -> None:
    if is_active:
        start_time = time.time()
        yield
        elapsed_time = time.time() - start_time
        minutes = int(elapsed_time // 60)
        print(f'[{name}] finished in {minutes}m {elapsed_time % 60:.2f}s')
    else:
        yield
