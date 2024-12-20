from contextlib import contextmanager
from threading import Semaphore


@contextmanager
def lock(semaphore: Semaphore):
    try:
        semaphore.acquire()
        yield
    finally:
        semaphore.release()
