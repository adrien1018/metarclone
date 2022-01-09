import logging
from contextlib import contextmanager

from .config import SyncConfig


@contextmanager
def wrap_oserror(conf: SyncConfig, path: bytes):
    try:
        yield
    except OSError as e:
        if conf.error_abort:
            raise e from None
        logging.warning(f'Error accessing {repr(path)[1:]}: {e}')
