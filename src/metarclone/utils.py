import logging
from base64 import b32decode, b32encode
from contextlib import contextmanager

__all__ = ['wrap_oserror', 'decode_child', 'encode_child', 'win_to_posix']


@contextmanager
def wrap_oserror(path: bytes):
    try:
        yield
    except OSError as e:
        logging.warning(f'Error accessing {repr(path)[1:]}: {e}')


def decode_child(name: str):
    pad_length = len(name) % 8
    return b32decode(name + '=' * (pad_length and 8 - pad_length))


def encode_child(name: bytes):
    return b32encode(name).decode().rstrip('=')


def win_to_posix(path: bytes):
    return path.replace(b'\\', b'/')
