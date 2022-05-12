import logging
import shutil
from base64 import b32decode, b32encode
from contextlib import contextmanager

__all__ = ['wrap_oserror', 'decode_child', 'encode_child', 'win_to_posix', 'is_path', 'cmd_to_abs_path']


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


def is_path(x: str):
    return '/' in x or '\\' in x


def cmd_to_abs_path(cmd: str):
    if is_path(cmd):
        return cmd
    res = shutil.which(cmd)
    if not res:
        raise FileNotFoundError(f'Cannot find executable: {cmd}')
    return res
