import logging
import os
import stat
from contextlib import contextmanager
from io import FileIO
from typing import Dict, Tuple, Optional, List

from .config import SyncConfig


class ChecksumWalkResult:
    total_size = 0
    total_files = 0
    hard_link_map: Dict[Tuple[int, int], bytes] = {}


@contextmanager
def wrap_oserror(conf: SyncConfig, path: bytes):
    try:
        yield
    except OSError as e:
        if conf.error_abort:
            raise e from None
        logging.warning(f'Error accessing {repr(path)[1:]}: {e}')


def init_file_checksum(name: bytes, st: os.stat_result, conf: SyncConfig):
    hash_obj = conf.hash_function(name + st.st_mode.to_bytes(4, 'little'))
    if stat.S_ISDIR(st.st_mode) and conf.use_directory_mtime:
        hash_obj.update(st.st_mtime_ns.to_bytes(16, 'little', signed=True))
    return hash_obj


def get_file_content_checksum(full_path: bytes, st: os.stat_result, conf: SyncConfig) -> Optional[bytes]:
    # noinspection PyUnusedLocal
    success = False
    file_hash_obj = conf.hash_function()
    if stat.S_ISREG(st.st_mode):
        buffer = memoryview(bytearray(256 * 1024))
        with wrap_oserror(conf, full_path):
            # auto typing is incorrect
            # noinspection PyTypeChecker
            fp: FileIO = open(full_path, 'rb', buffering=0)
        with fp:
            for n in iter(lambda: fp.readinto(buffer), 0):
                file_hash_obj.update(buffer[:n])
            success = True
    elif stat.S_ISLNK(st.st_mode):
        with wrap_oserror(conf, full_path):
            file_hash_obj.update(os.readlink(full_path))
            success = True
    else:
        success = True
    return file_hash_obj.digest() if success else None


def one_file_checksum(name: bytes, full_path: bytes, st: os.stat_result, conf: SyncConfig,
                      second_pass: bool) -> bytes:
    """
    if error, return empty bytes (b'')
    """
    hash_obj = init_file_checksum(name, st, conf)
    if conf.use_file_checksum and second_pass:
        file_hash = get_file_content_checksum(full_path, st, conf)
        if file_hash is None:
            return b''
        hash_obj.update(file_hash)
    else:
        hash_obj.update(st.st_size.to_bytes(16, 'little') + st.st_mtime_ns.to_bytes(16, 'little', signed=True))
    return hash_obj.digest()


def file_checksum(name: bytes, full_path: bytes, st: os.stat_result, conf: SyncConfig,
                  second_pass: bool, result: Optional[ChecksumWalkResult] = None) -> bytes:
    """
    Can raise OSError from open(), os.scandir() or os.readlink()
    """
    if stat.S_ISDIR(st.st_mode):
        hash_obj = init_file_checksum(name, st, conf)
        # noinspection PyUnusedLocal
        scan = None
        with wrap_oserror(conf, full_path):
            scan = os.scandir(full_path)
        if scan is None:
            # Because of the signature definition,
            # returning empty byte string effectively ignores the directory
            return b''
        with scan as it:
            lst: List[os.DirEntry] = sorted(it, key=lambda x: x.name)
            for f in lst:
                with wrap_oserror(conf, f.path):
                    f_st = f.stat(follow_symlinks=False)
                    sig = file_checksum(f.name, f.path, f_st, conf, second_pass, result)
                    hash_obj.update(sig)
        if result:
            result.total_files += 1
        return hash_obj.digest()
    else:
        res = one_file_checksum(name, full_path, st, conf, second_pass)
        if not res:
            return b''
        if result:
            result.total_size += st.st_size
            result.total_files += 1
            if st.st_nlink > 1:
                result.hard_link_map[(st.st_dev, st.st_ino)] = full_path
        return res


def checksum_walk(names: List[Tuple[bytes, os.stat_result]], path: bytes, conf: SyncConfig,
                  second_pass: bool, result: Optional[ChecksumWalkResult] = None) -> str:
    """
    Checksum of a file S(file) :=
      H(file.name + file.st_mode.to_bytes(4, 'little') +
        H(file.content))      <if conf.use_file_checksum and second_pass>
      H(file.name + file.st_mode.to_bytes(4, 'little')) +
        file.st_size.to_bytes(16, 'little') +
        file.st_mtime_ns.to_bytes(16, 'little', signed=True)) <otherwise>
    file.content is the content for regular files, destination for soft links, and empty for other files

    Checksum of a directory S(dir) :=
      H(H(dir.name + dir.st_mode.to_bytes(4, 'little') +
          dir.st_mtime_ns.to_bytes(16, 'little', signed=True))) +
        b''.join([S(f) for f in sorted(os.listdir(dir), key=f.name)])) <if conf.use_directory_mtime>
      H(H(dir.name + dir.st_mode.to_bytes(4, 'little')) +
        b''.join([S(f) for f in sorted(os.listdir(dir), key=f.name)])) <otherwise>

    Checksum of a group of same-level files checksum_walk(group) :=
      H(b''.join([S(f) for f in sorted(files, key=f.name)]))
    checksum_walk returns value in hexdigest for storing it in JSON

    H is config.hash_function (default is SHA1 for checksum speed)
    Note: all hashed content contains at most one variable-length input

    names: list of (name, stat_result)
    """
    names.sort(key=lambda x: x[0])
    hash_obj = conf.hash_function()
    for name, st in names:
        hash_obj.update(file_checksum(name, os.path.join(path, name), st, conf, second_pass, result))
    return hash_obj.hexdigest()
