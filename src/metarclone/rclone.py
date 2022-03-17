import logging
import os
import tempfile
import threading
from io import RawIOBase
from typing import List, Optional
from subprocess import run, Popen, PIPE, DEVNULL

from .config import SyncConfig, UploadConfig
from .utils import win_to_posix

__all__ = ['rclone_upload', 'rclone_download', 'rclone_upload_raw', 'rclone_download_raw', 'rclone_delete']

BUF_SIZE = 256 * 1024


def read_thread(f, res: List[bytes] = None):
    if res is None:
        f.read()
    else:
        res.append(f.read())


def rclone_upload(path: bytes, files: List[bytes], dest: str, conf: UploadConfig) -> int:
    with tempfile.NamedTemporaryFile('wb', delete=False) as f:
        fname = f.name
        f.write(b'\0'.join(files))
        f.write(b'\0')
    try:
        tar_cmd = [conf.tar_command.encode()]
        if conf.compression:
            tar_cmd += [b'-I', conf.compression.encode()]
        tar_cmd += [b'--null', b'--ignore-failed-read', b'--no-recursion', b'-H', b'posix', b'--acls',
                    b'-C', path, b'-T', fname.encode(), b'-Scf', b'-']
        rclone_cmd = [conf.rclone_command, 'rcat', *conf.rclone_args, dest]
        logging.debug(f'Invoke command: {tar_cmd}')
        tar_proc = Popen(tar_cmd, stdout=PIPE, stderr=PIPE)
        logging.debug(f'Invoke command: {rclone_cmd}')
        rclone_proc = Popen(rclone_cmd, stdin=PIPE, stdout=DEVNULL, stderr=PIPE)

        buffer = memoryview(bytearray(BUF_SIZE))
        stderr = []
        tarerr = []
        threrr = threading.Thread(target=read_thread, args=(rclone_proc.stderr, stderr))
        thrtarerr = threading.Thread(target=read_thread, args=(tar_proc.stderr, tarerr))
        threrr.start()
        thrtarerr.start()
        with tar_proc.stdout as fp, rclone_proc.stdin as out:
            fp: RawIOBase
            out: RawIOBase
            total_bytes = 0
            for n in iter(lambda: fp.readinto(buffer), 0):
                out.write(buffer[:n])
                total_bytes += n
        threrr.join()
        thrtarerr.join()
        status = tar_proc.wait(), rclone_proc.wait()
        if status[1]:
            logging.warning(f'rclone rcat failed with status {status[1]}: {stderr[0]}')
            return -1
        if status[0]:
            logging.warning(f'tar failed with status {status[1]}: {tarerr[0]}')
            return -1
        return total_bytes
    finally:
        os.remove(fname)


def rclone_download(path: str, dest: bytes, conf: SyncConfig) -> int:
    if os.name == 'nt':
        dest = win_to_posix(dest)
    rclone_cmd = [conf.rclone_command, 'cat', *conf.rclone_args, path]
    tar_cmd = [conf.tar_command.encode()]
    if conf.compression:
        tar_cmd += [b'-I', conf.compression.encode()]
    tar_cmd += [b'-C', dest, b'-Sxf', b'-']
    logging.debug(f'Invoke command: {rclone_cmd}')
    rclone_proc = Popen(rclone_cmd, stdout=PIPE, stderr=PIPE)
    logging.debug(f'Invoke command: {tar_cmd}')
    tar_proc = Popen(tar_cmd, stdin=PIPE, stdout=DEVNULL, stderr=PIPE)

    buffer = memoryview(bytearray(BUF_SIZE))
    stderr = []
    tarerr = []
    threrr = threading.Thread(target=read_thread, args=(rclone_proc.stderr, stderr))
    thrtarerr = threading.Thread(target=read_thread, args=(tar_proc.stderr, tarerr))
    threrr.start()
    thrtarerr.start()
    with rclone_proc.stdout as fp, tar_proc.stdin as out:
        fp: RawIOBase
        out: RawIOBase
        total_bytes = 0
        for n in iter(lambda: fp.readinto(buffer), 0):
            out.write(buffer[:n])
            total_bytes += n
    threrr.join()
    thrtarerr.join()
    status = tar_proc.wait(), rclone_proc.wait()
    if status[1]:
        logging.warning(f'rclone cat failed with status {status[1]}: {stderr[0]}')
        return -1
    if status[0]:
        logging.warning(f'tar failed with status {status[1]}: {tarerr[0]}')
        return -1
    return total_bytes


def rclone_upload_raw(dest: str, content: bytes, conf: SyncConfig) -> bool:
    rclone_cmd = [conf.rclone_command, 'rcat', *conf.rclone_args, dest]
    logging.debug(f'Invoke command: {rclone_cmd}')
    result = run(rclone_cmd, input=content, capture_output=True)
    if result.returncode:
        logging.warning(f'rclone rcat failed with status {result.returncode}: {result.stderr}')
        return False
    return True


def rclone_download_raw(dest: str, conf: SyncConfig) -> Optional[bytes]:
    rclone_cmd = [conf.rclone_command, 'cat', *conf.rclone_args, dest]
    logging.debug(f'Invoke command: {rclone_cmd}')
    result = run(rclone_cmd, capture_output=True)
    if result.returncode:
        logging.warning(f'rclone cat failed with status {result.returncode}: {result.stderr}')
        return None
    return result.stdout


def rclone_delete(path: str, is_dir: bool, conf: SyncConfig) -> bool:
    rclone_cmd = [conf.rclone_command, 'purge' if is_dir else 'delete', *conf.rclone_args, path]
    logging.debug(f'Invoke command: {rclone_cmd}')
    res = run(rclone_cmd, capture_output=True)
    if res.returncode:
        logging.warning(f'rclone purge failed with status{res.returncode}: {res.stderr}')
        return False
    return True
