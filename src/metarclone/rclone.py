import logging
import os
import tempfile
import threading
from io import RawIOBase
from typing import List
from subprocess import Popen, PIPE

from .config import SyncConfig


def read_thread(f, res: List[bytes] = None):
    if res is None:
        f.read()
    else:
        res.append(f.read())


def rclone_upload(path: bytes, files: List[bytes], dest: str, conf: SyncConfig) -> int:
    with tempfile.NamedTemporaryFile('wb', delete=False) as f:
        fname = f.name
        f.write(b'\0'.join(files))
        f.write(b'\0')
    try:
        tar_cmd = [conf.tar_command.encode()]
        if conf.compression:
            tar_cmd += [b'-I', conf.compression.encode()]
        tar_cmd += [b'--null', b'--ignore-failed-read', b'--no-recursion',
                    b'-C', path, b'-T', fname.encode(), b'-Scf', b'-']
        tar_proc = Popen(tar_cmd, stdout=PIPE, stderr=PIPE)
        rclone_proc = Popen([conf.rclone_command, 'rcat', dest], stdin=PIPE, stdout=PIPE, stderr=PIPE)

        buffer = memoryview(bytearray(256 * 1024))
        stdout = []
        stderr = []
        tarerr = []
        throut = threading.Thread(target=read_thread, args=(rclone_proc.stdout, stdout))
        threrr = threading.Thread(target=read_thread, args=(rclone_proc.stderr, stderr))
        thrtarerr = threading.Thread(target=read_thread, args=(tar_proc.stderr, tarerr))
        throut.start()
        threrr.start()
        thrtarerr.start()
        with tar_proc.stdout as fp, rclone_proc.stdin as out:
            fp: RawIOBase
            out: RawIOBase
            total_bytes = 0
            for n in iter(lambda: fp.readinto(buffer), 0):
                out.write(buffer[:n])
                total_bytes += n
        throut.join()
        threrr.join()
        thrtarerr.join()
        status = tar_proc.wait(), rclone_proc.wait()
        if status[1]:
            logging.warning(f'rclone failed: {stderr[0]}')
            return -1
        if status[0]:
            logging.warning(f'tar failed: {tarerr[0]}')
            return -1
        return total_bytes
    finally:
        os.remove(fname)
