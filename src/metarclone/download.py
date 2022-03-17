import logging
import os
import posixpath

from .config import DownloadConfig
from .utils import wrap_oserror, decode_child
from .rclone import rclone_download


class DownloadWalkResult:
    def __init__(self):
        self.real_transfer_size = 0
        self.real_transfer_files = 0
        self.error_count: int = 0


def download_walk(path: bytes, remote_path: str, metadata: dict, conf: DownloadConfig) -> DownloadWalkResult:
    res = DownloadWalkResult()
    try:
        os.makedirs(path, exist_ok=True)
    except OSError as e:
        logging.warning(f'Error creating {repr(path)[1:]}: {e}')
        res.error_count += 1
        return res

    if not conf.dest_as_empty:
        # TODO: checksum
        raise NotImplementedError()

    for child in metadata['files']:
        nbytes = rclone_download(posixpath.join(remote_path, child), path, conf)
        if nbytes < 0:
            logging.warning(f'Error downloading f{posixpath.join(remote_path, child)}')
            res.error_count += 1
        else:
            res.real_transfer_files += 1
            res.real_transfer_size += nbytes

    for child, child_meta in metadata['children'].items():
        child_res = download_walk(os.path.join(path, decode_child(child)), posixpath.join(remote_path, child),
                                  child_meta, conf)
        res.real_transfer_files += child_res.real_transfer_files
        res.real_transfer_size += child_res.real_transfer_size
        res.error_count += child_res.error_count
    return res


def download(path: bytes, remote_path: str, metadata: dict, conf: DownloadConfig) -> DownloadWalkResult:
    meta_checksum = metadata['checksum']
    conf.use_file_checksum = meta_checksum['use_file_checksum']
    conf.use_directory_mtime = meta_checksum['use_directory_mtime']
    conf.use_owner = meta_checksum['use_owner']
    conf.set_hash_function(meta_checksum['hash_function'])

    res = download_walk(path, remote_path, metadata['meta'], conf)

    # Restore hard links
    for item in metadata['hard_links']:
        group = list(map(decode_child, item['group']))
        link_src = os.path.join(path, group[0])
        for i in group[1:]:
            fpath = os.path.join(path, i)
            fdir = os.path.split(fpath)[0]
            try:
                dir_stat = os.stat(fdir)
                os.remove(os.path.join(path, i))
                os.link(link_src, fpath)
                os.utime(fdir, ns=(dir_stat.st_atime_ns, dir_stat.st_mtime_ns))
            except OSError as e:
                # TODO: The current hard link restoration would fail if the owner or permission settings prevents the
                #  file from being deleted (the owner & permission has been set by tar extraction). The correct way
                #  would be temporarily change the owner and permission to allow the operation, and restore them after
                #  the hard link is set up.
                logging.warning(f'Error restoring hardlink on {repr(fpath)[1:]}: {e}')
                res.error_count += 1

    root_file = posixpath.join(remote_path, metadata['root_name'])
    nbytes = rclone_download(root_file, path, conf)
    if nbytes < 0:
        logging.warning(f'Error downloading f{root_file}')
        res.error_count += 1
    else:
        res.real_transfer_files += 1
        res.real_transfer_size += nbytes
    return res
