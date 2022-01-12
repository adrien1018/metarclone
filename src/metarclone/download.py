import os
import posixpath

from .config import SyncConfig
from .utils import wrap_oserror, decode_child
from .rclone import rclone_download


def download_walk(path: bytes, remote_path: str, metadata: dict, conf: SyncConfig):
    # TODO: error handling
    os.makedirs(path, exist_ok=True)
    for child in metadata['files']:
        rclone_download(posixpath.join(remote_path, child), path, conf)
    for child, child_meta in metadata['children'].items():
        download_walk(os.path.join(path, decode_child(child)), posixpath.join(remote_path, child),
                      child_meta, conf)


def download(path: bytes, remote_path: str, metadata: dict, conf: SyncConfig):
    download_walk(path, remote_path, metadata['meta'], conf)
    # TODO: deal with hardlinks
    rclone_download(posixpath.join(remote_path, metadata['root_name']), path, conf)
