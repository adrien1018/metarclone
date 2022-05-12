import io
import json
import logging
import posixpath
from gzip import GzipFile
from io import BytesIO
from tempfile import NamedTemporaryFile

from .config import SyncConfig
from .rclone import rclone_upload_raw, rclone_download_raw

# Always use gzip for metadata compression to avoid complicated rclone piping
__all__ = ['load_metadata', 'save_metadata']


def metadata_path(remote_path: str, conf: SyncConfig):
    if conf.metadata_path is None:
        return True, posixpath.join(remote_path, f'{conf.reserved_prefix}META.json.gz')
    else:
        return ':' in conf.metadata_path and ':\\' not in conf.metadata_path and ':/' not in conf.metadata_path, \
               conf.metadata_path


def load_metadata(remote_path: str, conf: SyncConfig):
    is_remote, path = metadata_path(remote_path, conf)
    if is_remote:
        data = rclone_download_raw(path, conf)
        if data is None:
            return None
        with BytesIO(data) as stream, GzipFile(mode='rb', fileobj=stream) as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return None
    else:
        try:
            with open(path, 'rb') as stream, GzipFile(mode='rb', fileobj=stream) as f:
                return json.load(f)
        except FileNotFoundError:
            return None


def save_metadata(metadata: dict, remote_path: str, conf: SyncConfig):
    with BytesIO() as stream:
        with GzipFile(mode='wb', fileobj=stream) as f:
            with io.TextIOWrapper(f) as ftext:
                json.dump(metadata, ftext)
        data = stream.getvalue()
    is_remote, path = metadata_path(remote_path, conf)
    if is_remote:
        if rclone_upload_raw(path, data, conf):
            return
    else:
        try:
            with open(path, 'wb') as f:
                f.write(data)
            return
        except OSError as e:
            logging.warning(f'Cannot open metadata file {path} for writing: {e.strerror}')
    try:
        with NamedTemporaryFile(delete=False) as f:
            logging.warning(f'Writing to metadata file failed. Trying to write to {f.name} instead...\n')
            f.write(data)
        logging.warning(
            'Success! Please store the metadata file properly and specify metadata file in subsequent runs. '
            'Otherwise, downloading would fail and uploading will upload the whole directory again.')
    except OSError as e:
        logging.fatal('FATAL: Metadata writing failed.')
        raise e from None
