import os
import hashlib
import shutil
from typing import Set, Iterable, List, Optional

from metarclone.utils import cmd_to_abs_path


class SyncConfig:
    def __init__(self):
        # if True, treat destination as empty; that is, upload / download without checking checksums and existing files
        self.dest_as_empty = False
        self.use_file_checksum = False
        self.use_owner = False
        self.use_directory_mtime = False
        self.hash_function = hashlib.sha1
        # We emit a warning, exit with non-zero at the end and treat the file that caused the error as nonexistent
        #   (if applicable) if any non-fatal error occurred.
        #   Use ignore_errors to suppress the non-zero exit code.
        self.ignore_errors = False
        self.rclone_args: List[str] = []
        self.compression: Optional[str] = 'gzip'  # passed to tar's -I option
        self.tar_command = 'tar'
        self.rclone_command = 'rclone'
        self.reserved_prefix = '_METARCLONE_'  # must be [0-9A-Z_]
        self.metadata_path: Optional[str] = None
        self.s3_min_chunk_size_kib = 5 * 1024
        self.dry_run = False
        # TODO: add allowed device list (same_fs bool and fs_num int)
        # TODO: allow encode_child/decode_child to be something other than base32

    def head_bytes(self):
        first_byte = (
            (1 << 0 if self.use_file_checksum else 0) |
            (1 << 1 if self.use_owner else 0) |
            (1 << 2 if self.use_directory_mtime else 0)
        )
        return bytes([first_byte, 0, 0, 0])

    def set_hash_function(self, name: str):
        if name not in hashlib.algorithms_available:
            raise NameError('Hash function not found')
        if name in dir(hashlib):
            self.hash_function = getattr(hashlib, name)
        else:
            self.hash_function = hashlib.new(name)

    def convert_command_to_abs_path(self):
        # In MinGW/Windows, direct exec will not use the PATH variable to find the executable properly,
        #  so we do it manually here
        self.tar_command = cmd_to_abs_path(self.tar_command)
        self.rclone_command = cmd_to_abs_path(self.rclone_command)


class UploadConfig(SyncConfig):
    def __init__(self):
        super().__init__()
        self.metadata_version = 1  # reserved for future use
        self.file_base_bytes = 64
        self.merge_threshold = 10 * 1024 * 1024
        self.delete_after_upload = True
        self.grouping_order = 'size'
        self.compression_suffix = '.gz'
        # these lists should be already joined with base path and without ending slash
        # include_list should have all possible prefixes in the set
        self.include_list: Set[bytes] = set()
        self.exclude_list: Set[bytes] = set()

    def deduct_compression_suffix(self) -> bool:
        if self.compression == 'none' or not self.compression:
            self.compression = None
            self.compression_suffix = ''
            return True
        compress_cmd = self.compression.split()
        if not compress_cmd:
            return False
        compression = compress_cmd[0]
        compression_map = {'gzip': '.gz', 'gunzip': '.gz', 'pigz': '.gz',
                           'bzip2': '.bz2', 'bunzip2': '.bz2', 'pbzip2': '.bz2',
                           'xz': '.xz', 'unxz': '.xz',
                           'zstd': '.zst', 'unzstd': '.zst', 'pzstd': '.zst'}
        if compression not in compression_map:
            return False
        self.compression_suffix = compression_map[compression]
        return True

    def set_include_list(self, path: bytes, orig_include_list: Iterable[bytes]):
        for i in orig_include_list:
            npath = os.path.normpath(i)
            while os.path.splitdrive(npath)[1]:
                self.include_list.add(os.path.join(path, npath))
                npath = os.path.split(npath)[0]

    def set_exclude_list(self, path: bytes, orig_exclude_list: Iterable[bytes]):
        for i in orig_exclude_list:
            self.exclude_list.add(os.path.join(path, os.path.normpath(i)))


class DownloadConfig(SyncConfig):
    pass
