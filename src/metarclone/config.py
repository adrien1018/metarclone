import hashlib
import os
from typing import Set, Iterable


class SyncConfig:
    def __init__(self):
        # if True, treat destination as empty; that is, upload / download without checking checksums and existing files
        self.dest_as_empty = False
        self.use_file_checksum = False
        self.use_owner = False
        self.use_directory_mtime = False
        self.hash_function = hashlib.sha1
        # We emit a warning, exit with non-zero at the end and treat the file that caused the error as nonexistent
        #   (if applicable) if any non-fatal error occured during process.
        #   Use ignore_errors to suppress the non-zero exit code.
        self.ignore_errors = False
        self.rclone_args = []
        self.compression = 'gzip'  # passed to tar's -I option
        self.tar_command = 'tar'
        self.rclone_command = 'rclone'
        # TODO: add allowed device list (same_fs bool and fs_num int)

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


class UploadConfig(SyncConfig):
    def __init__(self):
        super().__init__()
        self.metadata_version = 1  # reserved for future use
        self.file_base_bytes = 64
        self.merge_threshold = 10 * 1024 * 1024
        self.delete_after_upload = True
        self.grouping_order = 'size'
        self.reserved_prefix = '_METARCLONE_'  # must be [0-9A-Z_]
        self.compression_suffix = '.gz'
        # these lists should be already joined with base path and without ending slash
        # include_list should have all possible prefixes in the set
        self.include_list: Set[bytes] = set()
        self.exclude_list: Set[bytes] = set()

    def set_include_list(self, path: bytes, orig_include_list: Iterable[bytes]):
        for i in orig_include_list:
            npath = os.path.normpath(i)
            while os.path.splitdrive(npath)[1]:
                self.include_list.add(os.path.join(path, npath))
                npath = os.path.split(npath)[0]

    def set_exclude_list(self, path: bytes, orig_exclude_list: Iterable[bytes]):
        for i in orig_exclude_list:
            self.exclude_list.add(os.path.join(path, os.path.normpath(i)))

