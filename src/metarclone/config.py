import hashlib


class SyncConfig:
    def __init__(self):
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
