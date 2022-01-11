import hashlib


class SyncConfig:
    def __init__(self):
        self.metadata_version = 1  # reserved for future use
        self.same_device = False
        self.file_base_bytes = 64
        self.merge_threshold = 10 * 1024 * 1024
        self.delete_after_upload = True
        self.use_file_checksum = False
        self.use_directory_mtime = False
        self.hash_function = hashlib.sha1
        # We treat a file as nonexistent, emit a warning and exit with non-zero at the end if an error occurs when
        #   stat/reading it. Use ignore_errors to suppress the non-zero exit code.
        self.ignore_errors = False
        self.grouping_order = 'size'
        self.reserved_prefix = '_METARCLONE_'  # must be [0-9A-Z_]
        self.rclone_args = []
        self.compression = 'gzip'  # passed to tar's -I option
        self.compression_suffix = '.gz'
        self.tar_command = 'tar'
        self.rclone_command = 'rclone'
        # TODO: add allowed device list (same_fs bool and fs_num int)
