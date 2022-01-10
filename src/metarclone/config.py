import hashlib


class SyncConfig:
    metadata_version = 1  # reserved for future use
    same_device = False
    file_base_bytes = 64
    merge_threshold = 10 * 1024 * 1024
    delete_after_upload = True
    use_file_checksum = False
    use_directory_mtime = False
    hash_function = hashlib.sha1
    # default: treat a file as nonexistent and emit a warning if an error occurs when stat/reading it
    error_abort = False
    grouping_order = 'size'
    reserved_prefix = '_METARCLONE_'  # must be [0-9A-Z_]
    rclone_args = []
    compression = 'gzip'  # passed to tar's -I option
    compression_suffix = '.gz'
    tar_command = 'tar'
    rclone_command = 'rclone'
    # TODO: add allowed device list (same_fs bool and fs_num int)
