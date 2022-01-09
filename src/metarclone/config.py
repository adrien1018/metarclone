import hashlib


class SyncConfig:
    same_device = False
    file_base_bytes = 64
    merge_threshold = 10 * 1024 * 1024
    delete_after_upload = True
    use_file_checksum = False
    use_directory_mtime = False
    hash_function = hashlib.sha1
    # default: treat a file as nonexistent if an error occurs when stat/reading it
    error_abort = False
    grouping_order = 'size'
    reserved_prefix = '_METARCLONE_'  # must be [0-9A-Z_]
    rclone_args = []
    # TODO: add allowed device list (same_fs bool and fs_num int)
