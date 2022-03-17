import argparse
import re

from .config import SyncConfig, UploadConfig, DownloadConfig
from .upload import upload
from .download import download


def get_parser():
    root_parser = argparse.ArgumentParser(prog='metarclone')
    subparsers = root_parser.add_subparsers(title='methods', dest='method', required=True,
                                            metavar='method', help=None)

    def add_sync_arguments(parser: argparse.ArgumentParser):
        parser.add_argument('--dest-as-empty', action='store_true')
        parser.add_argument('-c', '--use-file-checksum', action='store_true')
        parser.add_argument('--use-directory-mtime', action='store_true')
        parser.add_argument('--use-owner', action='store_true')
        parser.add_argument('--checksum-choice')
        parser.add_argument('--ignore-errors', action='store_true')
        parser.add_argument('--rclone-args')
        parser.add_argument('-I', '--use-compress-program')
        parser.add_argument('--tar-path')
        parser.add_argument('--rclone-path')
        parser.add_argument('--reserved-prefix')
        parser.add_argument('--metadata-path')

    upload_parser = subparsers.add_parser('upload', help='Upload to rclone remote')
    add_sync_arguments(upload_parser)
    upload_parser.add_argument('--file-base-bytes', type=int)
    upload_parser.add_argument('--merge-threshold')
    upload_parser.add_argument('--delete-after-upload', action='store_false')
    upload_parser.add_argument('--grouping-order')
    upload_parser.add_argument('--compression-suffix')
    upload_parser.add_argument('--exclude-file', action='append')
    upload_parser.add_argument('--include-file', action='append')
    upload_parser.add_argument('local')
    upload_parser.add_argument('remote')
    upload_parser.set_defaults(func=upload_func)

    download_parser = subparsers.add_parser('download', help='Download from rclone remote')
    add_sync_arguments(download_parser)
    download_parser.add_argument('remote')
    download_parser.add_argument('local')
    download_parser.set_defaults(func=download_func)

    return root_parser


def populate_sync_config(args: argparse.Namespace, conf: SyncConfig):
    conf.dest_as_empty = args.dest_as_empty
    conf.use_file_checksum = args.use_file_checksum
    conf.use_directory_mtime = args.use_directory_mtime
    conf.use_owner = args.use_owner
    if args.checksum_choice is not None:
        conf.set_hash_function(args.checksum_choice)
    conf.ignore_errors = args.ignore_errors
    if args.rclone_args is not None:
        conf.rclone_args = args.rclone_args.split()
    if args.use_compress_program is not None:
        conf.compression = args.use_compress_program
    if args.tar_path is not None:
        conf.tar_command = args.tar_path
    if args.rclone_path is not None:
        conf.rclone_command = args.rclone_path
    if args.reserved_prefix is not None:
        if not re.fullmatch(r'[0-9A-Z_]*', args.reserved_prefix):
            raise ValueError("Reserved prefix should only contain upper-case alphanumeric characters or '_'.")
        conf.reserved_prefix = args.reserved_prefix
    conf.metadata_path = args.metadata_path


def upload_func(args: argparse.Namespace):
    conf = UploadConfig()
    populate_sync_config(args, conf)
    if args.file_base_bytes is not None:
        conf.file_base_bytes = args.file_base_bytes
    if args.merge_threshold is not None:
        suffix_map = {'': 1, 'k': 1024, 'm': 1024 ** 2, 'g': 1024 ** 3, 't': 1024 ** 4}
        merge_thresh = re.fullmatch(r'(\d+)([kmgt]?)', args.merge_threshold.lower())
        if not merge_thresh:
            raise ValueError('Invalid size pattern.')
        conf.merge_threshold = int(merge_thresh[1]) * suffix_map[merge_thresh[2]]
    conf.delete_after_upload = args.delete_after_upload
    if args.grouping_order is not None:
        if args.grouping_order not in ['size', 'mtime', 'ctime']:
            raise ValueError('Invalid grouping order.')
        conf.grouping_order = args.grouping_order
    if args.compression_suffix is not None:
        if not re.fullmatch(r'[0-9a-zA-Z_.]*', args.compression_suffix):
            raise ValueError("Compression suffix should only contain alphanumeric characters, '.' or '_'.")
        conf.compression_suffix = args.compression_suffix
    else:
        if not conf.deduct_compression_suffix():
            raise ValueError('Unknown compression; please specify --compression-suffix')

    src = args.local.encode()
    dest = args.remote.encode()
    if args.include_file:
        conf.set_include_list(src, map(lambda x: x.encode(), args.include_file))
    if args.exclude_file:
        conf.set_exclude_list(src, map(lambda x: x.encode(), args.exclude_file))
    result = upload(src, dest, conf)
    print(result)


def download_func(args: argparse.Namespace):
    conf = DownloadConfig()
    populate_sync_config(args, conf)
    result = download(args.local.encode(), args.remote.encode(), conf)
    print(result)


def cli_entry():
    parser = get_parser()
    args = parser.parse_args()
    try:
        args.func(args)
    except ValueError as e:
        parser.error(str(e))
