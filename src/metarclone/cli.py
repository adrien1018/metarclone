import argparse
import logging
import os
import re
import sys

from .config import SyncConfig, UploadConfig, DownloadConfig
from .upload import upload
from .download import download


def get_parser():
    root_parser = argparse.ArgumentParser(prog='metarclone')
    subparsers = root_parser.add_subparsers(title='methods', dest='method', required=True,
                                            metavar='method', help=None)

    def add_sync_arguments(parser: argparse.ArgumentParser):
        parser.add_argument('-v', dest='verbose', action='count', default=0,
                            help="Verbose output. Use -vv for more verbosity.")
        parser.add_argument('--stats', action='store_true',
                            help="Output statistics at the end")
        parser.add_argument('--dest-as-empty', action='store_true',
                            help="Treat destination as empty; "
                                 "that is, upload / download without checking checksums and existing files")
        parser.add_argument('-c', '--use-file-checksum', action='store_true',
                            help="Use whole-file checksum instead of timestamp to determine whether a file changed")
        parser.add_argument('--use-directory-mtime', action='store_true',
                            help="Update an aggregated file if the mtime of any directory in it changed")
        parser.add_argument('--use-owner', action='store_true',
                            help="Update a file if its ownership changed")
        parser.add_argument('--checksum-choice', metavar='hashfn',
                            help="The program hashes timestamps, filenames and other information in an aggregated file "
                                 "into a single checksum using this checksum function. Default: sha1")
        parser.add_argument('--ignore-errors', action='store_true',
                            help="Suppress non-zero exit code (we emit a warning, exit with non-zero at the end and "
                                 "treat the file that caused the error as nonexistent (if applicable) if "
                                 "any non-fatal error occurred")
        parser.add_argument('--rclone-args', metavar='args...',
                            help='Additional arguments passed to rclone')
        # specify 'none' for no compression
        parser.add_argument('-I', '--use-compress-program', metavar='prog[ args...]',
                            help="Compression program (passed to tar's -I option); "
                                 "specify 'none' to disable compression. "
                                 "Must use the same compress program for upload and download. "
                                 "Default: gzip")
        parser.add_argument('--tar-path', metavar='path', help="The path to tar program")
        parser.add_argument('--rclone-path', metavar='path', help="The path to rclone program")
        parser.add_argument('--metadata-path', metavar='path',
                            help="The path to metadata file; can be a local path or a remote path. "
                                 "Defaults to _METARCLONE_META.json.gz under the remote path. "
                                 "This file stores information of the current status of remote files, "
                                 "so it must be instant retrievable. Full re-sync is needed if this file is lost.")
        parser.add_argument('--reserved-prefix', metavar='prefix')

    upload_parser = subparsers.add_parser('upload', usage="metarclone upload [-h] [options...] local remote",
                                          help="Upload to rclone remote")
    add_sync_arguments(upload_parser)
    upload_parser.add_argument('--file-base-bytes', type=int, metavar='bytes',
                               help="Add this size to each file and directory when calculating file size "
                                    "for aggregation. Default: 64")
    upload_parser.add_argument('--merge-threshold', metavar='size',
                               help="Merge files or directories smaller than this threshold into larger files. "
                                    "Use K,M,G,T suffix (case-insensitive) to indicate KiB,MiB,GiB,TiB. "
                                    "A directory will contain at most one aggregated file smaller than this threshold. "
                                    "Default: 10M")
    upload_parser.add_argument('--s3-min-chunk-size', metavar='size',
                               help="Minimum S3 upload chunk size. "
                                    "(Do not specify --s3-chunk-size using --rclone-args, since metarclone "
                                    "will increase chunk size automatically if the upload file size is large)")
    upload_parser.add_argument('--delete-before-upload', dest='delete_after_upload', action='store_false',
                               help="Delete remote unused files before upload "
                                    "(default is deleting them after completing upload)")
    upload_parser.add_argument('--grouping-order', metavar='order',
                               help="Group files smaller than threshold using this order. "
                                    "Possible choices: size, name, ctime, mtime. Default: size")
    upload_parser.add_argument('--compression-suffix', metavar='suffix',
                               help="The file suffix of compressed tarballs. "
                                    "The program will try to deduct it if -I is specified. Default: .gz")
    # specified by relative path from upload root or absolute path
    upload_parser.add_argument('--exclude-file', action='append', metavar='path',
                               help="Upload these files only. Specify once for each path. "
                                    "Can be a relative path from local base path or an absolute path.")
    upload_parser.add_argument('--include-file', action='append', metavar='path',
                               help="Upload these files only. Specify once for each path. "
                                    "Can be a relative path from local base path or an absolute path. ")
    upload_parser.add_argument('local', help="Local base path")
    upload_parser.add_argument('remote', help="Remote base path")
    upload_parser.set_defaults(func=upload_func)

    download_parser = subparsers.add_parser('download', usage="metarclone download [-h] [options...] remote local",
                                            help='Download from rclone remote')
    add_sync_arguments(download_parser)
    download_parser.add_argument('remote', help="Remote base path")
    download_parser.add_argument('local', help="Local base path")
    download_parser.set_defaults(func=download_func)

    return root_parser


def parse_size_bytes(x: str):
    suffix_map = {'': 1024, 'b': 1, 'k': 1024, 'm': 1024 ** 2, 'g': 1024 ** 3, 't': 1024 ** 4}
    match = re.fullmatch(r'(\d+(?:\.\d+)?)([bkmgt]?)', x.lower())
    if not match:
        raise ValueError('Invalid size pattern.')
    return int(float(match[1]) * suffix_map[match[2]])


def populate_sync_config(args: argparse.Namespace, conf: SyncConfig):
    if args.verbose == 0:
        logging.getLogger().setLevel(logging.WARNING)
    elif args.verbose == 1:
        logging.getLogger().setLevel(logging.INFO)
    else:
        logging.getLogger().setLevel(logging.DEBUG)

    conf.dest_as_empty = args.dest_as_empty
    conf.use_file_checksum = args.use_file_checksum
    conf.use_directory_mtime = args.use_directory_mtime
    conf.use_owner = args.use_owner
    if args.checksum_choice is not None:
        conf.set_hash_function(args.checksum_choice)
    conf.ignore_errors = args.ignore_errors
    if args.rclone_args is not None:
        if 's3-chunk-size' in args.rclone_args:
            logging.warning("Specifying --s3-chunk-size using --rclone-args will likely make large uploads fail.")
        conf.rclone_args = args.rclone_args.split()
    if args.use_compress_program is not None:
        conf.compression = args.use_compress_program
    if conf.compression == 'none':
        conf.compression = None
    if args.tar_path is not None:
        conf.tar_command = args.tar_path
    if args.rclone_path is not None:
        conf.rclone_command = args.rclone_path
    if args.reserved_prefix is not None:
        if not re.fullmatch(r'[0-9A-Z_]*', args.reserved_prefix):
            raise ValueError("Reserved prefix should only contain upper-case alphanumeric characters or '_'.")
        conf.reserved_prefix = args.reserved_prefix
    conf.metadata_path = args.metadata_path
    conf.convert_command_to_abs_path()


def upload_func(args: argparse.Namespace):
    conf = UploadConfig()
    populate_sync_config(args, conf)
    if args.file_base_bytes is not None:
        conf.file_base_bytes = args.file_base_bytes
    if args.merge_threshold is not None:
        conf.merge_threshold = parse_size_bytes(args.merge_threshold)
    if args.s3_min_chunk_size is not None:
        conf.s3_min_chunk_size_kib = parse_size_bytes(args.s3_min_chunk_size) // 1024
    conf.delete_after_upload = args.delete_after_upload
    if args.grouping_order is not None:
        if args.grouping_order not in ['size', 'mtime', 'ctime', 'name']:
            raise ValueError('Invalid grouping order.')
        conf.grouping_order = args.grouping_order
    if args.compression_suffix is not None:
        if not re.fullmatch(r'[0-9a-zA-Z_.]*', args.compression_suffix):
            raise ValueError("Compression suffix should only contain alphanumeric characters, '.' or '_'.")
        conf.compression_suffix = args.compression_suffix
    else:
        if not conf.deduct_compression_suffix():
            raise ValueError('Unknown compression; please specify --compression-suffix')

    src: bytes = os.path.normpath(args.local).encode()
    dest: str = args.remote
    if args.include_file:
        conf.set_include_list(src, map(lambda x: x.encode(), args.include_file))
    if args.exclude_file:
        conf.set_exclude_list(src, map(lambda x: x.encode(), args.exclude_file))
    result = upload(src, dest, conf)

    if args.stats:
        print()
        print(f'All files: {result.total_size} bytes, {result.total_files} files')
        print(f'Files to transfer: {result.total_transfer_size} bytes, {result.total_transfer_files} files')
        print(f'Sent to remote: {result.real_transfer_size} bytes, {result.real_transfer_files} files')
        print(f'Deleted {result.total_deleted_files} remote files')
    if result.error_count > 0:
        logging.error(f'{result.error_count} errors occured; check previous output')
        sys.exit(1)


def download_func(args: argparse.Namespace):
    conf = DownloadConfig()
    populate_sync_config(args, conf)

    dest: bytes = args.local.encode()
    src: str = args.remote
    result = download(dest, src, conf)
    print(result, result.__dict__)

    if args.stats:
        print()
        print(f'Received from remote: {result.real_transfer_size} bytes, {result.real_transfer_files} files')
    if result.error_count > 0:
        logging.error(f'{result.error_count} errors occured; check previous output')
        sys.exit(1)


def cli_entry():
    parser = get_parser()
    args = parser.parse_args()
    try:
        if os.name == 'nt':
            if 'MSYSTEM' not in os.environ:
                logging.warning('Running directly in Windows is not supported. This command is likely going to fail. '
                                'Please install Git Bash or MSYS2 and run inside it.')
        args.func(args)
    except ValueError as e:
        import traceback
        traceback.print_exc()
        parser.error(str(e))
