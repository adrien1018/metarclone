import argparse


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

    upload_parser = subparsers.add_parser('upload', help='Upload to rclone remote')
    add_sync_arguments(upload_parser)
    upload_parser.add_argument('--file-base-bytes', type=int)
    upload_parser.add_argument('--merge-threshold')
    upload_parser.add_argument('--delete-after-upload', action='store_false')
    upload_parser.add_argument('--grouping-order')
    upload_parser.add_argument('--reserved-prefix')
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


def upload_func(args):
    print(args)


def download_func(args):
    print(args)


def cli_entry():
    parser = get_parser()
    args = parser.parse_args()
    args.func(args)
