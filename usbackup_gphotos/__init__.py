import sys
import argparse
from usbackup_gphotos.manager import UsBackupGPhotosManager, UsBackupGPhotosConfigError, UsBackupGPhotosIdentity
from usbackup_gphotos.info import __app_name__, __version__, __description__, __author__, __author_email__, __author_url__, __license__

def main():
    # get args from command line
    parser = argparse.ArgumentParser(description=__description__)

    parser.add_argument('--config', dest='config_files', action='append', help='Alternative config file(s)')
    parser.add_argument('--identity', dest='identities', action='append', help='Identity name(s)')
    parser.add_argument('--use-webserver', dest='use_webserver', help='Use webserver to authenticate', action='store_true', default=False)
    parser.add_argument('--log', dest='log_file', help='Log file where to write logs')
    parser.add_argument('--log-level', dest='log_level', help='Log level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], default='INFO')
    parser.add_argument('--version', action='version', version=f'{__app_name__} {__version__}')

    subparsers = parser.add_subparsers(title="Commands", dest="command")

    index_parser = subparsers.add_parser('index', help='Index media items')
    index_parser.add_argument('--skip-media-items', dest='skip_media_items', help='Skip media items sync', action='store_true', default=False)
    index_parser.add_argument('--skip-albums', dest='skip_albums', help='Skip albums sync', action='store_true', default=False)
    index_parser.add_argument('--rescan', dest='rescan', help='Rescan all media items', action='store_true', default=False)
    index_parser.add_argument('--album', dest='albums', action='append', help='Album name(s)')

    sync_parser = subparsers.add_parser('sync', help='Sync media items')
    sync_parser.add_argument('--skip-index', dest='skip_index', help='Skip indexing', action='store_true', default=False)
    sync_parser.add_argument('--skip-media-items', dest='skip_media_items', help='Skip media items sync', action='store_true', default=False)
    sync_parser.add_argument('--skip-albums', dest='skip_albums', help='Skip albums sync', action='store_true', default=False)
    sync_parser.add_argument('--rescan', dest='rescan', help='Rescan all media items', action='store_true', default=False)
    sync_parser.add_argument('--album', dest='albums', action='append', help='Album name(s)')
    sync_parser.add_argument('--concurrency', dest='concurrency', help='Concurrency', type=int, default=10)

    delete_parser = subparsers.add_parser('delete', help='Delete obsolete media items')

    auth_parser = subparsers.add_parser('auth', help='Authenticate')
    stats_parser = subparsers.add_parser('stats', help='Stats')

    ignore_parser = subparsers.add_parser('ignore', help='Ignore media items')
    # create mutually exclusive group
    ignore_group = ignore_parser.add_mutually_exclusive_group(required=True)
    ignore_group.add_argument('--set', dest='set', action='append', help='Set ignored media id(s)')
    ignore_group.add_argument('--reset', dest='reset', help='Reset ignored media ids', action='store_true', default=False)

    args = parser.parse_args()

    if args.command is None:
      parser.print_help()
      sys.exit()

    try:
        usbackup_gphotos = UsBackupGPhotosManager({
            'config_files': args.config_files,
            'identities': args.identities,
            'use_webserver': args.use_webserver,
            'log_file': args.log_file,
            'log_level': args.log_level,
        })
    except UsBackupGPhotosConfigError as e:
        print(f"Config error: {e}\nCheck documentation for more information on how to configure {__app_name__} identities")
        sys.exit(2)

    if args.command == 'index':
        usbackup_gphotos.index({
            'skip_media_items': args.skip_media_items,
            'skip_albums': args.skip_albums,
            'rescan': args.rescan,
            'albums': args.albums,
        })
    elif args.command == 'sync':
        usbackup_gphotos.sync({
            # index options
            'skip_media_items': args.skip_media_items,
            'skip_albums': args.skip_albums,
            'rescan': args.rescan,
            'albums': args.albums,
            # sync options
            'skip_index': args.skip_index,
            'concurrency': args.concurrency,
        })
    elif args.command == 'delete':
        usbackup_gphotos.delete_obsolete()
    elif args.command == 'auth':
        usbackup_gphotos.auth()
    elif args.command == 'stats':
        usbackup_gphotos.stats()
    elif args.command == 'ignore':
        usbackup_gphotos.ignore({
            'set': args.set,
            'reset': args.reset,
        })

    sys.exit(0)