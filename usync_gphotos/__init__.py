import sys
import argparse
from usync_gphotos.manager import USyncGPhotosManager, USyncGPhotosConfigError, USyncGPhotosIdentity
from usync_gphotos.info import __app_name__, __version__, __description__, __author__, __author_email__, __author_url__, __license__

def main():
    # get args from command line
    parser = argparse.ArgumentParser(description=__description__)

    parser.add_argument('--config', dest='config_files', action='append', help='Alternative config file(s)')
    parser.add_argument('--identity', dest='identities', action='append', help='Identity name')
    parser.add_argument('--use-webserver', dest='use_webserver', help='Use webserver to authenticate', action='store_true', default=False)
    parser.add_argument('--log', dest='log_file', help='Log file where to write logs')
    parser.add_argument('--log-level', dest='log_level', help='Log level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], default='INFO')
    parser.add_argument('--version', action='version', version=f'{__app_name__} {__version__}')

    subparsers = parser.add_subparsers(title="Commands", dest="command")

    sync_parser = subparsers.add_parser('sync', help='Sync photos')
    sync_parser.add_argument('--no-index', dest='no_index', help='Skip indexing', action='store_true', default=False)
    sync_parser.add_argument('--no-media-items', dest='no_media_items', help='Skip media items sync', action='store_true', default=False)
    sync_parser.add_argument('--no-albums', dest='no_albums', help='Skip albums sync', action='store_true', default=False)
    sync_parser.add_argument('--rescan', dest='rescan', help='Rescan all photos', action='store_true', default=False)
    sync_parser.add_argument('--album', dest='albums', action='append', help='Album name')
    sync_parser.add_argument('--concurrency', dest='concurrency', help='Concurrency', type=int, default=10)

    index_parser = subparsers.add_parser('index', help='Index photos')
    index_parser.add_argument('--no-media-items', dest='no_media_items', help='Skip media items sync', action='store_true', default=False)
    index_parser.add_argument('--no-albums', dest='no_albums', help='Skip albums sync', action='store_true', default=False)
    index_parser.add_argument('--rescan', dest='rescan', help='Rescan all photos', action='store_true', default=False)
    index_parser.add_argument('--album', dest='albums', action='append', help='Album name')

    auth_parser = subparsers.add_parser('auth', help='Authenticate')

    maintenance_parser = subparsers.add_parser('maintenance', help='Maintenance')
    maintenance_parser.add_argument('--delete-stale', dest='delete_stale', help='Delete stale items (photos not found in Google Photos anymore)', action='store_true', default=False)
    maintenance_parser.add_argument('--delete-orphaned', dest='delete_orphaned', help='Delete orphaned items (photos not found in index but still on filesystem)', action='store_true', default=False)
    maintenance_parser.add_argument('--ignore-media-id', dest='ignore_media_ids', action='append', help='Ignore media id')
    maintenance_parser.add_argument('--reset-ignored', dest='reset_ignored', help='Reset ignored media ids', action='store_true', default=False)

    stats_parser = subparsers.add_parser('stats', help='Stats')

    args = parser.parse_args()

    if args.command is None:
      parser.print_help()
      sys.exit()

    try:
        usync_gphotos = USyncGPhotosManager({
            'config_files': args.config_files,
            'identities': args.identities,
            'use_webserver': args.use_webserver,
            'log_file': args.log_file,
            'log_level': args.log_level,
        })
    except USyncGPhotosConfigError as e:
        print(f"Config error: {e}\nCheck documentation for more information on how to configure USync-GPhotos identities")
        sys.exit(2)

    if args.command == 'sync':
        usync_gphotos.sync({
            'no_index': args.no_index,
            'no_media_items': args.no_media_items,
            'no_albums': args.no_albums,
            'rescan': args.rescan,
            'albums': args.albums,
            'concurrency': args.concurrency,
        })
    elif args.command == 'index':
        usync_gphotos.index({
            'no_media_items': args.no_media_items,
            'no_albums': args.no_albums,
            'rescan': args.rescan,
            'albums': args.albums,
        })
    elif args.command == 'auth':
        usync_gphotos.auth()
    elif args.command == 'maintenance':
        usync_gphotos.maintenance({
            'delete_stale': args.delete_stale,
            'delete_orphaned': args.delete_orphaned,
            'ignore_media_ids': args.ignore_media_ids,
            'reset_ignored': args.reset_ignored,
        })
    elif args.command == 'stats':
        usync_gphotos.stats()

    sys.exit(0)