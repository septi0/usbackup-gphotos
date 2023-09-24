import os
import logging
from datetime import datetime
from usbackup_gphotos.gauth import GAuth
from usbackup_gphotos.gphotos_api import GPhotosApi
from usbackup_gphotos.media_items import MediaItems
from usbackup_gphotos.albums import Albums
from usbackup_gphotos.storage import Storage
from usbackup_gphotos.media_items_model import MediaItemsModel
from usbackup_gphotos.albums_model import AlbumsModel
from usbackup_gphotos.settings_model import SettingsModel

__all__ = ['UsBackupGPhotosIdentity', 'UsBackupGPhotosIdentityError']

class UsBackupGPhotosIdentityError(Exception):
    pass

class UsBackupGPhotosIdentity:
    def __init__(self, name: str, config: dict, *, logger: logging.Logger) -> None:
        self._name: str = name
        self._logger: logging.Logger = logger.getChild(self._name)
        self._settings_model: SettingsModel = None
        self._settings: dict = None
        self._lock_file: str = None

        self._gauth: GAuth = None
        self._media_items: MediaItems = None
        self._albums: Albums = None

        self._setup(config)

    @property
    def name(self) -> str:
        return self._name
    
    def lock(self) -> None:
        if os.path.exists(self._lock_file):
            raise UsBackupGPhotosIdentityError(f'Lock file "{self._lock_file}" already exists. Is another instance running?')
        
        with open(self._lock_file, 'w') as f:
            f.write(str(os.getpid()))

    def unlock(self) -> None:
        if not os.path.exists(self._lock_file):
            return
        
        with open(self._lock_file, 'r') as f:
            pid = f.read()

        if pid != str(os.getpid()):
            raise UsBackupGPhotosIdentityError(f'Lock file "{self._lock_file}" is not owned by current process') from None
        
        os.remove(self._lock_file)

    def index(self, options: dict) -> None:
        self._gauth.ensure_valid_auth()

        if not options.get('skip_media_items'):
            # index media items ########################################
            self._logger.info(f'* Indexing media items')

            mi_sdate = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            processed = self._media_items.index_items(
                last_index=self._settings.get('media_items_last_index', None),
                rescan=options.get('rescan', False)
            )

            if bool(processed):
                if processed['indexed'] and not processed['failed']:
                    self._update_aseting('media_items_last_index', mi_sdate)

                self._logger.info(f'Processed {processed.total} media items ({processed})')
            else:
                self._logger.info(f'No media items indexed')
            ############################################################

        if not options.get('skip_albums'):
            # index albums #############################################
            self._logger.info(f'* Indexing albums')

            a_sdate = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            processed = self._albums.index_albums(
                last_index=self._settings.get('albums_last_index', None),
                rescan=options.get('rescan', False),
                filter_albums=options.get('albums', []),
            )

            if bool(processed):
                if processed['indexed'] and not processed['failed']:
                    self._update_aseting('albums_last_index', a_sdate)

                self._logger.info(f'Processed {processed.total} albums ({processed})')
            else:
                self._logger.info(f'No albums indexed')
            ############################################################

    def sync(self, options: dict) -> None:
        self._gauth.ensure_valid_auth()

        if not options.get('skip_index'):
            self.index({
                'skip_media_items': options.get('skip_media_items', False),
                'skip_albums': options.get('skip_albums', False),
                'rescan': options.get('rescan', False),
                'albums': options.get('albums', []),
            })

        # scan synced media items on filesystem ####################
        processed = self._media_items.scan_synced_items_fs()

        if bool(processed):
            self._logger.info(f'Fixed {processed["fixed"]} missing media items from filesystem')
        ############################################################

        # scan synced albums items on filesystem ###################
        processed = self._albums.scan_synced_albums_items_fs()

        if bool(processed):
            self._logger.info(f'Fixed {processed["fixed"]} missing albums items from filesystem')
        ############################################################

        # sync media items #########################################
        self._logger.info(f'* Syncing media items')
        processed = self._media_items.sync_items(
            concurrency=options.get('concurrency', 20),
        )

        if bool(processed):
            self._logger.info(f'Processed {processed.total} media items ({processed})')
        else:
            self._logger.info(f'No media items synced')
        ############################################################

        # sync albums ##############################################
        self._logger.info(f'* Syncing albums items')

        processed = self._albums.sync_albums_items(
            concurrency=options.get('concurrency', 20),
        )

        if bool(processed):
            self._logger.info(f'Processed {processed.total} albums items ({processed})')
        else:
            self._logger.info(f'No albums synced')
        ############################################################

    def delete_obsolete(self) -> None:
        # delete obsolete albums items #############################
        self._logger.info(f'* Deleting obsolete albums items')
        processed = self._albums.delete_obsolete_albums_items()

        if bool(processed):
            self._logger.info(f'Processed {processed.total} albums items ({processed})')
        else:
            self._logger.info(f'No obsolete albums items deleted')
        ############################################################

        # delete obsolete albums ###################################
        self._logger.info(f'* Deleting obsolete albums')
        processed = self._albums.delete_obsolete_albums()

        if bool(processed):
            self._logger.info(f'Processed {processed.total} albums ({processed})')
        else:
            self._logger.info(f'No obsolete albums deleted')
        ############################################################

        # delete obsolete media items ##############################
        self._logger.info(f'* Deleting obsolete media items')
        processed = self._media_items.delete_obsolete_items()

        if bool(processed):
            self._logger.info(f'Processed {processed.total} media items ({processed})')
        else:
            self._logger.info(f'No obsolete media items deleted')
        ############################################################

    def auth(self) -> None:
        self._logger.info(f'* Authenticating')

        self._gauth.issue_new_token()

    def stats(self) -> dict:
        return {
            'media_items_last_index': self._settings.get('media_items_last_index', None),
            'albums_last_index': self._settings.get('albums_last_index', None),
            'media_items': self._media_items.stats(),
            'albums': self._albums.stats_albums(),
            'albums_items': self._albums.stats_albums_items(),
        }

    def ignore(self, options: dict) -> None:
        if options.get('set'):
            # ignore media items #######################################
            self._logger.info(f'* Ignoring media items')
            processed = self._media_items.ignore_items(options.get('set'))

            if bool(processed):
                self._logger.info(f'Processed {processed.total} media items ({processed})')
            else:
                self._logger.info(f'No media items ignored')
            ############################################################

        if options.get('reset'):
            # reset ignored media items ################################
            self._logger.info(f'* Resetting ignored media items')
            processed = self._media_items.reset_ignored_items()

            if bool(processed):
                self._logger.info(f'Processed {processed.total} media items ({processed})')
            else:
                self._logger.info(f'No media items reset')
            ############################################################

    def _setup(self, config: dict) -> None:
        data_dir = self._gen_data_dir(config.get('data_dir', ''))
        library_dir = os.path.join(data_dir, 'library')
        storage_file = os.path.join(data_dir, 'usbackup_gphotos.db')
        auth_file = config.get('auth_file', '')
        auth_scopes = [
            'https://www.googleapis.com/auth/photoslibrary.readonly',
            'https://www.googleapis.com/auth/photoslibrary.sharing',
        ]

        storage = Storage(storage_file)

        settings_model = SettingsModel(storage)
        mi_model = MediaItemsModel(storage)
        a_model = AlbumsModel(storage)

        settings = settings_model.get_settings()
        token_hash = settings.get('token_hash')

        gauth = GAuth(auth_file, token_hash, auth_scopes, logger=self._logger)

        gauth.set_auth_callback(self._update_token_hash)

        if config.get('webserver'):
            gauth.set_webserver(port=config.get('webserver_port', 8080))

        google_api = GPhotosApi(gauth=gauth, logger=self._logger)

        self._settings_model = settings_model
        self._settings = settings
        self._lock_file = os.path.join(data_dir, 'usbackup_gphotos.lock')

        self._gauth = gauth
        self._media_items = MediaItems(library_dir, model=mi_model, google_api=google_api, logger=self._logger)
        self._albums = Albums(library_dir, model=a_model, google_api=google_api, media_items=self._media_items, logger=self._logger)

    def _gen_data_dir(self, data_dir: str) -> str:
        data_dir = os.path.realpath(data_dir)

        if not data_dir:
            raise UsBackupGPhotosIdentityError('Data dir not provided')

        if not os.path.exists(data_dir):
            self._logger.info(f'Creating destination directory "{data_dir}"')
            os.makedirs(data_dir)
        else:
            # make sure directory is writable
            if not os.access(data_dir, os.W_OK):
                raise ValueError(f'Destination directory "{data_dir}" is not writable')
            
        return data_dir
    
    def _get_settings(self) -> dict:
        settings = self._settings_t.select().fetchall()

        if not settings:
            return {}
        
        return {s['key']: s['value'] for s in settings}
    
    def _update_aseting(self, key: str, value: str) -> int:
        return self._settings_model.update_aseting(key, value)
    
    def _update_token_hash(self, token_hash: str) -> None:
        self._update_aseting('token_hash', token_hash)