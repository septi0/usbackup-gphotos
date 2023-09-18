import os
import logging
from datetime import datetime
from usync_gphotos.gauth import GAuth
from usync_gphotos.gphotos_api import GPhotosApi
from usync_gphotos.media_items import MediaItems
from usync_gphotos.albums import Albums
from usync_gphotos.storage import Storage
from usync_gphotos.media_items_model import MediaItemsModel
from usync_gphotos.albums_model import AlbumsModel
from usync_gphotos.settings_model import SettingsModel

__all__ = ['USyncGPhotosIdentity', 'USyncGPhotosIdentityError']

class USyncGPhotosIdentityError(Exception):
    pass

class USyncGPhotosIdentity:
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
            raise USyncGPhotosIdentityError(f'Lock file "{self._lock_file}" already exists. Is another instance running?')
        
        with open(self._lock_file, 'w') as f:
            f.write(str(os.getpid()))

    def unlock(self) -> None:
        if not os.path.exists(self._lock_file):
            return
        
        with open(self._lock_file, 'r') as f:
            pid = f.read()

        if pid != str(os.getpid()):
            raise USyncGPhotosIdentityError(f'Lock file "{self._lock_file}" is not owned by current process') from None
        
        os.remove(self._lock_file)

    def index(self, options: dict) -> None:
        self._gauth.ensure_valid_auth()

        # index media items
        if not options.get('no_media_items'):
            self._logger.info(f'Indexing media items')

            mi_sdate = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            indexed = self._media_items.index(
                last_index=self._settings.get('media_items_last_index', None),
                rescan=options.get('rescan', False)
            )

            self._update_aseting('media_items_last_index', mi_sdate)

            if indexed:
                self._logger.info(f'Indexed {indexed} media items')
            else:
                self._logger.info(f'No media items indexed')

        if not options.get('no_albums'):
            self._logger.info(f'Indexing albums')

            a_sdate = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            indexed = self._albums.index(
                rescan=options.get('rescan', False),
                filter_albums=options.get('albums', []),
            )

            self._update_aseting('albums_last_index', a_sdate)

            if indexed:
                self._logger.info(f'Indexed {indexed} albums')
            else:
                self._logger.info(f'No albums indexed')

    def sync(self, options: dict) -> None:
        self._gauth.ensure_valid_auth()

        if not options.get('no_index'):
            self.index({
                'no_media_items': options.get('no_media_items', False),
                'no_albums': options.get('no_albums', False),
                'rescan': options.get('rescan', False),
                'albums': options.get('albums', []),
            })

        # sync media items
        self._logger.info(f'Syncing media items')
        self._media_items.sync(
            concurrency=options.get('concurrency', 20),
        )

        # sync albums
        self._logger.info(f'Syncing albums')
        self._albums.sync(
            concurrency=options.get('concurrency', 20),
        )

        # delete stale media
        if options.get('delete_stale'):
            self._logger.info(f'Deleting stale albums')
            self._albums.delete_stale()

            self._logger.info(f'Deleting stale media items')
            self._media_items.delete_stale()

    def auth(self) -> None:
        self._logger.info(f'Authenticating')

        self._gauth.issue_new_token()

    def _setup(self, config: dict) -> None:
        data_dir = self._gen_data_dir(config.get('data_dir', ''))
        storage_file = os.path.join(data_dir, 'usync_gphotos.db')
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
        self._lock_file = os.path.join(data_dir, 'usync_gphotos.lock')

        self._gauth = gauth
        self._media_items = MediaItems(data_dir, model=mi_model, google_api=google_api, logger=self._logger)
        self._albums = Albums(data_dir, model=a_model, google_api=google_api, media_items=self._media_items, logger=self._logger)

    def _gen_data_dir(self, data_dir: str) -> str:
        data_dir = os.path.realpath(data_dir)

        if not data_dir:
            raise USyncGPhotosIdentityError('Data dir not provided')

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