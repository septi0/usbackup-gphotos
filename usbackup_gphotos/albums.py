import os
import logging
import asyncio
from datetime import datetime
from usbackup_gphotos.albums_model import AlbumsModel
from usbackup_gphotos.media_items import MediaItems
from usbackup_gphotos.gphotos_api import GPhotosApi
from usbackup_gphotos.action_stats import ActionStats
from usbackup_gphotos.utils import transform_fs_safe, gen_batch_stats

__all__ = ['Albums']

class Albums:
    def __init__(self, dest_path: str, *, model: AlbumsModel, google_api: GPhotosApi, media_items: MediaItems, logger: logging.Logger) -> None:
        self._dest_path: str = dest_path
        self._model: AlbumsModel = model

        self._google_api: GPhotosApi = google_api
        self._media_items: MediaItems = media_items
        self._logger: logging.Logger = logger.getChild('albums')

        self._album_list_limit: int = 50
        self._album_items_list_limit: int = 100

        self._albums_dir = 'albums'

    @property
    def dest_path(self) -> str:
        return self._dest_path

    def index_albums(self, *, last_index: str = None, rescan: bool = False, filter_albums: list = []) -> ActionStats:
        page_token = None
        limit = self._album_list_limit
        check_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        info = ActionStats(indexed=0, skipped=0, failed=0)

        # TODO: list albums by mdate greater than last_index (if it will be available in API)

        # always rescan for now
        rescan = True

        while True:
            to_index = self._google_api.albums_list(page_token=page_token, page_size=limit)

            if not to_index:
                break

            albums = to_index.get('albums', [])
            page_token = to_index.get('nextPageToken')

            for album in albums:
                try:
                    status = self.index_album(album, filter_albums, commit=False)
                except Exception as e:
                    self._logger.error(f'Index for album "{album["title"]}" failed. {e}')
                    info.increment(failed=1)
                else:
                    if status == 'indexed':
                        info.increment(indexed=1)
                    else:
                        info.increment(skipped=1)

            self._model.commit()

            if not page_token:
                break

        if rescan and not filter_albums:
            # mark all albums older than check_date as stale
            stale_cnt = self._model.set_albums_meta_stale(last_checked=check_date)
            self._propagate_stale_albums()

            if stale_cnt:
                self._logger.info(f'Marked {stale_cnt} albums as stale')

        return info
    
    def index_album(self, album: dict, filter_albums: list = None, *, commit=True) -> str:
        album_meta = self._model.get_album_meta(remote_id=album['id'])

        if filter_albums and album['title'] not in filter_albums:
            self._logger.debug(f'Index for album "{album["title"]}" skipped. Filtered out')
            return 'skipped'

        # check if album was renamed
        if album_meta and album_meta['name'] != album['title']:
            self._logger.info(f'Queueing album "{album_meta["name"]}" for rename to "{album["title"]}"')
            # TODO: queue album for rename

        if not self._index_needed(album_meta, album):
            last_checked = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self._model.update_album_meta(album_meta['album_id'], last_checked=last_checked)

            self._logger.debug(f'Index for album "{album_meta["name"]}" skipped. Up to date')

            return 'skipped'
        
        self._add_album(album)

        if commit:
            self._model.commit()

        # retrieve album meta again in case it wasn't created initially
        album_meta = self._model.get_album_meta(remote_id=album['id'])

        try:
            self.index_album_items(album_meta['album_id'], commit=commit)
        except Exception as e:
            self._model.update_album_meta(album_meta['album_id'], status='index_error')

            raise e from None

        return 'indexed'

    def index_album_items(self, album_id: int, *, commit=True) -> ActionStats:
        album_meta = self._model.get_album_meta(album_id=album_id)
        page_token = None
        limit = self._album_items_list_limit
        info = ActionStats(indexed=0, failed=0)

        self._model.set_albums_items_meta_stale(album_id=album_meta['album_id'])

        while True:
            to_index = self._google_api.media_items_search(album_id=album_meta['remote_id'], page_token=page_token, page_size=limit)

            if not to_index:
                break

            media_items = to_index.get('mediaItems', [])
            page_token = to_index.get('nextPageToken')

            for media_item in media_items:
                self._add_album_item(album_meta, media_item)
                info.increment(indexed=1)

            self._logger.info(f'Album items batch index: indexed {len(to_index.get("mediaItems", []))}')

            if commit:
                self._model.commit()

            if not page_token:
                break

        return info

    def sync_albums_items(self, *, concurrency: int = 1, use_symlinks: bool = True) -> ActionStats:
        return asyncio.run(self._sync_albums_items(concurrency=concurrency, use_symlinks=use_symlinks))
    
    def delete_obsolete_albums(self) -> ActionStats:
        limit = 100
        offset = 0
        total = self._model.get_albums_meta_cnt(status='stale')
        info = ActionStats(deleted=0, failed=0)

        if not total:
            return info

        while True:
            to_delete = self._model.search_albums_meta(limit=limit, offset=offset, status='stale')

            if not to_delete:
                break

            for album_meta in to_delete:
                try:                    
                    self._delete_album_dir(album_meta)
                    self._model.delete_album_meta(album_meta['album_id'])
                except Exception as e:
                    self._logger.error(f'Deletion for album "{album_meta["name"]}" failed. Reason: {e}')

                    offset += 1
                    info.increment(failed=1)
                else:
                    info.increment(deleted=1)

            self._model.commit()

        return info

    def delete_obsolete_albums_items(self) -> ActionStats:
        info = ActionStats(deleted=0, failed=0)

        db_info = self._delete_obsolete_albums_items_db()
        info.increment(**dict(db_info))

        fs_info = self._delete_obsolete_albums_items_fs()
        info.increment(**dict(fs_info))

        return info

    def get_album_meta(self, *, album_id: int = None, remote_id: str = None) -> dict:
        return self._model.get_album_meta(album_id=album_id, remote_id=remote_id)
    
    def stats_albums(self) -> dict:
        return self._model.get_albums_meta_stats()
    
    def stats_albums_items(self) -> dict:
        return self._model.get_albums_items_meta_stats()
    
    def scan_synced_albums_items_fs(self) -> ActionStats:
        limit = 100
        offset = 0
        total = self._model.get_albums_items_meta_cnt(status='synced')
        info = ActionStats(fixed=0)

        if not total:
            return info
        
        while True:
            to_check = self._model.search_albums_items_meta(limit=limit, offset=offset, status='synced')

            if not to_check:
                break

            for album_item_meta in to_check:
                if not album_item_meta['item_cname'] or not album_item_meta['album_cname']:
                    self._logger.warning(f'Missing meta for album item #{album_item_meta["album_item_id"]}')
                    continue

                if not self._album_item_exists_fs(album_item_meta):
                    self._logger.debug(f'Media item "{album_item_meta["item_name"]}" not found on filesystem. Setting status to pending_sync')
                    self._model.update_album_item_meta(album_item_meta['album_item_id'], status='pending_sync')

                    info.increment(fixed=1)

            self._model.commit()

            offset += limit

        return info
    
    def _get_canonicalized_name(self, album_name: str, path: str) -> str:
        unique = 1

        if not album_name:
            album_name = 'Untitled'

        album_name = transform_fs_safe(album_name)

        while True:
            if not self._model.search_albums_meta(cname=album_name, path=path):
                return album_name

            name, ext = os.path.splitext(album_name)

            album_name = f'{name} ({unique}){ext}'

            unique += 1

    def _delete_album_dir(self, album_meta: dict) -> None:
        dest_dir = os.path.join(self._dest_path, album_meta['path'], album_meta['cname'])

        self._logger.info(f'Deleting album "{album_meta["name"]}"')

        if os.path.isdir(dest_dir):
            os.rmdir(dest_dir)
        else:
            self._logger.debug(f'Deletion for album "{album_meta["name"]}" skipped. Directory not found')

    def _delete_album_item_file(self, album_item_meta: dict) -> None:
        if not album_item_meta['item_cname'] or not album_item_meta['album_cname']:
            raise ValueError(f'Missing meta for album item #{album_item_meta["album_item_id"]}')
        
        dest_file = os.path.join(
            self._dest_path,
            album_item_meta['album_path'],
            album_item_meta['album_cname'],
            album_item_meta['item_cname']
        )

        self._logger.debug(f'Deleting album item "{album_item_meta["item_name"]}" ("{album_item_meta["album_name"]}")')

        if os.path.isfile(dest_file):
            os.remove(dest_file)
        else:
            self._logger.debug(f'Deletion for album item "{album_item_meta["item_name"]}" ("{album_item_meta["album_name"]}") skipped. File not found')

    def _album_item_exists_fs(self, album_item_meta: dict) -> bool:
        dest_file = os.path.join(
            self._dest_path,
            album_item_meta['album_path'],
            album_item_meta['album_cname'],
            album_item_meta['item_cname']
        )

        return os.path.isfile(dest_file)
    
    def _propagate_stale_albums(self) -> None:
        limit = 100
        offset = 0
        total = self._model.get_albums_meta_cnt(status='stale')

        if not total:
            return
        
        while True:
            to_propagate = self._model.search_albums_meta(limit=limit, offset=offset, status='stale')

            if not to_propagate:
                break

            for album_meta in to_propagate:
                self._model.set_albums_items_meta_stale(album_id=album_meta['album_id'])

            offset += limit
    
    def _index_needed(self, album_meta: dict, album: dict) -> bool:
        if not album_meta:
            return True
        
        album_items_cnt = self._model.get_albums_items_meta_cnt(album_id=album_meta['album_id'], status_not=['stale'])
        
        synced = album_meta['status'] in ['indexed']
        same_size = int(album_meta['size']) == int(album['mediaItemsCount']) == album_items_cnt

        if not synced or not same_size:
            return True
        
        # TODO: check mdate (if it will be available in API)
        
        return False

    def _add_album(self, album: dict) -> int:
        path = self._albums_dir
        cname = self._get_canonicalized_name(album['title'], path)
        index_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        self._logger.info(f'Indexing album "{album["title"]}" with {album["mediaItemsCount"]} items')

        return self._model.add_album_meta(
            remote_id=album['id'],
            name=album['title'],
            cname=cname,
            size=album['mediaItemsCount'],
            cover_photo_id=album['coverPhotoMediaItemId'],
            path=path,
            index_date=index_date,
            last_checked=index_date,
            status='indexed',
        )

    def _add_album_item(self, album_meta: dict, media_item: dict) -> int:
        # make sure album item is indexed
        self._media_items.index_item(media_item)

        media_item_meta = self._media_items.get_item_meta(remote_id=media_item['id'])

        self._logger.debug(f'Indexing album item "{media_item["filename"]}"')

        return self._model.add_album_item_meta(
            album_id=album_meta['album_id'],
            media_item_id=media_item_meta['media_id'],
            status='pending_sync',
        )

    async def _sync_albums_items(self, *, concurrency: int = 1, use_symlinks: bool = True) -> ActionStats:
        limit = 100
        offset = 0
        opts = {'use_symlinks': use_symlinks,}
        total = self._model.get_albums_items_meta_cnt(status=['pending_sync', 'sync_error'])
        info = ActionStats(synced=0, skipped=0, failed=0)

        if not total:
            return info

        processed = 0
        t_start = datetime.now()

        while True:
            to_sync = self._model.search_albums_items_meta(limit=limit, offset=offset, status=['pending_sync', 'sync_error'])

            if not to_sync:
                break

            # break items into chunks of concurrency length
            chunks_to_sync = [to_sync[i:i + concurrency] for i in range(0, len(to_sync), concurrency)]

            # sync album items in chunks concurrently
            for chunk in chunks_to_sync:
                c_info = await self._sync_album_items_concurrently(chunk, **opts)

                offset += c_info['failed']
                info.increment(**dict(c_info))

            self._model.commit()

            t_end = datetime.now()
            processed += len(to_sync)
            
            (percentage, eta) = gen_batch_stats(t_start, t_end, processed, total)

            self._logger.info(f'Albums items batch sync: {percentage}, eta: {eta}')

        return info
    
    async def _sync_album_items_concurrently(self, to_sync: list, **opts) -> ActionStats:
        tasks = []
        info = ActionStats(synced=0, skipped=0, failed=0)

        for album_item_meta in to_sync:
            # sync album item
            tasks.append(asyncio.create_task(self._sync_album_item(album_item_meta, **opts), name=album_item_meta['album_item_id']))

        await asyncio.gather(*tasks, return_exceptions=True)

        # update items status based on async tasks results
        for t in tasks:
            if t.exception():
                self._logger.error(f'Sync for album item #{t.get_name()} failed. Reason {t.exception()}')
                self._model.update_album_item_meta(t.get_name(), status='sync_error')

                info.increment(failed=1)
            else:
                status = t.result()
                status_upd = 'synced' if status in ['synced', 'skipped'] else status

                self._model.update_album_item_meta(t.get_name(), status=status_upd)

                if status == 'synced':
                    info.increment(synced=1)
                else:
                    info.increment(skipped=1)

        return info

    async def _sync_album_item(self, album_item_meta: dict, *, use_symlinks: bool = True) -> str:
        if not album_item_meta['item_cname'] or not album_item_meta['album_cname']:
            raise ValueError(f'Missing meta for album item #{album_item_meta["album_item_id"]}')
        
        if album_item_meta['item_status'] not in ['synced', 'ignored']:
            raise ValueError('media item is not synced')

        if album_item_meta['item_status'] == 'ignored':
            self._logger.debug(f'Sync for album item "{album_item_meta["item_name"]}" ("{album_item_meta["album_name"]}") skipped. Media item is ignored')
            return 'ignored'
        
        src_file = os.path.join(self._media_items.dest_path, album_item_meta['item_path'], album_item_meta['item_cname'])
        dest_path = os.path.join(self._dest_path, album_item_meta['album_path'], album_item_meta['album_cname'])
        dest_file = os.path.join(dest_path, album_item_meta['item_cname'])

        if not os.path.isfile(src_file):
            raise ValueError(f'missing source file')

        # if file already exists, skip
        if os.path.isfile(dest_file):
            self._logger.debug(f'Sync for album item "{album_item_meta["item_name"]}" ("{album_item_meta["album_name"]}") skipped. Item already exists')
            return 'skipped'

        self._logger.debug(f'Linking album item "{album_item_meta["item_name"]}" ("{album_item_meta["album_name"]}")')

        if not os.path.isdir(dest_path):
            os.makedirs(dest_path)

        if use_symlinks:
            src_file_relative = os.path.relpath(src_file, dest_path)

            # create symbolic link
            os.symlink(src_file_relative, dest_file)
        else:
            # use hard links
            os.link(src_file, dest_file)

        return 'synced'

    def _delete_obsolete_albums_items_db(self) -> ActionStats:
        limit = 100
        offset = 0
        total = self._model.get_albums_items_meta_cnt(status='stale')
        info = ActionStats(deleted=0, failed=0)

        if not total:
            return info

        while True:
            to_delete = self._model.search_albums_items_meta(limit=limit, offset=offset, status='stale')

            if not to_delete:
                break

            for album_item_meta in to_delete:
                try:
                    self._delete_album_item_file(album_item_meta)
                    self._model.delete_album_item_meta(album_item_meta['album_item_id'])
                except Exception as e:
                    self._logger.error(f'Deletion for album item "{album_item_meta["item_name"]}" ("{album_item_meta["album_name"]}") failed. Reason: {e}')

                    offset += 1
                    info.increment(failed=1)
                else:
                    info.increment(deleted=1)

            self._model.commit()

        return info
    
    def _delete_obsolete_albums_items_fs(self) -> ActionStats:
        albums_items_path = os.path.join(self._dest_path, self._albums_dir)
        info = ActionStats(deleted=0, failed=0)

        for root, dirs, files in os.walk(albums_items_path):
            if not files:
                continue

            for file in files:
                album = os.path.basename(root)
                album_item_meta = self._model.search_albums_items_meta(album_cname=album, item_cname=file)

                if not album_item_meta:
                    self._logger.debug(f'Album item "{file}" not found in database. Deleting')

                    try:
                        os.remove(os.path.join(root, file))
                    except Exception as e:
                        self._logger.error(f'Deletion for album item "{file}" failed. {e}')
                        info.increment(failed=1)
                    else:
                        info.increment(deleted=1)

        return info