import os
import logging
import asyncio
from datetime import datetime
from usync_gphotos.albums_model import AlbumsModel
from usync_gphotos.media_items import MediaItems
from usync_gphotos.gphotos_api import GPhotosApi
from usync_gphotos.utils import transform_fs_safe, gen_batch_stats

__all__ = ['Albums']

class Albums:
    def __init__(self, dest_path: str, *, model: AlbumsModel, google_api: GPhotosApi, media_items: MediaItems, logger: logging.Logger) -> None:
        self._dest_path: str = dest_path
        self._model: AlbumsModel = model

        self._google_api: GPhotosApi = google_api
        self._media_items: MediaItems = media_items
        self._logger: logging.Logger = logger.getChild('library_index')

        self._album_list_limit: int = 50
        self._album_items_list_limit: int = 100

    @property
    def dest_path(self) -> str:
        return self._dest_path

    # rescan not used for now. Due to the limitations of the API, we can't get albums sorted by date
    # returns number of indexed albums
    def index(self, *, rescan: bool = False, filter_albums: list = []) -> int:
        page_token = None
        limit = self._album_list_limit
        index_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        processed = 0

        # TODO: list albums by date when it will be available in API

        while True:
            to_index = self._google_api.albums_list(page_token=page_token, page_size=limit)

            # if no albums to index, break
            if not to_index:
                break

            albums = to_index.get('albums', [])
            page_token = to_index.get('nextPageToken')
            batch_processed = 0

            for album in albums:
                if filter_albums and album['title'] not in filter_albums:
                    self._logger.debug(f'Index for album "{album["title"]}" skipped. Filtered out')
                    continue

                indexed = self.ensure_album_indexed(album)

                if indexed:
                    batch_processed += 1

            # commit batch
            self._model.commit()

            processed += batch_processed

            if not page_token:
                break

        self._model.set_albums_meta_stale(last_checked=index_date)
        self._model.set_albums_items_meta_stale()

        return processed

    def sync(self, *, concurrency: int = 1, use_symlinks: bool = True) -> dict:
        return asyncio.run(self._sync_albums(concurrency=concurrency, use_symlinks=use_symlinks))

    def delete_stale(self) -> None:
        self._delete_stale_albums_items()
        self._delete_stale_albums()

    def get_album_meta(self, *, album_id: int = None, remote_id: str = None) -> dict:
        return self._model.get_album_meta(album_id=album_id, remote_id=remote_id)

    def ensure_album_indexed(self, album: dict) -> bool:
        album_meta = self._model.get_album_meta(remote_id=album['id'])
        indexed = False

        if self._index_needed(album_meta, album):
            self._index_album(album)

            # retrieve album meta again in case it wasn't created initially
            # it saves us alot of queries when processing the items
            album_meta = self._model.get_album_meta(remote_id=album['id'])

            try:
                self._index_album_items(album_meta)
            except Exception as e:
                self._logger.error(f'Index for album "{album_meta["name"]}" failed. {e}')

            indexed = True
        else:
            last_checked = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self._model.update_album_meta(album_meta['album_id'], last_checked=last_checked)

            self._logger.debug(f'Index for album "{album_meta["name"]}" skipped. Up to date')

        return indexed
    
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
            try:
                os.rmdir(dest_dir)
            except OSError as e:
                self._logger.error(f'Deletion for album "{album_meta["name"]}" failed. Reason: {e}')
        else:
            self._logger.debug(f'Deletion for album "{album_meta["name"]}" skipped. Directory not found')

    def _delete_album_item_file(self, album_meta: dict, media_item_meta: dict) -> None:
        if not album_meta or not media_item_meta:
            return
        
        dest_file = os.path.join(self._dest_path, album_meta['path'], album_meta['cname'], media_item_meta['cname'])

        self._logger.debug(f'Deleting album item "{media_item_meta["name"]}"')

        if os.path.isfile(dest_file):
            os.remove(dest_file)
        else:
            self._logger.debug(f'Deletion for album item "{media_item_meta["name"]}" skipped. File not found')
    
    def _index_needed(self, album_meta: dict, album: dict) -> bool:
        if not album_meta:
            return True
        
        album_items_cnt = self._model.get_albums_items_meta_cnt(album_id=album_meta['album_id'])
        
        synced = album_meta['status'] in ('synced', 'pending_sync')
        same_size = int(album_meta['size']) == int(album['mediaItemsCount']) == album_items_cnt
        same_name = album_meta['name'] == album['title']

        if not synced or not same_size or not same_name:
            return True
        
        # TODO: check for mdate changes when it will be available in API
        
        return False

    def _index_album(self, album: dict) -> int:
        path = 'albums'
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
            status='pending_sync',
        )
    
    def _index_album_items(self, album_meta: dict) -> None:
        album_remote_id = album_meta['remote_id']
        page_token = None
        limit = self._album_items_list_limit

        self._model.set_album_items_meta_stale(album_meta['album_id'])

        while True:
            to_index = self._google_api.media_items_search(album_id=album_remote_id, page_token=page_token, page_size=limit)

            # if no items to index, break
            if not to_index:
                break

            media_items = to_index.get('mediaItems', [])
            page_token = to_index.get('nextPageToken')

            for media_item in media_items:
                self._index_album_item(album_meta, media_item)

            self._logger.info(f'Album items batch index: indexed {len(to_index.get("mediaItems", []))}')

            # commit batch
            self._model.commit()

            if not page_token:
                break

    def _index_album_item(self, album_meta: dict, media_item: dict) -> int:
        # make sure media item is indexed
        self._media_items.ensure_item_indexed(media_item)

        media_item_meta = self._media_items.get_item_meta(remote_id=media_item['id'])

        self._logger.debug(f'Indexing album item "{media_item["filename"]}"')

        return self._model.add_album_item_meta(
            album_id=album_meta['album_id'],
            media_item_id=media_item_meta['media_id'],
            status='pending_sync',
        )

    async def _sync_albums(self, **opts) -> dict:
        limit = 100
        offset = 0

        info = {
            'synced': 0,
            'failed': 0,
        }

        total = self._model.get_albums_meta_cnt(status=['pending_sync', 'sync_error'])

        if not total:
            return info

        while True:
            to_sync = self._model.search_albums_meta(limit=limit, offset=offset, status=['pending_sync', 'sync_error'])

            # if no albums to sync, break
            if not to_sync:
                break

            for album_meta in to_sync:
                try:
                    stats = await self._sync_album(album_meta, **opts)

                    if stats['failed']:
                        raise Exception(f'{stats["failed"]} album items failed to sync')
                except Exception as e:
                    self._logger.error(f'Sync for album "{album_meta["name"]}" failed. {e}')
                    self._model.update_album_meta(album_meta['album_id'], status='sync_error')

                    offset += 1
                    info['failed'] += 1
                else:
                    self._model.update_album_meta(album_meta['album_id'], status='synced')

                    info['synced'] += 1

            # commit batch
            self._model.commit()

        return info

    async def _sync_album(self, album_meta: dict, *, concurrency: int = 1, use_symlinks: bool = True) -> dict:
        album_id = album_meta['album_id']
        limit = 100
        offset = 0
        opts = {
            'use_symlinks': use_symlinks,
        }

        info = {
            'synced': 0,
            'skipped': 0,
            'failed': 0,
        }

        self._logger.info(f'Syncing album "{album_meta["name"]}" with {album_meta["size"]} items')

        total = self._model.get_albums_items_meta_cnt(album_id=album_id, status=['pending_sync', 'sync_error'])

        if not total:
            return info

        processed = 0
        t_start = datetime.now()

        while True:
            to_sync = self._model.search_albums_items_meta(limit=limit, offset=offset, album_id=album_id, status=['pending_sync', 'sync_error'])

            # if no items to sync, break
            if not to_sync:
                break

            # break items into chunks of concurrency length
            chunks_to_sync = [to_sync[i:i + concurrency] for i in range(0, len(to_sync), concurrency)]

            # sync album items in chunks concurrently
            for chunk in chunks_to_sync:
                chunk_info = await self._sync_album_concurrently(album_meta, chunk, **opts)

                offset += chunk_info['failed']
                info['synced'] += chunk_info['synced']
                info['skipped'] += chunk_info['skipped']
                info['failed'] += chunk_info['failed']

            # commit batch
            self._model.commit()

            t_end = datetime.now()
            processed += len(to_sync)
            
            (percentage, eta) = gen_batch_stats(t_start, t_end, processed, total)

            self._logger.info(f'Album items batch sync: {percentage}, eta: {eta}')

        return info
    
    async def _sync_album_concurrently(self, album_meta: dict, to_sync: list, **opts) -> dict:
        tasks = []
        chunk_info = {
            'synced': 0,
            'skipped': 0,
            'failed': 0,
        }

        for album_item in to_sync:
            media_item_meta = self._media_items.get_item_meta(media_id=album_item['media_id'])
            # sync album item
            tasks.append(asyncio.create_task(self._sync_album_item(album_meta, media_item_meta, **opts), name=album_item['media_id']))

        await asyncio.gather(*tasks, return_exceptions=True)

        # update items status based on task results
        for t in tasks:
            if t.exception():
                self._logger.error(f'Sync for album item #{t.get_name()} failed. Reason {t.exception()}')
                self._model.update_album_item_meta(album_meta['album_id'], t.get_name(), status='sync_error')

                chunk_info['failed'] += 1
            else:
                status = t.result()

                self._model.update_album_item_meta(album_meta['album_id'], t.get_name(), status=status)

                if status == 'synced':
                    chunk_info['synced'] += 1
                else:
                    chunk_info['skipped'] += 1

        return chunk_info

    async def _sync_album_item(self, album_meta: dict, media_item_meta: dict, *, use_symlinks: bool = True) -> str:
        relative_src_path = media_item_meta.get('path')
        relative_dest_path = album_meta.get('path')
        album_name = album_meta.get('cname')
        item_name = media_item_meta.get('cname')

        if not relative_src_path:
            raise ValueError('missing source path')
        
        if not relative_dest_path:
            raise ValueError('missing destination path')
        
        if not album_name:
            raise ValueError('missing album name')
        
        if not item_name:
            raise ValueError('missing item name')
        
        if media_item_meta['status'] not in ['synced', 'ignored']:
            raise ValueError('media item is not synced')

        if media_item_meta['status'] == 'ignored':
            self._logger.debug(f'Sync for album item "{media_item_meta["name"]}" skipped. File is ignored')
            return 'ignored'
        
        src_file = os.path.join(self._media_items.dest_path, relative_src_path, item_name)
        dest_path = os.path.join(self._dest_path, relative_dest_path, album_name)
        dest_file = os.path.join(dest_path, item_name)

        if not os.path.isfile(src_file):
            raise ValueError(f'missing source file')

        # if file already exists, skip
        if os.path.isfile(dest_file):
            self._logger.debug(f'Sync for album item "{media_item_meta["name"]}" skipped. Item already exists')
            return 'synced'

        self._logger.debug(f'Linking album item "{media_item_meta["name"]}"')

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
    
    def _delete_stale_albums(self) -> None:
        limit = 100

        while True:
            to_delete = self._model.search_albums_meta(limit=limit, status='stale')

            # if no items to delete, break
            if not to_delete:
                break

            for album_meta in to_delete:
                self._delete_album_dir(album_meta)
                self._model.delete_album_meta(album_meta['album_id'])

            # commit batch
            self._model.commit()

    def _delete_stale_albums_items(self) -> None:
        limit = 100

        total = self._model.get_albums_items_meta_cnt(status='stale')

        if not total:
            return

        processed = 0
        t_start = datetime.now()

        while True:
            to_delete = self._model.search_albums_items_meta(limit=limit, status='stale')

            # if no items to delete, break
            if not to_delete:
                break

            for album_item in to_delete:
                album_meta = self.get_album_meta(album_id=album_item['album_id'])
                media_item_meta = self._media_items.get_item_meta(media_id=album_item['media_id'])

                self._delete_album_item_file(album_meta, media_item_meta)
                self._model.delete_album_item_meta(album_item['album_id'], album_item['media_id'])

            # commit batch
            self._model.commit()
            t_end = datetime.now()

            processed += len(to_delete)
            
            (percentage, eta) = gen_batch_stats(t_start, t_end, processed, total)

            self._logger.info(f'Albums items batch delete: {percentage}, eta: {eta}')