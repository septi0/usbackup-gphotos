import os
import logging
import asyncio
import requests
import tempfile
import shutil
from datetime import datetime
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from usbackup_gphotos.media_items_model import MediaItemsModel
from usbackup_gphotos.gphotos_api import GPhotosApi
from usbackup_gphotos.action_stats import ActionStats
from usbackup_gphotos.utils import transform_fs_safe, gen_batch_stats

__all__ = ['MediaItems', 'MediaItemDownloadError']

class MediaItemDownloadError(Exception):
    pass

class MediaItems:
    def __init__(self, dest_path: str, *, model: MediaItemsModel, google_api: GPhotosApi, logger: logging.Logger) -> None:
        self._dest_path: str = dest_path
        self._model: MediaItemsModel = model

        self._google_api: GPhotosApi = google_api
        self._logger: logging.Logger = logging.LoggerAdapter(logger.getChild('media_items'), {})

        self._media_items_list_limit: int = 100
        self._media_items_batch_limit: int = 50

        self._dl_session: requests.Session = None

        self._items_dir = 'items'

    @property
    def dest_path(self) -> str:
        return self._dest_path

    def index_items(self, *, last_index: str = None, rescan: bool = False) -> ActionStats:
        from_date = None
        page_token = None
        limit = self._media_items_list_limit
        filters = {}
        check_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        info = ActionStats(indexed=0, skipped=0, failed=0)

        filters['mediaTypeFilter'] = {
            'mediaTypes': ['ALL_MEDIA'],
        }

        if not rescan and last_index:
            from_date = datetime.strptime(last_index, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')

        if from_date:
            to_date = '9999-12-31'

            filters['dateFilter'] = {
                'ranges': [
                    {
                        'startDate': GPhotosApi.format_date(from_date),
                        'endDate': GPhotosApi.format_date(to_date),
                    }
                ],
            }

            self._logger.info(f'Searching media items starting from {from_date}')

        while True:
            to_index = self._google_api.media_items_search(page_token=page_token, page_size=limit, filters=filters)

            if not to_index:
                break

            media_items = to_index.get('mediaItems', [])
            page_token = to_index.get('nextPageToken')
            batch_indexed = 0

            for media_item in media_items:
                try:
                    status = self.index_item(media_item, commit=False)
                except Exception as e:
                    self._logger.error(f'Index for media item "{media_item["filename"]}" failed. {e}')
                    info.increment(failed=1)
                else:
                    if status == 'indexed':
                        info.increment(indexed=1)
                        batch_indexed += 1
                    else:
                        info.increment(skipped=1)

            self._model.commit()

            if batch_indexed:
                self._logger.info(f'Media items batch index: indexed {batch_indexed}')

            if not page_token:
                break

        if rescan:
            # mark all items older than check_date date as stale
            stale_cnt = self._model.set_media_items_stale(last_checked=check_date)

            if stale_cnt:
                self._logger.info(f'Marked {stale_cnt} media items as stale')

        return info
    
    def index_item(self, media_item: dict, *, commit=True) -> str:
        media_item_meta = self.get_item_meta(remote_id=media_item['id'])

        if not self._index_needed(media_item_meta, media_item):
            last_checked = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self._model.update_media_item_meta(media_item_meta['media_id'], last_checked=last_checked)

            self._logger.debug(f'Index for media item "{media_item_meta["name"]}" skipped. Index not needed')

            return 'skipped'
        
        self._add_item(media_item)

        if commit:
            self._model.commit()

        return 'indexed'

    def sync_items(self, *, concurrency: int = 1) -> ActionStats:
        self._dl_session = requests.Session()

        # https://cloud.google.com/apis/design/errors
        retries = Retry(
            total=5,
            backoff_factor=3,
            status_forcelist=[409, 429, 499, 500, 502, 503, 504],
            respect_retry_after_header=True,
            raise_on_status=False,
        )

        self._dl_session.mount("https://", HTTPAdapter(max_retries=retries, pool_maxsize=concurrency))
    
        return asyncio.run(self._sync_items(concurrency=concurrency))

    def delete_obsolete_items(self) -> ActionStats:
        info = ActionStats(deleted=0, failed=0)

        db_info = self._delete_obsolete_items_by_db()
        info.increment(**dict(db_info))

        fs_info = self._delete_obsolete_items_by_fs()
        info.increment(**dict(fs_info))

        return info

    def get_item_meta(self, *, media_id: int = None, remote_id: str = None) -> dict:
        return self._model.get_media_item_meta(media_id=media_id, remote_id=remote_id)
    
    def ignore_items(self, media_items: list) -> ActionStats:
        info = ActionStats(ignored=0, failed=0)

        for media_item in media_items:
            try:
                ignored = self._model.update_media_item_meta(media_item, status='ignored')
                if not ignored:
                    raise ValueError(f'Media item "{media_item}" not found')
            except Exception as e:
                info.increment(failed=1)
            else:
                info.increment(ignored=1)

        self._model.commit()

        return info

    def reset_ignored_items(self) -> ActionStats:
        info = ActionStats(reset=0)

        reset = self._model.reset_ignored_media_items()
        info.increment(reset=reset)

        return info 

    def stats(self) -> dict:
        return self._model.get_media_items_meta_stats()

    def scan_synced_items_fs(self) -> ActionStats:
        limit = 100
        offset = 0
        total = self._model.get_media_items_meta_cnt(status='synced')
        info = ActionStats(fixed=0)

        if not total:
            return info
        
        while True:
            to_check = self._model.search_media_items_meta(limit=limit, offset=offset, status='synced')

            if not to_check:
                break

            for media_item_meta in to_check:
                if not self._item_exists_fs(media_item_meta):
                    self._logger.debug(f'Media item "{media_item_meta["name"]}" not found on filesystem. Setting status to pending_sync')
                    self._model.update_media_item_meta(media_item_meta['media_id'], status='pending_sync')

                    info.increment(fixed=1)

            self._model.commit()

            offset += limit

        return info

    def _get_canonicalized_name(self, file_name: str, path: str) -> str:
        unique = 1

        # split file name and extension
        (name, ext) = os.path.splitext(file_name)

        name = transform_fs_safe(name)

        file_name = f'{name}{ext}'

        while True:
            if not self._model.search_media_items_meta(cname=file_name, path=path):
                return file_name
            
            name, ext = os.path.splitext(file_name)

            file_name = f'{name} ({unique}){ext}'

            unique += 1

    async def _get_items_to_sync(self, *, limit: int = 100, offset: int = 0) -> list:
        media_items_meta = self._model.search_media_items_meta(limit=limit, offset=offset, status=['pending_sync', 'sync_error'])

        if not media_items_meta:
            return []

        keys = [media_item['remote_id'] for media_item in media_items_meta]

        media_items = await asyncio.to_thread(self._google_api.media_items_batch_get, keys)

        if not media_items:
            return []
        
        to_sync = []

        for media_item_meta, media_item in zip(media_items_meta, media_items):
            if media_item.get('mediaItem'):
                media_item = media_item['mediaItem']
            else:
                media_item = {
                    'error': media_item['status']['message'],
                }

            to_sync.append((media_item_meta, media_item))
        
        return to_sync

    def _delete_item_file(self, media_item_meta: dict) -> None:
        dest_file = os.path.join(self._dest_path, media_item_meta['path'], media_item_meta['cname'])

        self._logger.debug(f'Deleting media item "{media_item_meta["name"]}"')

        if os.path.isfile(dest_file):
            os.remove(dest_file)
        else:
            self._logger.debug(f'Deletion for media item "{media_item_meta["name"]}" skipped. File not found')

    def _gen_path_by_cdate(self, date: str) -> str:
        date = datetime.strptime(date, '%Y-%m-%dT%H:%M:%SZ')

        year = date.strftime('%Y')
        month = date.strftime('%m')

        return os.path.join(self._items_dir, year, month)
    
    def _download_item(self, url: str, dest_file: str) -> None:
        try:
            resp = self._dl_session.get(url, stream=True, timeout=(5, 30))
        except Exception as e:
            raise MediaItemDownloadError(f'Failed to download media item. Reason: {e}') from None
        
        resp.raise_for_status()

        length = int(resp.headers.get('content-length', 0))
        downloaded = 0

        with open(dest_file, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=1024):
                f.write(chunk)
                downloaded += len(chunk)

        if length != downloaded:
            os.remove(dest_file)
            raise MediaItemDownloadError(f'Downloaded size {downloaded} does not match content-length {length}')

    def _item_exists_fs(self, media_item_meta: dict) -> bool:
        dest_file = os.path.join(self._dest_path, media_item_meta['path'], media_item_meta['cname'])

        return os.path.isfile(dest_file)
    
    def _index_needed(self, media_item_meta: dict, media_item: dict) -> bool:
        if not media_item_meta:
            return True

        if media_item_meta['status'] not in ['synced', 'pending_sync', 'ignored']:
            return True

        # TODO: check mdate (if it will be available in API)

        return False
    
    def _add_item(self, media_item: dict) -> int:
        path = self._gen_path_by_cdate(media_item['mediaMetadata']['creationTime'])
        cname = self._get_canonicalized_name(media_item['filename'], path)
        create_date = datetime.strptime(media_item['mediaMetadata']['creationTime'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
        index_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        self._logger.debug(f'Indexing media item "{media_item["filename"]}"')

        return self._model.add_media_item_meta(
            remote_id=media_item['id'],
            name=media_item['filename'],
            cname=cname,
            mime_type=media_item['mimeType'],
            create_date=create_date,
            modify_date=create_date, # TODO: set mdate (if it will be available in API)
            path=path,
            index_date=index_date,
            last_checked=index_date,
            status='pending_sync',
        )

    async def _sync_items(self, *, concurrency: int = 1) -> ActionStats:
        limit = self._media_items_batch_limit
        offset = 0
        total = self._model.get_media_items_meta_cnt(status=['pending_sync', 'sync_error'])
        info = ActionStats(synced=0, skipped=0, failed=0)

        if not total:
            return info

        processed = 0
        t_start = datetime.now()

        while True:
            to_sync = await self._get_items_to_sync(limit=limit, offset=offset)

            if not to_sync:
                break

            # break to_sync into chunks of concurrency length
            chunks_to_sync = [to_sync[i:i + concurrency] for i in range(0, len(to_sync), concurrency)]

            # sync items concurrently
            for chunk in chunks_to_sync:
                c_info = await self._sync_items_concurrently(chunk)

                offset += c_info['failed']
                info.increment(**dict(c_info))

            self._model.commit()

            t_end = datetime.now()
            processed += len(to_sync)

            (percentage, eta) = gen_batch_stats(t_start, t_end, processed, total)

            self._logger.info(f'Media items batch sync: {percentage}, eta: {eta}')

        return info
    
    async def _sync_items_concurrently(self, to_sync: list) -> ActionStats:
        tasks = []
        info = ActionStats(synced=0, skipped=0, failed=0)

        for (media_item_meta, media_item) in to_sync:
            # sync media item
            tasks.append(asyncio.create_task(self._sync_item(media_item_meta, media_item), name=media_item_meta['media_id']))

        await asyncio.gather(*tasks, return_exceptions=True)

        # update items status based on async tasks results
        for t in tasks:
            if t.exception():
                self._logger.error(f'Sync for media item #{t.get_name()} failed')
                self._logger.debug(t.exception(), exc_info=True)
                self._model.update_media_item_meta(t.get_name(), status='sync_error')

                info.increment(failed=1)
            else:
                status = t.result()
                status_upd = 'synced' if status in ['synced', 'skipped'] else status

                self._model.update_media_item_meta(t.get_name(), status=status_upd)

                if status == 'synced':
                    info.increment(synced=1)
                else:
                    info.increment(skipped=1)

        return info

    async def _sync_item(self, media_item_meta: dict, media_item: dict) -> str:
        if media_item.get('error'):
            raise ValueError(media_item["error"])
        
        download_url = media_item.get('baseUrl')
        media_type = media_item_meta.get('mime_type').split('/')[0]

        if not download_url:
            raise ValueError(f'Missing download_url')
        
        # if media item has status ignored, stop here
        if media_item_meta['status'] == 'ignored':
            self._logger.debug(f'Sync for media item "{media_item_meta["name"]}" skipped. Item ignored')
            return 'ignored'
        
        # for videos, download only if status is READY
        if media_type == 'video' and media_item['mediaMetadata']['video'].get('status') != 'READY':
            raise ValueError(f'Video "{media_item_meta["name"]}" is not ready ({media_item["mediaMetadata"]["video"].get("status")})')

        dest_path = os.path.join(self._dest_path, media_item_meta['path'])
        dest_file = os.path.join(dest_path, media_item_meta['cname'])

        create_date_ts = datetime.strptime(media_item_meta['create_date'], '%Y-%m-%d %H:%M:%S').timestamp()
        modify_date_ts = datetime.strptime(media_item_meta['modify_date'], '%Y-%m-%d %H:%M:%S').timestamp()

        # if file already exists, remove it if mtime is different
        if os.path.isfile(dest_file):
            file_stat = os.stat(dest_file)

            if file_stat.st_mtime != modify_date_ts:
                os.remove(dest_file)
            else:
                self._logger.debug(f'Sync for media item "{media_item_meta["name"]}" skipped. File already exists')
                return 'skipped'
        
        # add download type so we can download original file
        if media_type == 'video':
            download_url += '=dv'
        else:
            download_url += '=d'
        
        self._logger.debug(f'Downloading media item "{media_item_meta["name"]}"')

        # create tmp file name
        # we use a tmp file so we can move it to dest file after download is complete to avoid partial/incomplete files
        tmp_file = tempfile.NamedTemporaryFile(delete=False).name

        # download file
        await asyncio.to_thread(self._download_item, download_url, tmp_file)

        if not os.path.isdir(dest_path):
            os.makedirs(dest_path)

        # move tmp file to dest file
        # Note: don't use os.rename() as it will fail if directory is on a different filesystem
        shutil.move(tmp_file, dest_file)

        # set file create / modify time
        os.utime(dest_file, (create_date_ts, modify_date_ts))

        # set permissions
        os.chmod(dest_file, 0o644)

        return 'synced'

    def _delete_obsolete_items_by_db(self) -> ActionStats:
        limit = 100
        offset = 0
        total = self._model.get_media_items_meta_cnt(status='stale')
        info = ActionStats(deleted=0, failed=0)

        if not total:
            return info

        while True:
            to_delete = self._model.search_media_items_meta(limit=limit, offset=offset, status='stale')

            if not to_delete:
                break

            for media_item_meta in to_delete:
                try:
                    self._delete_item_file(media_item_meta)
                    self._model.delete_media_item_meta(media_item_meta['media_id'])
                except Exception as e:
                    self._logger.error(f'Deletion for media item "{media_item_meta["name"]}" failed. {e}')

                    offset += 1
                    info.increment(failed=1)
                else:
                    info.increment(deleted=1)

            self._model.commit()

        return info
    
    def _delete_obsolete_items_by_fs(self) -> ActionStats:
        items_path = os.path.join(self._dest_path, self._items_dir)
        info = ActionStats(deleted=0, failed=0)

        for root, dirs, files in os.walk(items_path):
            if not files:
                continue

            for file in files:
                relative_path = os.path.relpath(root, self._dest_path)
                media_item_meta = self._model.search_media_items_meta(cname=file, path=relative_path)

                if not media_item_meta:
                    self._logger.debug(f'Media item "{file}" not found in database. Deleting')

                    try:
                        os.remove(os.path.join(root, file))
                    except Exception as e:
                        self._logger.error(f'Deletion for media item "{file}" failed. {e}')
                        info.increment(failed=1)
                    else:
                        info.increment(deleted=1)

        return info