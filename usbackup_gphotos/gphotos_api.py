import time
import requests
import logging
from datetime import datetime
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from usbackup_gphotos.gauth import GAuth

__all__ = ['GPhotosApi', 'GPhotosApiException']

class GPhotosApiException(Exception):
    pass

class GPhotosApi:
    def __init__(self, *, gauth: GAuth, logger: logging.Logger) -> None:
        self._gauth: GAuth = gauth
        self._logger: logging.Logger = logger.getChild('gphotos_api')

        self._api_url: str = 'https://photoslibrary.googleapis.com/v1'

        self._session: requests.Session = requests.Session()

        # https://cloud.google.com/apis/design/errors
        retries = Retry(
            total=5,
            backoff_factor=3,
            status_forcelist=[409, 429, 499, 500, 502, 503, 504],
            respect_retry_after_header=True,
            raise_on_status=False,
        )

        self._session.mount("https://", HTTPAdapter(max_retries=retries))

    @staticmethod
    def format_date(date: str) -> str:
        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            raise GPhotosApiException('Invalid date format. Must be YYYY-MM-DD') from None
        
        date_parts = date.split('-')

        return {
            'year': int(date_parts[0]),
            'month': int(date_parts[1]),
            'day': int(date_parts[2]),
        }

    def media_items_list(self, *, page_size: int = 100, page_token: str = None) -> dict:
        params = {
            'pageSize': page_size,
        }

        if page_token:
            params['pageToken'] = page_token

        resp = self._call_api('mediaItems', 'get', get_params=params)

        if not resp:
            return {}

        if not resp.get('mediaItems'):
            raise GPhotosApiException('"mediaItems" response doesn\'t contain any mediaItems')

        return resp
    
    def media_items_search(self, *, album_id: str = None, page_size: int = None, page_token: str = None, filters: dict = None, order_by: str = None) -> dict:
        params = {}

        if album_id:
            params['albumId'] = album_id

        if page_size:
            params['pageSize'] = page_size

        if page_token:
            params['pageToken'] = page_token

        if filters:
            params['filters'] = filters

        if order_by:
            params['orderBy'] = order_by

        resp = self._call_api('mediaItems:search', 'post', post_params=params)

        if not resp:
            return {}

        if not resp.get('mediaItems'):
            raise GPhotosApiException('"mediaItems:search" response doesn\'t contain any mediaItems')

        return resp
    
    def media_item_get(self, media_id: str) -> dict:
        if not media_id:
            raise ValueError('media_id not provided')
        
        resp = self._call_api(f'mediaItems/{media_id}', 'get')

        if not resp:
            return {}

        return resp
    
    def media_items_batch_get(self, media_ids: list) -> dict:
        if not media_ids:
            raise ValueError('media_ids not provided')
        
        params = {
            'mediaItemIds': media_ids
        }

        resp = self._call_api('mediaItems:batchGet', 'get', get_params=params)

        if not resp:
            return {}

        if not resp.get('mediaItemResults'):
            raise GPhotosApiException('"mediaItems:batchGet" response doesn\'t contain any mediaItemResults')

        return resp['mediaItemResults']
    
    def albums_list(self, *, page_size: int = 100, page_token: str = None) -> dict:
        params = {
            'pageSize': page_size,
        }

        if page_token:
            params['pageToken'] = page_token

        resp = self._call_api('albums', 'get', get_params=params)

        if not resp:
            return {}

        if not resp.get('albums'):
            raise GPhotosApiException('"albums" response doesn\'t contain any albums')

        return resp
    
    def _call_api(self, endpoint: str, method: str, *, get_params: dict = None, post_params: dict = None, retry: int = 1) -> dict:
        # if endpoint starts with http, assume it's a full URL
        if endpoint.startswith('http'):
            url = endpoint
        else:
            url = self._api_url + '/' + endpoint

        headers = {}

        if not self._gauth.access_token:
            raise GPhotosApiException('Invalid access token')

        headers['Authorization'] = 'Bearer ' + self._gauth.access_token

        if method == 'post':
            headers['Content-Type'] = 'application/json'

        try:
            resp = requests.request(method, url, headers=headers, params=get_params, json=post_params, timeout=(5, 30))
        except Exception as e:
            raise GPhotosApiException(f'API call failed: {e}') from None
        
        # refresh token and retry
        if resp.status_code == 401:
            if retry < 3:
                self._gauth.refresh_token()

                self._logger.debug(f'Refreshed access token and retrying API call (retry={retry+1})')

                return self._call_api(endpoint, method, get_params=get_params, post_params=post_params, retry=retry+1)
            else:
                raise GPhotosApiException(f'API call failed: {resp.text}. Max retries reached')
        
        resp_data = resp.json()
        
        if resp.status_code == 200:
            return resp_data
        else:
            if resp_data.get('error'):
                raise GPhotosApiException(f'API call failed: {resp_data["error"]["message"]}')
            else:
                raise GPhotosApiException(f'API call failed: {resp.text}')