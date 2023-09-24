import os
import logging
import json
import hashlib
import time
import requests
import base64
from urllib.parse import urlparse, parse_qs
from http.server import BaseHTTPRequestHandler, HTTPServer

__all__ = ['GAuth', 'GAuthError', 'GAuthValueError']

class GAuthError(Exception):
    pass

class GAuthValueError(Exception):
    pass

class GAuth:
    def __init__(self, credentials_file: str, token_hash: str, scopes: list, *, logger: logging.Logger) -> None:
        self._logger: logging.Logger = logger.getChild('gauth')

        self._credentials: dict = self._parse_credentials(credentials_file)
        self._token: dict = self._parse_token(token_hash)

        self._scopes = scopes

        self._client_id: str = self._credentials.get('client_id', '')
        self._client_id_hash: str = hashlib.md5(self._client_id.encode()).hexdigest()
        self._client_secret: str = self._credentials.get('client_secret', '')

        self._auth_callback: callable = None
        self._use_webserver: bool = False
        self._listen_port: int = 8080

    @property
    def access_token(self) -> str:
        return self._token.get('access_token', '')
    
    def set_auth_callback(self, callback: callable) -> None:
        self._auth_callback = callback
    
    def set_webserver(self, *, port: int = 8080) -> None:
        self._use_webserver = True
        self._listen_port = port
    
    def ensure_valid_auth(self) -> None:
        if not self._token_exists():
            raise GAuthError('No access token found. Please use the "auth" command to authenticate first')
        
        if self._token_expired():
            self._refresh_existing_token()

    def refresh_token(self) -> None:
        self._logger.info(f'Access token expired, refreshing')
        self._refresh_existing_token()

    def issue_new_token(self) -> None:
        self._logger.info(f'Issuing new access token')
        self._issue_new_token()

    def _parse_credentials(self, credentials_file: str) -> dict:
        if not credentials_file:
            raise GAuthValueError('Credentials file not provided')

        if not os.path.isfile(credentials_file):
            raise GAuthValueError(f'Credentials file {credentials_file} not found')

        # read file and json decode
        with open(credentials_file, 'r') as f:
            credentials = json.load(f)

        return credentials.get('installed', {})
    
    def _parse_token(self, token_hash: str) -> dict:
        if not token_hash:
            return {}

        return json.loads(base64.b64decode(token_hash.encode()).decode())
    
    def _encrypt_token(self) -> str:
        return base64.b64encode(json.dumps(self._token).encode()).decode()
    
    def _token_exists(self) -> bool:
        return True if self._token.get('access_token') else False
    
    def _token_expired(self) -> bool:
        if not self._token.get('access_token'):
            raise GAuthError('Access token not found')

        if self._token.get('expires_at') < time.time():
            return True

        return False
    
    def _refresh_existing_token(self) -> None:
        if not self._token.get('refresh_token'):
            raise GAuthError('Refresh token not found')
        
        token = self._gen_oauth2_token('refresh_token', refresh_token=self._token.get('refresh_token'))

        self._update_token({
            'access_token': token['access_token'],
            'expires_at': token['expires_at'],
        })

        if token:
            self._logger.info(f'Access token refreshed')
        else:
            raise GAuthError('Could not refresh token. Please use the "auth" command to authenticate again')

    def _issue_new_token(self) -> None:
        url = self._gen_auth_url()

        print(f'Please access the following url from any web browser, to get an access token: {url}')

        if self._use_webserver:
            code = self._get_auth_code_from_webserver()
        else:
            code = self._get_auth_code_from_input()

        token = self._gen_oauth2_token('authorization_code', code=code)

        if token:
            self._update_token(token, replace=True)
            self._logger.info(f'Access token issued')
        else:
            self._update_token({}, replace=True)
            raise GAuthError('Could not issue token. Please use the "auth" command to authenticate again')

    def _gen_auth_url(self) -> str:
        params = {
            'client_id': self._credentials['client_id'],
            'redirect_uri': self._credentials['redirect_uris'][0] + ':' + str(self._listen_port),
            'response_type': 'code',
            'scope': '+'.join(self._scopes),
            'access_type': 'offline',
        }

        url = self._credentials['auth_uri'] + '?' + '&'.join([f'{k}={v}' for k, v in params.items()])

        return url
    
    def _get_auth_code_from_webserver(self) -> str:
        class AuthHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                code = parse_qs(urlparse(self.path).query).get('code')[0]

                if code:
                    self.send_response(200)
                    self.send_header('Content-type', 'text/html')
                    self.end_headers()

                    self.wfile.write(b'<html><head><title>Authentication successful</title></head>')
                    self.wfile.write(b'<body><p>Authentication successful, you can close this window now.</p></body></html>')

                    self.server.code = code
                else:
                    self.send_response(400)
                    self.send_header('Content-type', 'text/html')
                    self.end_headers()

                    self.wfile.write(b'<html><head><title>Authentication failed</title></head>')
                    self.wfile.write(b'<body><p>Authentication failed, please try again.</p></body></html>')

        server = HTTPServer(('localhost', self._listen_port), AuthHandler)

        # disable logging
        server.log_message = lambda format, *args: None

        server.code = ''

        while not server.code:
            server.handle_request()

        return server.code

    def _get_auth_code_from_input(self) -> str:
        print(f'After you have followed the instructions, please enter the url you were redirected to: ', end='')

        auth_url = input()

        if not auth_url:
            raise GAuthValueError('Invalid url')

        parsed_url = urlparse(auth_url)

        if not parsed_url.query:
            raise GAuthValueError('Invalid url')

        query_params = parse_qs(parsed_url.query)

        if not query_params.get('code'):
            raise GAuthValueError('Invalid url')

        code = query_params.get('code')[0]

        return code
    
    def _update_token(self, data: dict, *, replace: bool = False) -> None:
        allowed_keys = ['access_token', 'refresh_token', 'scope', 'token_type', 'expires_at']

        # filter allowed keys
        data = {k: v for k, v in data.items() if k in allowed_keys}

        if replace:
            self._token = data
        else:
            for (tkey, tval) in data.items():
                self._token[tkey] = tval

        if self._auth_callback:
            self._auth_callback(self._encrypt_token())
    
    def _gen_oauth2_token(self, grant_type: str, *, code: str = None, refresh_token: str = None) -> dict:
        post_params = {
            'client_id': self._credentials['client_id'],
            'client_secret': self._credentials['client_secret']
        }

        if ('authorization_code' == grant_type):
            if not code:
                raise GAuthValueError('Code not provided')

            post_params['grant_type'] = grant_type
            post_params['code'] = code
            post_params['redirect_uri'] = self._credentials['redirect_uris'][0] + ':' + str(self._listen_port)
        elif('refresh_token' == grant_type):
            if not refresh_token:
                raise GAuthValueError('Refresh token not provided')
            
            post_params['grant_type'] = grant_type
            post_params['refresh_token'] = refresh_token

        response = requests.post(self._credentials['token_uri'], data=post_params)

        self._logger.debug(f'OAuth2 token response: {response.text}')

        if response.status_code == 400:
            error = response.json().get('error', '')

            # return empty token if refresh token is invalid
            if 'invalid_grant' == error:
                self._logger.warning('Invalid refresh token')
                return {}

        if response.status_code != 200:
            raise GAuthError('Invalid response: ' + response.text)
        
        token = response.json()

        if 'authorization_code' == grant_type:
            required_keys = ['access_token', 'refresh_token', 'expires_in']
        elif 'refresh_token' == grant_type:
            required_keys = ['access_token', 'expires_in']

        for key in required_keys:
            if not token.get(key):
                raise GAuthError(f'Invalid token, missing {key}')

        token['expires_at'] = time.time() + token['expires_in']

        del token['expires_in']
        
        return token