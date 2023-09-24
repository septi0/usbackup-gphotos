import os
import logging
from configparser import ConfigParser
from usbackup_gphotos.identity import UsBackupGPhotosIdentity

__all__ = ['UsBackupGPhotosManager', 'UsBackupGPhotosConfigError']

class UsBackupGPhotosConfigError(Exception):
    pass

class UsBackupGPhotosManager:
    def __init__(self, params: dict) -> None:
        self._logger: logging.Logger = self._gen_logger(params.get('log_file', ''), params.get('log_level', 'INFO'))

        config = self._parse_config(params.get('config_files', []))
        config['GLOBALS'] = {
            'webserver': params.get('use_webserver', False),
            'webserver_port': 8080,
        }

        self._identities: list[UsBackupGPhotosIdentity] = self._gen_identities(params.get('identities', []), config)

    def index(self, options: dict) -> None:
        for identity in self._identities:
            try:
                identity.lock()
                identity.index(options)
                identity.unlock()
            except Exception as e:
                identity.unlock()
                self._logger.exception(f'Index action for identity "{identity.name}" failed. Reason: {e}', exc_info=True)
            except KeyboardInterrupt:
                identity.unlock()
                self._logger.info(f'Index action for identity "{identity.name}" interrupted by user')
                break

    def sync(self, options: dict) -> None:
        for identity in self._identities:
            try:
                identity.lock()
                identity.sync(options)
                identity.unlock()
            except Exception as e:
                identity.unlock()
                self._logger.exception(f'Sync action for identity "{identity.name}" failed. Reason: {e}', exc_info=True)
            except KeyboardInterrupt:
                identity.unlock()
                self._logger.info(f'Sync action for identity "{identity.name}" interrupted by user')
                break

    def delete_obsolete(self) -> None:
        for identity in self._identities:
            try:
                identity.delete_obsolete()
            except Exception as e:
                self._logger.exception(f'Delete obsolete action for identity "{identity.name}" failed. Reason: {e}', exc_info=True)
            except KeyboardInterrupt:
                self._logger.info(
                    f'Delete obsolete action for identity "{identity.name}" interrupted by user')
                break

    def auth(self) -> None:
        for identity in self._identities:
            try:
                identity.auth()
            except Exception as e:
                self._logger.exception(f'Authenticate action for identity "{identity.name}" failed. Reason: {e}', exc_info=True)
            except KeyboardInterrupt:
                self._logger.info(f'Authenticate action for identity "{identity.name}" interrupted by user')
                break

    def stats(self) -> None:
        for identity in self._identities:
            stats = identity.stats()

            print(f'Identity: {identity.name}')
            print(f'  Media items last index: {stats["media_items_last_index"]}')
            print(f'  Albums last index: {stats["albums_last_index"]}')
            print(f'  Media items: {", ".join([f"{value} {key}" for key, value in stats["media_items"].items()])}')
            print(f'  Albums: {", ".join([f"{value} {key}" for key, value in stats["albums"].items()])}')
            print(f'  Albums items: {", ".join([f"{value} {key}" for key, value in stats["albums_items"].items()])}')
            print()

    def ignore(self, options: dict) -> None:
        for identity in self._identities:
            try:
                identity.ignore(options)
            except Exception as e:
                self._logger.exception(f'Ignore action for identity "{identity.name}" failed. Reason: {e}', exc_info=True)
            except KeyboardInterrupt:
                self._logger.info(f'Ignore action for identity "{identity.name}" interrupted by user')
                break

    def _parse_config(self, config_files: list[str]) -> dict:
        if not config_files:
            config_files = [
                '/etc/usbackup-gphotos/config.conf',
                '/etc/opt/usbackup-gphotos/config.conf',
                os.path.expanduser('~/.config/usbackup-gphotos/config.conf'),
            ]

        config_inst = ConfigParser()
        config_inst.read(config_files)

        # check if any config was found
        if not config_inst.sections():
            raise UsBackupGPhotosConfigError("No config found")

        config = {}

        for section in config_inst.sections():
            section_data = {}

            # make sure our section has "auth_file" and "work_dir" options
            if not config_inst.has_option(section, 'auth_file') or not config_inst.has_option(section, 'data_dir'):
                raise UsBackupGPhotosConfigError(f'Config section "{section}" is missing "auth_file" or "work_dir"')

            for key, value in config_inst.items(section):
                section_data[key] = value

            config[section] = section_data

        return config
    
    def _gen_identities(self, identities_names: list[str], config: dict) -> list[UsBackupGPhotosIdentity]:
        identities_to_create = []
        
        if identities_names:
            for identity_name in identities_names:
                if not identity_name in config:
                    raise UsBackupGPhotosConfigError(f'Identity "{identity_name}" not found in config')

                identities_to_create.append(identity_name)
        else:
            # all identities, except "GLOBALS"
            identities_to_create = [identity_name for identity_name in config.keys() if identity_name != 'GLOBALS']

        if not identities_to_create:
            raise UsBackupGPhotosConfigError('No identities found')
        
        global_config = config.get('GLOBALS', {})
        
        identities = []

        for identity_name in identities_to_create:
            identity_config = config[identity_name]
            identity_config = {**global_config, **identity_config}

            identity = UsBackupGPhotosIdentity(identity_name, identity_config, logger=self._logger)

            identities.append(identity)

        return identities
    
    def _gen_logger(self, log_file: str, log_level: str) -> logging.Logger:
        levels = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL,
        }

        format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

        if not log_level in levels:
            log_level = "INFO"

        logger = logging.getLogger()
        logger.setLevel(levels[log_level])

        if log_file:
            handler = logging.FileHandler(log_file)
        else:
            handler = logging.StreamHandler()

        handler.setLevel(levels[log_level])
        handler.setFormatter(logging.Formatter(format))

        logger.addHandler(handler)

        return logger