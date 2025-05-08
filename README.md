# UsBackup-GPhotos

## WARNING!
The software no longer works as Google has changed the API permissions and fetching the media items outside of the app's scope is not possible anymore. Repository kept for historical reasons only.

.

.

.

.

.

.

.

.

.

.

.

.

.

.

.

.

.

.

.

.

## Description

**UsBackup-GPhotos** is a tool for downloading and syncing media items and albums from Google Photos to your local storage. It's useful for creating backups of your photos, accessing memories offline or organizing your personal digital life. This tool ensures that Google Photos is always in sync with your local storage by downloading newly added media items and albums while optionally deleting obsolete ones from the local storage.

## Features

- Media items download
- Albums download
- Multiple identities support
- Media items organization by date (year/month)
- Albums organization by name
- Obsolete items deletion (from local storage)
- Storage efficient (album items can be created as links to the original items)

## LIMITATIONS:
- Burst photos not supported
- GPS info is removed from items
- Original quality is not available

## Software requirements

- python3

## Installation

#### 1. As a package

```
pip install --upgrade <git-repo>
```

or 

```
git clone <git-repo>
cd <git-repo>
python setup.py install
```

#### 2. As a standalone script

```
git clone <git-repo>
```

## Usage

UsBackup-GPhotos can be used in 3 ways:

#### 1. As a package (if installed globally)

```
/usr/bin/usbackup-gphotos <parameters>
```

#### 2. As a package (if installed in a virtualenv)

```
<path-to-venv>/bin/usbackup-gphotos <parameters>
```

#### 3. As a standalone script

```
<git-clone-dir>/run.py <parameters>
```

Check "Command line arguments" section for more information about the available parameters.

## Command line arguments

```
usbackup-gphotos [-h] [--config CONFIG_FILES] [--identity IDENTITIES] [--use-webserver] [--log LOG_FILE] [--log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}] [--version] {index,sync,delete,auth,stats,ignore} ...

options:
  -h, --help            show this help message and exit
  --config CONFIG_FILES
                        Alternative config file(s)
  --identity IDENTITIES
                        Identity name(s)
  --use-webserver       Use webserver to authenticate
  --log LOG_FILE        Log file where to write logs
  --log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}
                        Log level
  --version             show program's version number and exit

Commands:
  {index,sync,delete,auth,stats,ignore}
    index               Index media items
        options:
        -h, --help          show this help message and exit
        --skip-media-items  Skip media items sync
        --skip-albums       Skip albums sync
        --rescan            Rescan all media items
        --album ALBUMS      Album name(s)

    sync                Sync media items
        options:
        -h, --help            show this help message and exit
        --skip-index          Skip indexing
        --skip-media-items    Skip media items sync
        --skip-albums         Skip albums sync
        --rescan              Rescan all media items
        --album ALBUMS        Album name(s)
        --concurrency CONCURRENCY
                                Concurrency
        --albums-sync-mode {symlink,hardlink,copy}
                    Albums sync mode

    delete              Delete obsolete media items
    auth                Authenticate
    stats               Stats
    ignore              Ignore media items
        options:
        -h, --help  show this help message and exit
        --set SET   Set ignored media id(s)
        --reset     Reset ignored media ids
```

## Configuration file

For a sample configuration file see `config.sample.conf` file. Aditionally, you can copy the file to `/etc/usbackup-gphotos/config.conf`, `/etc/opt/usbackup-gphotos/config.conf` or `~/.config/usbackup-gphotos/config.conf` (or where you want as long as you provide the `--config` parameter) and adjust the values to your needs.

Each section in the configuration file is a **identity**. These sections are independent of each other and each needs to be configured separately.

Section properties:
- `auth_file` - Path to the google auth file. (You must provide it from the google cloud console)
- `data_dir` - # Path for the data directory (where the data/metadata will be stored)

## Authentication file

The authentication file is a json file that contains the credentials for the google photos api. You can create it from the google cloud console. For more information see: https://developers.google.com/photos/library/guides/get-started

## Disclaimer

This software is provided as is, without any warranty. Use at your own risk. The author is not responsible for any damage caused by this software.

The software and author are not affiliated with Google Photos or Alphabet in any way.

## License

This software is licensed under the GNU GPL v3 license. See the LICENSE file for more information.