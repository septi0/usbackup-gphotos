from usync_gphotos.storage import Storage

__all__ = ['AlbumsModel']

class AlbumsModel:
    def __init__(self, storage: Storage) -> None:
        self._storage: Storage = storage

        self._ensure_table()

    def commit(self) -> None:
        self._storage.commit()

    def get_album_meta(self, *, album_id: int = None, remote_id: str = None) -> dict:
        if not album_id and not remote_id:
            raise ValueError('Missing media_id or remote_id')

        placeholders = {}
        where = []

        if album_id:
            where.append('album_id=:album_id')
            placeholders['album_id'] = album_id
        elif remote_id:
            where.append('remote_id=:remote_id')
            placeholders['remote_id'] = remote_id

        query = (
            "SELECT *",
            "FROM albums",
            f"WHERE {' AND '.join(where)}",
            "LIMIT 1",
        )

        with self._storage.execute(query, placeholders) as cursor:
            row = cursor.fetchone()

            if not row:
                return {}

            return dict(row)

    def get_albums_meta(self, *, limit: int = 100, offset: int = 0, status = None) -> list:
        placeholders = {}
        where = []

        where.append(self._storage.gen_in_condition('status', status, placeholders))

        query = (
            "SELECT *",
            "FROM albums",
            f"WHERE {' AND '.join(where)}",
            "ORDER BY album_id ASC",
            "LIMIT :limit OFFSET :offset",
        )

        placeholders['limit'] = limit
        placeholders['offset'] = offset

        with self._storage.execute(query, placeholders) as cursor:
            rows = cursor.fetchall()

            if not rows:
                return []

            return [dict(r) for r in rows]
    
    def get_albums_items_meta(self, *, limit: int = 100, offset: int = 0, status = None, album_id: int = None) -> list:
        placeholders = {}
        where = []

        where.append(self._storage.gen_in_condition('status', status, placeholders))

        if album_id:
            where.append('album_id=:album_id')
            placeholders['album_id'] = album_id

        query = (
            "SELECT *",
            "FROM albums_items",
            f"WHERE {' AND '.join(where)}",
            "ORDER BY album_id ASC, media_id ASC",
            "LIMIT :limit OFFSET :offset",
        )

        placeholders['album_id'] = album_id
        placeholders['limit'] = limit
        placeholders['offset'] = offset

        with self._storage.execute(query, placeholders) as cursor:
            rows = cursor.fetchall()

            if not rows:
                return []

            return [dict(r) for r in rows]
    
    def get_album_items_meta_cnt(self, album_id: int, *, status = None) -> int:
        if not album_id:
            raise ValueError('Missing album_id')
        
        placeholders = {}
        where = []
        
        where.append('album_id=:album_id')
        where.append(self._storage.gen_in_condition('status', status, placeholders))
        
        query = (
            "SELECT COUNT(album_id) AS cnt",
            "FROM albums_items",
            f"WHERE {' AND '.join(where)}",
        )

        with self._storage.execute(query, {'album_id': album_id}) as cursor:
            row = cursor.fetchone()

            if not row:
                return 0

            return row['cnt']
        
    def search_album_meta(self, *, limit: int = 100, offset: int = 0, cname: str = None, path: str = None) -> list:
        placeholders = {}
        where = []

        if cname:
            where.append('cname=:cname')
            placeholders['cname'] = cname

        if path:
            where.append('path=:path')
            placeholders['path'] = path

        query = (
            "SELECT *",
            "FROM albums",
            f"WHERE {' AND '.join(where)}",
            "ORDER BY album_id ASC",
            "LIMIT :limit OFFSET :offset",
        )

        placeholders['limit'] = limit
        placeholders['offset'] = offset

        with self._storage.execute(query, placeholders) as cursor:
            rows = cursor.fetchall()

            if not rows:
                return []

            return [dict(r) for r in rows]

    def update_album_meta(self, album_id: int, **kwargs) -> int:
        if not album_id:
            raise ValueError('Missing album_id')
        
        allowed_keys = ['name', 'cname', 'size', 'cover_photo_id', 'status', 'index_date', 'last_checked']

        for key in kwargs.keys():
            if key not in allowed_keys:
                raise ValueError(f'Invalid key "{key}"')
            
        placeholders = {}
            
        update = self._storage.gen_update_fields(kwargs, placeholders)

        query = (
            "UPDATE albums",
            f"SET {update}",
            "WHERE album_id=:album_id",
            "LIMIT 1",
        )
    
        placeholders['album_id'] = album_id

        with self._storage.execute(query, placeholders, commit=False) as cursor:
            return cursor.rowcount


    def update_album_item_meta(self, album_id: int, media_id: int, **kwargs) -> int:
        if not album_id or not media_id:
            raise ValueError('Missing album_id or media_id')
        
        allowed_keys = ['status']

        for key in kwargs.keys():
            if key not in allowed_keys:
                raise ValueError(f'Invalid key "{key}"')

        placeholders = {}

        update = self._storage.gen_update_fields(kwargs, placeholders)

        query = (
            "UPDATE albums_items",
            f"SET {update}",
            "WHERE album_id=:album_id AND media_id=:media_id",
            "LIMIT 1",
        )

        placeholders['album_id'] = album_id
        placeholders['media_id'] = media_id

        with self._storage.execute(query, placeholders, commit=False) as cursor:
            return cursor.rowcount

    def set_albums_meta_stale(self, *, last_checked: str = None) -> int:
        placeholders = {}
        where = []

        if last_checked:
            where.append('last_checked<:last_checked')
            placeholders['last_checked'] = last_checked

        query = (
            "UPDATE albums",
            "SET status='stale'",
            f"WHERE {' AND '.join(where)}",
        )

        with self._storage.execute(query, placeholders) as cursor:
            return cursor.rowcount

    def set_albums_items_meta_stale(self) -> int:
        query = (
            "UPDATE albums_items",
            "SET status='stale'",
            "FROM (",
            "   SELECT * FROM albums_items ai",
            "   LEFT JOIN albums a ON ai.album_id=a.album_id",
            "   WHERE a.album_id IS NULL OR a.status='stale'",
            ") AS t",
            "WHERE albums_items.album_id=t.album_id AND albums_items.media_id=t.media_id",
        )

        with self._storage.execute(query) as cursor:
            return cursor.rowcount

    def set_album_items_meta_stale(self, album_id: str) -> int:
        if not album_id:
            raise ValueError('Missing album_id')
        
        placeholders = {}

        query = (
            "UPDATE albums_items",
            "SET status='stale'",
            "WHERE album_id=:album_id",
        )

        placeholders['album_id'] = album_id

        with self._storage.execute(query, placeholders) as cursor:
            return cursor.rowcount

    def delete_album_meta(self, album_id: int) -> int:
        if not album_id:
            raise ValueError('Missing album_id')
        
        placeholders = {}

        query = (
            "DELETE FROM albums",
            "WHERE album_id=:album_id",
            "LIMIT 1",
        )

        placeholders['album_id'] = album_id

        with self._storage.execute(query, placeholders, commit=False) as cursor:
            return cursor.rowcount

    def delete_album_item_meta(self, album_id: int, media_id: int) -> int:
        if not album_id or not media_id:
            raise ValueError('Missing album_id or media_id')
        
        placeholders = {}

        query = (
            "DELETE FROM albums_items",
            "WHERE album_id=:album_id AND media_id=:media_id",
            "LIMIT 1",
        )

        placeholders['album_id'] = album_id
        placeholders['media_id'] = media_id

        with self._storage.execute(query, placeholders, commit=False) as cursor:
            return cursor.rowcount

    def add_album_meta(
            self,
            remote_id: str,
            name: str,
            cname: str,
            size: int,
            cover_photo_id: str,
            path: str,
            index_date: str,
            last_checked: str,
            status: str = None,
        ) -> int:
        placeholders = {}

        query = (
            "INSERT INTO albums (remote_id, name, cname, size, cover_photo_id, path, index_date, last_checked, status)",
            "VALUES (:remote_id, :name, :cname, :size, :cover_photo_id, :path, :index_date, :last_checked, :status)",
            "ON CONFLICT(remote_id) DO UPDATE SET",
            "size=:size, cover_photo_id=:cover_photo_id, index_date=:index_date, last_checked=:last_checked, status=:status",
        )

        placeholders['remote_id'] = remote_id
        placeholders['name'] = name
        placeholders['cname'] = cname
        placeholders['size'] = size
        placeholders['cover_photo_id'] = cover_photo_id
        placeholders['path'] = path
        placeholders['index_date'] = index_date
        placeholders['last_checked'] = last_checked
        placeholders['status'] = status or 'pending_sync'

        with self._storage.execute(query, placeholders, commit=False) as cursor:
            return cursor.lastrowid

    def add_album_item_meta(self, *, album_id: int, media_item_id: int, status: str = None) -> int:
        placeholders = {}

        query = (
            "INSERT INTO albums_items (album_id, media_id, status)",
            "VALUES (:album_id, :media_id, :status)",
            "ON CONFLICT(album_id, media_id) DO UPDATE SET",
            "status=:status",
        )

        placeholders['album_id'] = album_id
        placeholders['media_id'] = media_item_id
        placeholders['status'] = status or 'pending_sync'

        with self._storage.execute(query, placeholders, commit=False) as cursor:
            return cursor.lastrowid
        
    def _ensure_table(self) -> None:
        # create albums table if not exists
        query = (
            "CREATE TABLE IF NOT EXISTS albums (",
            "   album_id INTEGER PRIMARY KEY AUTOINCREMENT,",
            "   remote_id TEXT NOT NULL UNIQUE,",
            "   name TEXT,",
            "   cname TEXT,",
            "   size INTEGER,",
            "   cover_photo_id TEXT,",
            "   path TEXT,",
            "   index_date DATETIME,",
            "   last_checked DATETIME,",
            "   status TEXT",
            ")"
        )

        with self._storage.execute(query):
            pass

        # create albums_items table if not exists
        query = (
            "CREATE TABLE IF NOT EXISTS albums_items (",
            "   album_id INTEGER NOT NULL,",
            "   media_id INTEGER NOT NULL,",
            "   status TEXT,",
            "   PRIMARY KEY (album_id, media_id)",
            ")",
        )

        with self._storage.execute(query):
            pass
