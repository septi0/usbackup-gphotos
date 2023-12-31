from usbackup_gphotos.storage import Storage

__all__ = ['MediaItemsModel']

class MediaItemsModel:
    def __init__(self, storage: Storage) -> None:
        self._storage: Storage = storage

        self._item_statuses: list = ['pending_sync', 'sync_error', 'synced', 'stale', 'ignored']

        self._ensure_table()

    def commit(self) -> None:
        self._storage.commit()

    def get_media_item_meta(self, *, media_id: int = None, remote_id: str = None) -> dict:
        if not media_id and not remote_id:
            raise ValueError('Missing media_id or remote_id')
        
        placeholders = {}
        where = ['1=1']
        
        if media_id:
            where.append("media_id=:media_id")
            placeholders['media_id'] = media_id
        elif remote_id:
            where.append("remote_id=:remote_id")
            placeholders['remote_id'] = remote_id

        query = (
            "SELECT *",
            "FROM media_items",
            f"WHERE {' AND '.join(where)}",
            "LIMIT 1",
        )

        with self._storage.execute(query, placeholders) as cursor:
            row = cursor.fetchone()

            if not row:
                return {}

            return dict(row)
        
    def get_media_items_meta_cnt(self, *, status = None) -> int:
        placeholders = {}
        where = ['1=1']

        if status:
            where.append(self._storage.gen_in_condition('status', status, placeholders))

        query = (
            "SELECT COUNT(*) AS cnt",
            "FROM media_items",
            f"WHERE {' AND '.join(where)}",
        )

        with self._storage.execute(query, placeholders) as cursor:
            row = cursor.fetchone()

            if not row:
                return 0

            return row['cnt']

    def get_media_items_meta_stats(self) -> dict:
        query = (
            "SELECT status, COUNT(status) AS cnt",
            "FROM media_items",
            "GROUP BY status",
            "ORDER BY status ASC",
        )

        with self._storage.execute(query) as cursor:
            rows = cursor.fetchall()

            if not rows:
                return {}

            return {r['status']: r['cnt'] for r in rows}
        
    def search_media_items_meta(self, *, limit: int = 100, offset: int = 0, cname: str = None, path: str = None, status = None) -> list:
        placeholders = {}
        where = ['1=1']

        if cname:
            where.append('cname=:cname')
            placeholders['cname'] = cname

        if path:
            where.append('path=:path')
            placeholders['path'] = path

        if status:
            where.append(self._storage.gen_in_condition('status', status, placeholders))

        query = (
            "SELECT *",
            "FROM media_items",
            f"WHERE {' AND '.join(where)}",
            "ORDER BY media_id ASC",
            "LIMIT :limit OFFSET :offset",
        )

        placeholders['limit'] = limit
        placeholders['offset'] = offset

        with self._storage.execute(query, placeholders) as cursor:
            rows = cursor.fetchall()

            if not rows:
                return []

            return [dict(r) for r in rows]
    
    def update_media_item_meta(self, media_id: int, **kwargs) -> int:
        if not media_id:
            raise ValueError('Missing media_id')
        
        allowed_keys = ['index_date', 'last_checked', 'status']

        for key in kwargs.keys():
            if key not in allowed_keys:
                raise ValueError(f'Invalid key "{key}"')
            
        if 'status' in kwargs and kwargs['status'] not in self._item_statuses:
            raise ValueError(f'Invalid status "{kwargs["status"]}"')
            
        placeholders = {}
            
        update = self._storage.gen_update_fields(kwargs, placeholders)
            
        query = (
            "UPDATE media_items",
            f"SET {update}",
            "WHERE media_id=:media_id",
            "LIMIT 1",
        )

        placeholders['media_id'] = media_id

        with self._storage.execute(query, placeholders, commit=False) as cursor:
            return cursor.rowcount
    
    def set_media_items_stale(self, *, last_checked: str = None) -> int:
        placeholders = {}
        where = ['1=1']

        if last_checked:
            where.append('last_checked<:last_checked')
            placeholders['last_checked'] = last_checked

        query = (
            "UPDATE media_items",
            "SET status='stale'",
            f"WHERE {' AND '.join(where)}",
        )
        
        with self._storage.execute(query, placeholders) as cursor:
            return cursor.rowcount
        
    def reset_ignored_media_items(self) -> int:
        query = (
            "UPDATE media_items",
            "SET status='pending_sync'",
            "WHERE status='ignored'",
        )
        
        with self._storage.execute(query) as cursor:
            return cursor.rowcount

    def delete_media_item_meta(self, media_id: int) -> int:
        if not media_id:
            raise ValueError('Missing media_id')
        
        placeholders = {}

        query = (
            "DELETE FROM media_items",
            "WHERE media_id=:media_id",
        )

        placeholders['media_id'] = media_id

        with self._storage.execute(query, placeholders, commit=False) as cursor:
            return cursor.rowcount
    
    def add_media_item_meta(
            self,
            remote_id: str, 
            name: str,
            cname: str,
            mime_type: str,
            create_date: str,
            modify_date: str,
            path: str,
            index_date: str,
            last_checked: str,
            status: str = None
        ) -> int:
        placeholders = {}

        if status and status not in self._item_statuses:
            raise ValueError(f'Invalid status "{status}"')

        query = (
            "INSERT INTO media_items (remote_id, name, cname, mime_type, create_date, modify_date, path, index_date, last_checked, status)",
            "VALUES (:remote_id, :name, :cname, :mime_type, :create_date, :modify_date, :path, :index_date, :last_checked, :status)",
            "ON CONFLICT (remote_id) DO UPDATE",
            "SET index_date=:index_date, last_checked=:last_checked, status=:status",
        )

        placeholders['remote_id'] = remote_id
        placeholders['name'] = name
        placeholders['cname'] = cname
        placeholders['mime_type'] = mime_type
        placeholders['create_date'] = create_date
        placeholders['modify_date'] = modify_date
        placeholders['path'] = path
        placeholders['index_date'] = index_date
        placeholders['last_checked'] = last_checked
        placeholders['status'] = status

        with self._storage.execute(query, placeholders) as cursor:
            return cursor.lastrowid
    
    def _ensure_table(self):
        query = (
            "CREATE TABLE IF NOT EXISTS media_items (",
            "   media_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,",
            "   remote_id TEXT NOT NULL UNIQUE,",
            "   name TEXT NOT NULL,",
            "   cname TEXT NOT NULL,",
            "   mime_type TEXT NOT NULL,",
            "   create_date DATETIME NOT NULL,",
            "   modify_date DATETIME NOT NULL,",
            "   path TEXT NOT NULL,",
            "   index_date DATETIME,",
            "   last_checked DATETIME,",
            "   status TEXT NOT NULL,",
            "   CHECK (status IN ('" + "','".join(self._item_statuses) + "'))",
            ")",
        )
        
        with self._storage.execute(query) as cursor:
            pass