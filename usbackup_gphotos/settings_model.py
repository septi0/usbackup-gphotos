from usbackup_gphotos.storage import Storage

__all__ = ['SettingsModel']

class SettingsModel:
    def __init__(self, storage: str) -> None:
        self._storage: Storage = storage

        self._ensure_table()
    
    def get_settings(self) -> dict:
        query = (
            "SELECT *",
            "FROM settings",
        )

        with self._storage.execute(query) as cursor:
            rows = cursor.fetchall()

            if not rows:
                return {}

            return {r['key']: r['value'] for r in rows} 
    
    def update_aseting(self, key: str, value: str) -> int:
        if not key:
            raise ValueError('key must be specified')

        query = (
            "INSERT INTO settings (key, value)",
            "VALUES (:key, :value)",
            "ON CONFLICT (key) DO UPDATE SET value=:value",
        )

        placeholders = {
            'key': key,
            'value': value,
        }

        with self._storage.execute(query, placeholders) as cursor:
            return cursor.rowcount
        
    def _ensure_table(self):
        query = (
            "CREATE TABLE IF NOT EXISTS settings (",
            "   key TEXT PRIMARY KEY,",
            "   value TEXT",
            ")",
        )

        with self._storage.execute(query) as cursor:
            pass