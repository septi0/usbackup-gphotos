import os
import sqlite3
from contextlib import contextmanager

class Storage:
    def __init__(self, db_file: str) -> None:
        if not db_file:
            raise ValueError('db_file must be specified')
        
        db_path = os.path.dirname(db_file)

        if not os.path.isdir(db_path):
            os.makedirs(db_path)

        self._conn: sqlite3.Connection = sqlite3.connect(db_file, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row

    @contextmanager
    def execute(self, query: str | tuple, params: dict = None, *, commit: bool = True):
        if not query:
            raise ValueError('query must be specified')
        
        if isinstance(query, tuple):
            query = '\n'.join(query)

        # query_debug = query

        # # replace placeholders with values
        # for placeholder, value in placeholders.items():
        #     query_debug = query_debug.replace(f':{placeholder}', f'{value}')

        # print(query_debug)
        
        try:
            cursor = self._conn.cursor()
            # print('')
            # print(query)
            cursor.execute(query, params or {})
            yield cursor
        finally:
            if commit:
                self._conn.commit()

            cursor.close()

    def commit(self) -> None:
        self._conn.commit()

    def gen_in_condition(self, field: str, values: str | list, data: dict) -> str:
        if not field or not values:
            return ''
        
        if isinstance(values, str):
            values = [values]

        in_values = []

        for i, s in enumerate(values):
            in_values.append(f':{field}_{i}')
            data[f'{field}_{i}'] = s

        return f'{field} IN ({", ".join(in_values)})'
    
    def gen_update_fields(self, fields: dict, data: dict) -> str:
        if not fields:
            return ''
        
        update_fields = []

        for field, value in fields.items():
            update_fields.append(f'{field}=:{field}')
            data[field] = value

        return ', '.join(update_fields)