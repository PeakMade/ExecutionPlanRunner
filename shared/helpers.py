"""Shared database connection helpers for Fabric Administration hub."""
import os
import struct

from config import AUTH_RECORD_DB_PATH, DB_CONFIG, env

_fabric_credential = None


def get_fabric_connection(warehouse_name):
    """Connect to a Fabric Warehouse or SQL DB using Azure AD token auth."""
    import pyodbc
    from azure.identity import (InteractiveBrowserCredential,
                                TokenCachePersistenceOptions,
                                AuthenticationRecord)

    global _fabric_credential

    if _fabric_credential is None:
        user       = env.get('FABRIC_USER', '')
        cache_opts = TokenCachePersistenceOptions(name='fabric_admin')
        if os.path.exists(AUTH_RECORD_DB_PATH):
            with open(AUTH_RECORD_DB_PATH) as f:
                ar = AuthenticationRecord.deserialize(f.read())
            _fabric_credential = InteractiveBrowserCredential(
                login_hint=user or None,
                cache_persistence_options=cache_opts,
                authentication_record=ar)
        else:
            _fabric_credential = InteractiveBrowserCredential(
                login_hint=user or None,
                cache_persistence_options=cache_opts)
            ar = _fabric_credential.authenticate(
                scopes=['https://database.windows.net/.default'])
            with open(AUTH_RECORD_DB_PATH, 'w') as f:
                f.write(ar.serialize())

    token    = _fabric_credential.get_token('https://database.windows.net/.default')
    tb       = token.token.encode('UTF-16-LE')
    ts       = struct.pack(f'<I{len(tb)}s', len(tb), tb)

    server, catalog = DB_CONFIG[warehouse_name]
    conn = pyodbc.connect(
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'SERVER={server};DATABASE={catalog};'
        f'Encrypt=yes;TrustServerCertificate=no;',
        attrs_before={1256: ts})
    conn.autocommit = True
    return conn
