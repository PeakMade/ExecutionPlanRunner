"""Shared Azure authentication for the Fabric Administration hub.

Uses the same 'de_remediation' MSAL persistent token cache as the original
execution_plan_app so existing Windows DPAPI-cached tokens are reused without
requiring a browser re-authentication.  A single credential instance serves
both the DB scope and the Fabric API scope.
"""
import os
import struct
import threading

from config import env

_credential = None
_lock       = threading.Lock()

_FABRIC_USER  = env.get('FABRIC_USER', '')
_DB_SCOPE     = 'https://database.windows.net/.default'
_FABRIC_SCOPE = 'https://api.fabric.microsoft.com/.default'

_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Auth-record search order: original app first (already authenticated there),
# then the hub's own file if the user has authenticated through the hub before.
_AUTH_RECORD_CANDIDATES = [
    r'C:\execution_plan_app\.auth_record.json',
    os.path.join(_BASE_DIR, '.auth_record.json'),
]
_OWN_AUTH_RECORD = os.path.join(_BASE_DIR, '.auth_record.json')


def _get_credential():
    global _credential
    with _lock:
        if _credential is None:
            from azure.identity import (InteractiveBrowserCredential,
                                        TokenCachePersistenceOptions,
                                        AuthenticationRecord)
            cache_opts = TokenCachePersistenceOptions(name='de_remediation')

            # Find an existing auth record to enable fully-silent token refresh
            record_path = None
            for path in _AUTH_RECORD_CANDIDATES:
                if os.path.exists(os.path.normpath(path)):
                    record_path = os.path.normpath(path)
                    break

            if record_path:
                with open(record_path) as f:
                    ar = AuthenticationRecord.deserialize(f.read())
                _credential = InteractiveBrowserCredential(
                    login_hint=_FABRIC_USER or None,
                    cache_persistence_options=cache_opts,
                    authentication_record=ar)
            else:
                # First-time setup: open browser once, then save the record
                _credential = InteractiveBrowserCredential(
                    login_hint=_FABRIC_USER or None,
                    cache_persistence_options=cache_opts)
                ar = _credential.authenticate(scopes=[_DB_SCOPE])
                with open(_OWN_AUTH_RECORD, 'w') as f:
                    f.write(ar.serialize())

    return _credential


# ── Public helpers ─────────────────────────────────────────────────────────────

def get_db_credential():
    return _get_credential()


def get_fabric_credential():
    return _get_credential()


def get_db_token_struct():
    token = _get_credential().get_token(_DB_SCOPE)
    tb    = token.token.encode('UTF-16-LE')
    return struct.pack(f'<I{len(tb)}s', len(tb), tb)


def get_fabric_token():
    return _get_credential().get_token(_FABRIC_SCOPE).token
