"""Shared configuration for the Fabric Administration hub."""
import json
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ── .env loader ───────────────────────────────────────────────────────────────

def load_env(env_path=None):
    if env_path is None:
        env_path = os.path.join(BASE_DIR, '.env')
    env = {}
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, val = line.split('=', 1)
                    env[key.strip()] = val.strip()
    return env

env = load_env()

# ── Auth token cache paths ────────────────────────────────────────────────────

AUTH_RECORD_DB_PATH     = os.path.join(BASE_DIR, '.auth_record.json')
AUTH_RECORD_FABRIC_PATH = os.path.join(BASE_DIR, '.auth_record_fabric.json')

# ── App registry ──────────────────────────────────────────────────────────────

REGISTRY_PATH = os.path.join(BASE_DIR, 'app_registry.json')

def load_registry():
    with open(REGISTRY_PATH, encoding='utf-8') as f:
        return json.load(f)

def save_registry(data):
    with open(REGISTRY_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)

# ── Fabric workspace ──────────────────────────────────────────────────────────

FABRIC_API       = 'https://api.fabric.microsoft.com/v1'
WORKSPACE_ID     = env.get('FABRIC_WORKSPACE_ID', '')
WORKSPACE_NAME   = env.get('FABRIC_WORKSPACE_NAME', '')

# ── Database connection map ───────────────────────────────────────────────────

DW_ENDPOINT  = env.get('FABRIC_DW_ENDPOINT', '')
SQL_ENDPOINT = env.get('FABRIC_SQL_ENDPOINT', '')

DATABASES = {
    'WH_STAGING': {
        'type':       'DataWarehouse',
        'endpoint':   env.get('FABRIC_WH_STAGING', DW_ENDPOINT),
        'artifactId': env.get('FABRIC_WH_STAGING_ARTIFACT_ID', ''),
        'connId':     env.get('FABRIC_WH_STAGING_CONN_ID', ''),
        'display':    'WH_STAGING',
    },
    'WH_ODS': {
        'type':       'DataWarehouse',
        'endpoint':   env.get('FABRIC_WH_ODS', DW_ENDPOINT),
        'artifactId': env.get('FABRIC_WH_ODS_ARTIFACT_ID', ''),
        'connId':     env.get('FABRIC_WH_ODS_CONN_ID', ''),
        'display':    'WH_ODS',
    },
    'WH_PROD2': {
        'type':       'DataWarehouse',
        'endpoint':   env.get('FABRIC_WH_PROD2', DW_ENDPOINT),
        'artifactId': env.get('FABRIC_WH_PROD2_ARTIFACT_ID', ''),
        'connId':     env.get('FABRIC_WH_PROD2_CONN_ID', ''),
        'display':    'WH_PROD2',
    },
    'DB_APP_SUPPORT': {
        'type':       'FabricSqlDatabase',
        'endpoint':   env.get('FABRIC_DB_APP_SUPPORT', SQL_ENDPOINT),
        'artifactId': env.get('FABRIC_DB_APP_SUPPORT_ARTIFACT_ID', ''),
        'connId':     env.get('FABRIC_DB_APP_SUPPORT_CONN_ID', ''),
        'display':    'DB_App_Support',
    },
    'DB_BI_SUPPORT': {
        'type':       'FabricSqlDatabase',
        'endpoint':   env.get('FABRIC_DB_BI_SUPPORT', SQL_ENDPOINT),
        'artifactId': env.get('FABRIC_DB_BI_SUPPORT_ARTIFACT_ID', ''),
        'connId':     env.get('FABRIC_DB_BI_SUPPORT_CONN_ID', ''),
        'display':    'DB_BI_Support',
    },
}

DB_CONFIG = {
    'WH_STAGING':     (env.get('FABRIC_WH_STAGING', ''),     'WH_STAGING'),
    'WH_ODS':         (env.get('FABRIC_WH_ODS',     env.get('FABRIC_WH_STAGING', '')), 'WH_ODS'),
    'WH_PROD2':       (env.get('FABRIC_WH_PROD2',   env.get('FABRIC_WH_STAGING', '')), 'WH_PROD2'),
    'DB_APP_SUPPORT': (env.get('FABRIC_DB_APP_SUPPORT', ''), env.get('FABRIC_DB_APP_SUPPORT_CATALOG', 'DB_APP_SUPPORT')),
    'DB_BI_SUPPORT':  (env.get('FABRIC_DB_BI_SUPPORT',  env.get('FABRIC_DB_APP_SUPPORT', '')),
                       env.get('FABRIC_DB_BI_SUPPORT_CATALOG',  'DB_BI_SUPPORT')),
}
