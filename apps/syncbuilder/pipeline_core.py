"""
pipeline_core.py
Pure pipeline-building logic extracted from the CLI tool.
No interactive prompts, no argparse, no sys.exit.
Auth tokens are supplied by the caller (shared.auth).
"""
import base64
import json
import urllib.request
import urllib.error

from config import FABRIC_API, WORKSPACE_ID, WORKSPACE_NAME, DATABASES


# ── Workspace ID resolver ─────────────────────────────────────────────────────

_resolved_workspace_id = None

def get_workspace_id(fabric_token):
    """Return workspace ID — uses env var directly if set, else resolves by name."""
    global _resolved_workspace_id
    if WORKSPACE_ID:
        return WORKSPACE_ID
    if _resolved_workspace_id:
        return _resolved_workspace_id
    headers = {'Authorization': f'Bearer {fabric_token}'}
    req  = urllib.request.Request(f'{FABRIC_API}/workspaces', headers=headers)
    with urllib.request.urlopen(req) as resp:
        for ws in json.loads(resp.read()).get('value', []):
            if ws['displayName'] == WORKSPACE_NAME:
                _resolved_workspace_id = ws['id']
                return _resolved_workspace_id
    raise ValueError(f'Workspace not found: {WORKSPACE_NAME!r}')


# ── REST helpers ──────────────────────────────────────────────────────────────

def _hdrs(token):
    return {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}


def list_pipelines(fabric_token):
    """Return {name: id} dict of all DataPipeline items in the workspace."""
    ws_id = get_workspace_id(fabric_token)
    url   = f'{FABRIC_API}/workspaces/{ws_id}/items?type=DataPipeline'
    req   = urllib.request.Request(url, headers=_hdrs(fabric_token))
    with urllib.request.urlopen(req) as resp:
        return {p['displayName']: p['id']
                for p in json.loads(resp.read()).get('value', [])}


def export_pipeline(fabric_token, name):
    """Return the decoded pipeline-content JSON for an existing pipeline."""
    pipelines = list_pipelines(fabric_token)
    if name not in pipelines:
        raise ValueError(f'Pipeline not found: {name!r}')
    item_id = pipelines[name]
    ws_id   = get_workspace_id(fabric_token)
    url     = f'{FABRIC_API}/workspaces/{ws_id}/items/{item_id}/getDefinition'
    req     = urllib.request.Request(url, headers=_hdrs(fabric_token),
                                     method='POST', data=b'')
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read())
    for part in data.get('definition', {}).get('parts', []):
        if 'pipeline-content' in part.get('path', ''):
            return json.loads(base64.b64decode(part['payload']))
    return data


def deploy_pipeline(fabric_token, name, definition):
    """Create or update a pipeline. Returns the pipeline item ID."""
    ws_id   = get_workspace_id(fabric_token)
    payload_b64 = base64.b64encode(
        json.dumps(definition, indent=2).encode('utf-8')).decode('ascii')
    parts = [{'path': 'pipeline-content.json',
              'payload': payload_b64,
              'payloadType': 'InlineBase64'}]

    existing = list_pipelines(fabric_token)

    if name in existing:
        item_id = existing[name]
        url  = f'{FABRIC_API}/workspaces/{ws_id}/items/{item_id}/updateDefinition'
        body = json.dumps({'definition': {'parts': parts}}).encode('utf-8')
        req  = urllib.request.Request(url, data=body,
                                      headers=_hdrs(fabric_token), method='POST')
        action = 'updated'
    else:
        url  = f'{FABRIC_API}/workspaces/{ws_id}/items'
        body = json.dumps({'displayName': name, 'type': 'DataPipeline',
                           'definition': {'parts': parts}}).encode('utf-8')
        req  = urllib.request.Request(url, data=body,
                                      headers=_hdrs(fabric_token), method='POST')
        action = 'created'

    try:
        with urllib.request.urlopen(req) as resp:
            result  = json.loads(resp.read())
        item_id = result.get('id', existing.get(name, ''))
        return {'action': action, 'name': name, 'id': item_id}
    except urllib.error.HTTPError as e:
        err = e.read().decode('utf-8', errors='replace')
        raise RuntimeError(f'HTTP {e.code}: {err[:600]}')


# ── Table reference parser ────────────────────────────────────────────────────

def parse_table(raw):
    """Parse 'DATABASE.schema.table' → (db_config, schema, table)."""
    parts = raw.strip().split('.')
    if len(parts) != 3:
        raise ValueError(f'Expected DATABASE.schema.table, got: {raw!r}')
    db_key = parts[0].upper()
    if db_key not in DATABASES:
        raise ValueError(f'Unknown database: {parts[0]!r}. '
                         f'Supported: {", ".join(DATABASES)}')
    return DATABASES[db_key], parts[1], parts[2]


# ── Connection settings blocks ────────────────────────────────────────────────

def _dw_connection_settings(db):
    return {
        'name': db['display'],
        'properties': {
            'annotations': [],
            'type': 'DataWarehouse',
            'typeProperties': {
                'endpoint':    db['endpoint'],
                'artifactId':  db['artifactId'],
                'workspaceId': get_workspace_id.__defaults__,  # filled at call time
            },
            'externalReferences': {'connection': db['connId']},
        }
    }


def _sql_connection_settings(db):
    return {
        'name': db['display'],
        'properties': {
            'annotations': [],
            'type': 'FabricSqlDatabase',
            'typeProperties': {
                'workspaceId': '',   # filled at call time
                'artifactId':  db['artifactId'],
            },
            'externalReferences': {'connection': db['connId']},
        }
    }


def _dw_conn(db, ws_id):
    return {
        'name': db['display'],
        'properties': {
            'annotations': [],
            'type': 'DataWarehouse',
            'typeProperties': {
                'endpoint':    db['endpoint'],
                'artifactId':  db['artifactId'],
                'workspaceId': ws_id,
            },
            'externalReferences': {'connection': db['connId']},
        }
    }


def _sql_conn(db, ws_id):
    return {
        'name': db['display'],
        'properties': {
            'annotations': [],
            'type': 'FabricSqlDatabase',
            'typeProperties': {
                'workspaceId': ws_id,
                'artifactId':  db['artifactId'],
            },
            'externalReferences': {'connection': db['connId']},
        }
    }


def build_source_block(db, schema, table, ws_id):
    conn = _dw_conn(db, ws_id) if db['type'] == 'DataWarehouse' else _sql_conn(db, ws_id)
    if db['type'] == 'DataWarehouse':
        return {
            'type': 'DataWarehouseSource',
            'queryTimeout': '02:00:00',
            'partitionOption': 'None',
            'datasetSettings': {
                'annotations': [],
                'connectionSettings': conn,
                'type': 'DataWarehouseTable',
                'schema': [],
                'typeProperties': {'schema': schema, 'table': table},
            }
        }
    return {
        'type': 'FabricSqlDatabaseSource',
        'queryTimeout': '02:00:00',
        'partitionOption': 'None',
        'datasetSettings': {
            'annotations': [],
            'connectionSettings': conn,
            'type': 'FabricSqlDatabaseTable',
            'schema': [],
            'typeProperties': {'schema': schema, 'table': table},
        }
    }


def build_sink_block(db, schema, table, ws_id):
    conn         = _dw_conn(db, ws_id) if db['type'] == 'DataWarehouse' else _sql_conn(db, ws_id)
    pre_script   = (f"IF OBJECT_ID('{schema}.{table}', 'U') IS NOT NULL "
                    f"TRUNCATE TABLE {schema}.{table}")
    if db['type'] == 'DataWarehouse':
        return {
            'type': 'DataWarehouseSink',
            'preCopyScript': pre_script,
            'allowCopyCommand': True,
            'copyCommandSettings': {},
            'tableOption': 'autoCreate',
            'datasetSettings': {
                'annotations': [],
                'connectionSettings': conn,
                'type': 'DataWarehouseTable',
                'schema': [],
                'typeProperties': {'schema': schema, 'table': table},
            }
        }
    return {
        'type': 'FabricSqlDatabaseSink',
        'preCopyScript': pre_script,
        'writeBehavior': 'insert',
        'sqlWriterUseTableLock': False,
        'tableOption': 'autoCreate',
        'datasetSettings': {
            'annotations': [],
            'connectionSettings': conn,
            'type': 'FabricSqlDatabaseTable',
            'schema': [],
            'typeProperties': {'schema': schema, 'table': table},
        }
    }


def build_pipeline(fabric_token, pipeline_name, description,
                   src_db, src_schema, src_table,
                   tgt_db, tgt_schema, tgt_table):
    """Build the full pipeline definition dict, ready to deploy."""
    ws_id = get_workspace_id(fabric_token)
    return {
        'properties': {
            'description': description,
            'activities': [
                {
                    'name': f'Copy_{src_table}_to_{tgt_table}',
                    'type': 'Copy',
                    'dependsOn': [],
                    'policy': {
                        'timeout': '1:00:00',
                        'retry': 0,
                        'retryIntervalInSeconds': 30,
                        'secureOutput': False,
                        'secureInput': False,
                    },
                    'typeProperties': {
                        'source':  build_source_block(src_db, src_schema, src_table, ws_id),
                        'sink':    build_sink_block(tgt_db, tgt_schema, tgt_table, ws_id),
                        'enableStaging': False,
                        'translator': {
                            'type': 'TabularTranslator',
                            'typeConversion': True,
                            'typeConversionSettings': {
                                'allowDataTruncation': True,
                                'treatBooleanAsNumber': False,
                            }
                        }
                    }
                }
            ],
            'annotations': [tgt_schema, tgt_table, 'syncbuilder'],
        }
    }


def auto_description(src_db, src_schema, src_table, tgt_db, tgt_schema, tgt_table):
    return (f"Copies {src_schema}.{src_table} from {src_db['display']} "
            f"into {tgt_db['display']}.{tgt_schema}. "
            f"Full table, truncate-before-load. Manual trigger only.")
