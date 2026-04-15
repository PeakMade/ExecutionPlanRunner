"""
fetch_fabric_items.py
Collects all DataPipeline and Dataflow (Gen2) items from the configured
Fabric workspace, resolves folder paths, and extracts source/target tables
from item definitions. Writes:

  apps/datatrace/data/pipelines.json
  apps/datatrace/data/dataflows.json

Run from the FabricAdministration root:
  python apps/datatrace/scripts/fetch_fabric_items.py
"""
import base64
import json
import os
import re
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

# ── Path setup ────────────────────────────────────────────────────────────────
ROOT     = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
DATA_DIR = os.path.join(ROOT, 'apps', 'datatrace', 'data')
sys.path.insert(0, ROOT)

from config import FABRIC_API
from shared.auth import get_fabric_token

TARGET_WORKSPACE = 'Fabric ETL Workspace'


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _hdrs(token):
    return {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}


def _request(url, token, method='GET', data=None):
    """Single request with 429 retry."""
    for attempt in range(5):
        try:
            req = urllib.request.Request(url, headers=_hdrs(token),
                                         method=method, data=data)
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8', errors='replace')
            if e.code == 429:
                m = re.search(r'until:\s*(.+?)\s*\(UTC\)', body)
                if m:
                    until = datetime.strptime(m.group(1).strip(), '%m/%d/%Y %I:%M:%S %p')
                    until = until.replace(tzinfo=timezone.utc)
                    wait  = max((until - datetime.now(timezone.utc)).total_seconds() + 1, 1)
                    print(f'  Rate limited, waiting {wait:.0f}s...')
                    time.sleep(wait)
                    continue
                time.sleep(5 * (attempt + 1))
                continue
            raise RuntimeError(f'HTTP {e.code}: {body[:200]}')
    raise RuntimeError(f'Failed after 5 retries: {url}')


def _get_all_pages(base_url, token):
    """Follow Fabric API pagination via continuationToken."""
    items    = []
    next_url = base_url
    while next_url:
        data     = _request(next_url, token)
        items.extend(data.get('value', []))
        cont     = data.get('continuationToken')
        sep      = '&' if '?' in base_url else '?'
        next_url = f'{base_url}{sep}continuationToken={cont}' if cont else None
        time.sleep(0.3)
    return items


# ── Folder path resolution ────────────────────────────────────────────────────

def _build_folder_map(folders):
    by_id = {f['id']: f for f in folders}

    def resolve(fid, seen=None):
        if fid not in by_id:
            return ''
        if seen is None:
            seen = set()
        if fid in seen:
            return by_id[fid]['displayName']
        seen.add(fid)
        folder    = by_id[fid]
        parent_id = folder.get('parentFolderId')
        if parent_id:
            parent = resolve(parent_id, seen)
            return f'{parent} / {folder["displayName"]}' if parent else folder['displayName']
        return folder['displayName']

    return {fid: resolve(fid) for fid in by_id}


# ── Definition parsers ────────────────────────────────────────────────────────

def _parse_pipeline_definition(content):
    """Extract sources and targets from a pipeline-content JSON dict."""
    sources, targets = set(), set()

    def walk_activities(activities):
        for act in activities:
            atype = act.get('type', '')
            tp    = act.get('typeProperties', {})

            if atype == 'Copy':
                src_ds    = tp.get('source', {}).get('datasetSettings', {})
                snk_ds    = tp.get('sink',   {}).get('datasetSettings', {})
                src_conn  = src_ds.get('connectionSettings', {}).get('name', '')
                snk_conn  = snk_ds.get('connectionSettings', {}).get('name', '')
                src_props = src_ds.get('typeProperties', {})
                snk_props = snk_ds.get('typeProperties', {})
                if src_conn and src_props.get('table'):
                    sources.add(f"{src_conn}.{src_props.get('schema','dbo')}.{src_props['table']}")
                if snk_conn and snk_props.get('table'):
                    targets.add(f"{snk_conn}.{snk_props.get('schema','dbo')}.{snk_props['table']}")

            elif atype == 'SqlServerStoredProcedure':
                sp  = tp.get('storedProcedureName', {})
                sp_name = sp.get('value', sp) if isinstance(sp, dict) else sp
                db  = tp.get('database', {})
                db_name = db.get('value', db) if isinstance(db, dict) else db
                if sp_name and str(sp_name).strip():
                    ref = f"{db_name}.{sp_name}" if db_name else str(sp_name)
                    targets.add(f"SP: {ref}")

            # recurse into ForEach / IfCondition / Until bodies
            for key in ('activities', 'ifTrueActivities', 'ifFalseActivities',
                        'body', 'defaultActivities'):
                inner = tp.get(key)
                if isinstance(inner, list):
                    walk_activities(inner)
            for case in tp.get('cases', []):
                walk_activities(case.get('activities', []))

    acts = content.get('properties', {}).get('activities', [])
    walk_activities(acts)
    return sorted(sources), sorted(targets)


def _parse_dataflow_definition(parts_by_path):
    """Extract sources and targets from Dataflow Gen2 definition parts."""
    sources, targets = [], []

    # Target table(s) — look in .platform description first
    platform_raw = parts_by_path.get('.platform', '{}')
    try:
        meta = json.loads(platform_raw).get('metadata', {})
        desc = meta.get('description', '')
        # Match patterns like "WH_ODS.dbo.TABLE_NAME" or "WH_ODS as WH_ODS.dbo.TABLE_NAME"
        for db, schema, tbl in re.findall(
                r'\b((?:WH|DB)_\w+)\.((?:dbo|\w+))\.(\w+)', desc):
            ref = f'{db}.{schema}.{tbl}'
            if ref not in targets:
                targets.append(ref)
    except Exception:
        pass

    # Source connections — from queryMetadata.json
    qmeta_raw = parts_by_path.get('queryMetadata.json', '{}')
    try:
        qmeta = json.loads(qmeta_raw)
        warehouse_kinds = {'Warehouse', 'FabricSqlDatabase', 'DataWarehouse', 'SQLServer'}
        for conn in qmeta.get('connections', []):
            kind = conn.get('kind', '')
            path = conn.get('path', '')
            if kind not in warehouse_kinds:
                label = path if path else kind
                sources.append(f'{kind}: {label}')
    except Exception:
        pass

    # If no sources found from connections, try to detect from mashup.pq
    if not sources:
        pq = parts_by_path.get('mashup.pq', '')
        if 'SharePoint' in pq:
            sources.append('SharePoint')
        elif 'File' in pq or 'Csv' in pq:
            sources.append('File')

    return sources, targets


def _get_definition(ws_id, item_id, token):
    """Fetch item definition and return dict of {path: raw_payload_str}."""
    try:
        data  = _request(
            f'{FABRIC_API}/workspaces/{ws_id}/items/{item_id}/getDefinition',
            token, method='POST', data=b'')
        parts = data.get('definition', {}).get('parts', [])
        result = {}
        for part in parts:
            decoded = base64.b64decode(part['payload']).decode('utf-8', errors='replace')
            # Store by basename (e.g. "pipeline-content.json", "mashup.pq", ".platform")
            key = part.get('path', '').split('/')[-1]
            result[key] = decoded
        return result
    except Exception as e:
        print(f'    def fetch failed ({item_id}): {e}')
        return {}


# ── Core fetch ────────────────────────────────────────────────────────────────

def fetch_all(token):
    print('Fetching workspaces...')
    workspaces = _get_all_pages(f'{FABRIC_API}/workspaces', token)
    ws = next((w for w in workspaces if w['displayName'] == TARGET_WORKSPACE), None)
    if not ws:
        raise RuntimeError(f'Workspace not found: {TARGET_WORKSPACE!r}')
    ws_id, ws_name = ws['id'], ws['displayName']
    print(f'Found: {ws_name} ({ws_id})')

    raw_folders = _get_all_pages(f'{FABRIC_API}/workspaces/{ws_id}/folders', token)
    folder_map  = _build_folder_map(raw_folders)
    print(f'  {len(raw_folders)} folders resolved')

    pipelines, dataflows = [], []

    # ── Pipelines ──
    pl_items = _get_all_pages(f'{FABRIC_API}/workspaces/{ws_id}/items?type=DataPipeline', token)
    print(f'  {len(pl_items)} pipelines — fetching definitions...')
    for i, item in enumerate(pl_items, 1):
        fid         = item.get('folderId')
        folder_path = folder_map.get(fid, '') if fid else ''
        parts       = _get_definition(ws_id, item['id'], token)
        content_raw = parts.get('pipeline-content.json', '')
        sources, targets = [], []
        if content_raw:
            try:
                sources, targets = _parse_pipeline_definition(json.loads(content_raw))
            except Exception as e:
                print(f'    parse error ({item["displayName"]}): {e}')
        pipelines.append({
            'id':             item['id'],
            'name':           item['displayName'],
            'workspace_id':   ws_id,
            'workspace_name': ws_name,
            'folder_path':    folder_path,
            'description':    item.get('description', ''),
            'sources':        sources,
            'targets':        targets,
        })
        if i % 10 == 0:
            print(f'    {i}/{len(pl_items)}...')
        time.sleep(0.4)

    # ── Dataflows ──
    df_items = _get_all_pages(f'{FABRIC_API}/workspaces/{ws_id}/items?type=Dataflow', token)
    print(f'  {len(df_items)} dataflows — fetching definitions...')
    for i, item in enumerate(df_items, 1):
        fid         = item.get('folderId')
        folder_path = folder_map.get(fid, '') if fid else ''
        parts       = _get_definition(ws_id, item['id'], token)
        sources, targets = _parse_dataflow_definition(parts)
        dataflows.append({
            'id':             item['id'],
            'name':           item['displayName'],
            'workspace_id':   ws_id,
            'workspace_name': ws_name,
            'folder_path':    folder_path,
            'description':    item.get('description', ''),
            'sources':        sources,
            'targets':        targets,
        })
        time.sleep(0.4)

    pipelines.sort(key=lambda x: (x['folder_path'].lower(), x['name'].lower()))
    dataflows.sort(key=lambda x: (x['folder_path'].lower(), x['name'].lower()))
    return pipelines, dataflows


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    token = get_fabric_token()
    pipelines, dataflows = fetch_all(token)

    os.makedirs(DATA_DIR, exist_ok=True)
    pl_path = os.path.join(DATA_DIR, 'pipelines.json')
    df_path = os.path.join(DATA_DIR, 'dataflows.json')

    with open(pl_path, 'w', encoding='utf-8') as f:
        json.dump(pipelines, f, indent=2, ensure_ascii=False)
    with open(df_path, 'w', encoding='utf-8') as f:
        json.dump(dataflows, f, indent=2, ensure_ascii=False)

    print(f'Wrote {len(pipelines)} pipelines -> {pl_path}')
    print(f'Wrote {len(dataflows)} dataflows  -> {df_path}')


if __name__ == '__main__':
    main()
