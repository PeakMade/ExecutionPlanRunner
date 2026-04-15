"""DataTrace blueprint — SP catalog and data flow visualizer."""
import json
import os
import re
import shutil
import subprocess
import sys
import threading
from datetime import datetime
from flask import Blueprint, render_template, request, jsonify

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from config import load_registry

datatrace = Blueprint('datatrace', __name__)

_DATA_DIR     = os.path.join(os.path.dirname(__file__), '..', 'apps', 'datatrace', 'data')
DATA_PATH     = os.path.join(_DATA_DIR, 'processes.json')
PIPELINES_PATH = os.path.join(_DATA_DIR, 'pipelines.json')
DATAFLOWS_PATH = os.path.join(_DATA_DIR, 'dataflows.json')
DWHDOCS_DIR   = r'C:\DwhLoaderDocs'
CATALOG_SRC   = os.path.join(DWHDOCS_DIR, 'data', 'processes.json')
FETCH_FABRIC_SCRIPT = os.path.join(
    os.path.dirname(__file__), '..', 'apps', 'datatrace', 'scripts', 'fetch_fabric_items.py')

# ── Catalog refresh state ─────────────────────────────────────────────────────
_refresh: dict = {'status': 'idle', 'message': '', 'started_at': None, 'finished_at': None}
_refresh_lock = threading.Lock()


def _run_catalog_refresh():
    global _refresh
    with _refresh_lock:
        _refresh = {'status': 'running', 'message': 'Fetching SP definitions from databases…',
                    'started_at': datetime.now().isoformat(), 'finished_at': None}
    try:
        # Step 1 — fetch latest SP SQL from all warehouses
        r1 = subprocess.run(
            [sys.executable, 'fetch_sps.py'],
            cwd=DWHDOCS_DIR, capture_output=True, text=True, timeout=300)
        if r1.returncode != 0:
            raise RuntimeError(r1.stderr[-400:] or 'fetch_sps failed')

        with _refresh_lock:
            _refresh['message'] = 'Parsing and building SP catalog…'

        # Step 2 — rebuild processes.json
        r2 = subprocess.run(
            [sys.executable, 'scripts/build_catalog.py'],
            cwd=DWHDOCS_DIR, capture_output=True, text=True, timeout=60)
        if r2.returncode != 0:
            raise RuntimeError(r2.stderr[-400:] or 'build_catalog failed')

        # Step 3 — deploy SP catalog to the app
        shutil.copy2(CATALOG_SRC, DATA_PATH)

        # Count processes from output
        import re as _re
        m = _re.search(r'Wrote (\d+)', r2.stdout)
        sp_count = m.group(1) if m else '?'

        # Step 4 — fetch all Fabric pipelines and dataflows from all workspaces
        with _refresh_lock:
            _refresh['message'] = 'Fetching Fabric pipelines and dataflows…'

        r4 = subprocess.run(
            [sys.executable, os.path.abspath(FETCH_FABRIC_SCRIPT)],
            cwd=os.path.abspath(os.path.join(os.path.dirname(__file__), '..')),
            capture_output=True, text=True, timeout=120)
        if r4.returncode != 0:
            raise RuntimeError(r4.stderr[-400:] or 'fetch_fabric_items failed')

        pl_m = _re.search(r'Wrote (\d+) pipelines', r4.stdout)
        df_m = _re.search(r'Wrote (\d+) dataflows', r4.stdout)
        pl_count = pl_m.group(1) if pl_m else '?'
        df_count = df_m.group(1) if df_m else '?'

        with _refresh_lock:
            _refresh.update({'status': 'done',
                             'message': (f'Catalog updated — {sp_count} SPs, '
                                         f'{pl_count} pipelines, {df_count} dataflows loaded.'),
                             'finished_at': datetime.now().isoformat()})
    except Exception as e:
        with _refresh_lock:
            _refresh.update({'status': 'error',
                             'message': str(e)[:300],
                             'finished_at': datetime.now().isoformat()})

# ── Exec plan cache ───────────────────────────────────────────────────────────
_exec_plan: dict = {}
_exec_plan_loaded_at: datetime | None = None


def _fmt_duration(seconds) -> str:
    if seconds is None:
        return '—'
    s = int(seconds)
    if s < 60:
        return f'{s}s'
    m, s = divmod(s, 60)
    if m < 60:
        return f'{m}m {s}s'
    h, m = divmod(m, 60)
    return f'{h}h {m}m {s}s'


def _fetch_exec_plan() -> dict:
    try:
        from shared.helpers import get_fabric_connection
        conn   = get_fabric_connection('WH_STAGING')
        cursor = conn.cursor()
        cursor.execute("""
            SELECT STEP_NAME, LAST_RUN_DATETIME, LAST_RUN_STATUS, LAST_RUN_DURATION_S
            FROM DB_BI_Support.control.EXECUTION_PLAN
            WHERE LAST_RUN_DATETIME IS NOT NULL
        """)
        rows = cursor.fetchall()
        conn.close()
        result: dict = {}
        for step_name, dt, status, duration_s in rows:
            existing = result.get(step_name)
            if existing is None or (dt and dt > existing['last_run_dt']):
                result[step_name] = {
                    'last_run_dt':       dt,
                    'last_run_date':     dt.strftime('%Y-%m-%d %H:%M') if dt else '—',
                    'last_run_status':   status or '—',
                    'last_run_duration': _fmt_duration(duration_s),
                }
        return result
    except Exception as e:
        print(f'[datatrace] exec_plan fetch failed: {e}')
        return {}


def get_exec_plan() -> dict:
    global _exec_plan, _exec_plan_loaded_at
    if not _exec_plan_loaded_at:
        _exec_plan = _fetch_exec_plan()
        _exec_plan_loaded_at = datetime.now()
    return _exec_plan


def _fetch_exec_plan_entries(step_name: str) -> list:
    """Return all EXECUTION_PLAN rows where STEP_NAME matches, with formatted fields."""
    try:
        from shared.helpers import get_fabric_connection
        conn   = get_fabric_connection('WH_STAGING')
        cursor = conn.cursor()
        cursor.execute("""
            SELECT EXECUTION_ID, MASTER_NAME, AREA_NAME, GROUP_NAME,
                   STEP_ID, STEP_TYPE, FLAG_ACTIVE,
                   LAST_RUN_ID, LAST_RUN_DATETIME, LAST_RUN_STATUS, LAST_RUN_DURATION_S
            FROM DB_BI_Support.control.EXECUTION_PLAN
            WHERE STEP_NAME = ?
            ORDER BY MASTER_NAME, AREA_NAME, GROUP_NAME, STEP_ID
        """, step_name)
        rows = []
        for (exec_id, master, area, group, step_id, step_type, active,
             run_id, run_dt, status, dur_s) in cursor.fetchall():
            rows.append({
                'execution_id':   exec_id,
                'master_name':    master or '—',
                'area_name':      area   or '—',
                'group_name':     group  or '—',
                'step_id':        step_id,
                'step_type':      step_type or '—',
                'active':         bool(active),
                'last_run_id':    str(run_id) if run_id else '—',
                'last_run_date':  run_dt.strftime('%Y-%m-%d %H:%M') if run_dt else '—',
                'last_run_status': status or '—',
                'last_run_duration': _fmt_duration(dur_s),
            })
        conn.close()
        return rows
    except Exception as e:
        print(f'[datatrace] exec_plan_entries fetch failed: {e}')
        return []


def load_processes():
    with open(DATA_PATH, encoding='utf-8') as f:
        return json.load(f)


def _registry():
    return load_registry()


def mlabel(text: str) -> str:
    return (text.replace('"', "'").replace(';', ',').replace('#', ''))


def _is_staging(name: str) -> bool:
    """Return True for intermediate/staging tables that should be grouped visually."""
    low = name.lower()
    return '.xtemp.' in low or low.startswith('xtemp.')


def build_mermaid(proc: dict) -> str:
    lines  = ['flowchart LR']
    name   = proc['name']
    sp_nid = re.sub(r'\W', '_', name)

    # Sources
    for i, src in enumerate(proc['sources']):
        src_id = re.sub(r'\W', '_', src) + f'_s{i}'
        lines.append(f'    {src_id}["{mlabel(src)}"]')
        lines.append(f'    style {src_id} fill:#1e3a5f,stroke:#4f6ef7,color:#a5b4fc,cursor:pointer')
        lines.append(f'    click {src_id} "/dwh/table/{src}" _self')
        lines.append(f'    {src_id} --> {sp_nid}')

    # SP node
    color = 'fill:#0284c7' if proc['cross_db'] else 'fill:#1d4ed8'
    lines.append(f'    {sp_nid}["{mlabel(name)}\\n{mlabel(proc["load_strategy"])}"]')
    lines.append(f'    style {sp_nid} {color},color:#fff,stroke:#333')

    # Split targets: staging vs real
    staging_tgts = [(i, t) for i, t in enumerate(proc['targets']) if _is_staging(t)]
    real_tgts    = [(i, t) for i, t in enumerate(proc['targets']) if not _is_staging(t)]

    # Real targets as normal nodes
    for i, tgt in real_tgts:
        tgt_id = re.sub(r'\W', '_', tgt) + f'_t{i}'
        lines.append(f'    {tgt_id}["{mlabel(tgt)}"]')
        lines.append(f'    style {tgt_id} fill:#1e3a5f,stroke:#4f6ef7,color:#a5b4fc,cursor:pointer')
        lines.append(f'    click {tgt_id} "/dwh/table/{tgt}" _self')
        lines.append(f'    {sp_nid} --> {tgt_id}')

    # Staging targets: collapsed into a subgraph so they stack vertically
    if staging_tgts:
        lines.append(f'    subgraph xtemp_out [" Staging · xtemp ({len(staging_tgts)}) "]')
        lines.append(f'      direction TB')
        for i, tgt in staging_tgts:
            tgt_id = re.sub(r'\W', '_', tgt) + f'_t{i}'
            short  = mlabel(tgt.rsplit('.', 1)[-1])
            lines.append(f'      {tgt_id}["{short}"]')
            lines.append(f'      style {tgt_id} fill:#1c2a1e,stroke:#4ade80,color:#86efac,cursor:pointer')
        lines.append(f'    end')
        for i, tgt in staging_tgts:
            tgt_id = re.sub(r'\W', '_', tgt) + f'_t{i}'
            lines.append(f'    {sp_nid} --> {tgt_id}')

    return '\n'.join(lines)


def _topo_levels(nodes: list, edges: list) -> dict:
    """Longest-path topological level for each node (= column in LR layout)."""
    from collections import defaultdict, deque
    out_adj = defaultdict(list)
    in_deg  = {n['id']: 0 for n in nodes}
    levels  = {n['id']: 0 for n in nodes}
    for e in edges:
        out_adj[e['from']].append(e['to'])
        in_deg[e['to']] = in_deg.get(e['to'], 0) + 1
    queue = deque(nid for nid, d in in_deg.items() if d == 0)
    while queue:
        nid = queue.popleft()
        for child in out_adj[nid]:
            new = levels[nid] + 1
            if new > levels.get(child, 0):
                levels[child] = new
            in_deg[child] -= 1
            if in_deg[child] == 0:
                queue.append(child)
    return levels


def build_chain_graph(processes: list) -> tuple[dict, list]:
    """Return vis.js-compatible {nodes, edges} for the chain view.

    The hierarchical layout engine will automatically place nodes in columns
    by topological depth:  source tables → prep SPs → staging tables → main
    SPs → final target tables.
    """
    nodes_out    = []
    edges_out    = []
    added        = set()
    search_index = []
    eid          = 0

    def tid(t): return 'T_'  + re.sub(r'\W', '_', t)
    def sid(n): return 'SP_' + re.sub(r'\W', '_', n)

    def _tbl_node(full_name: str) -> dict:
        xtemp = _is_staging(full_name)
        label = full_name.rsplit('.', 1)[-1] if xtemp else full_name
        bg    = '#1c2a1e' if xtemp else '#0f1f35'
        bdr   = '#4ade80' if xtemp else '#4f6ef7'
        bdr_h = '#86efac' if xtemp else '#818cf8'
        fc    = '#86efac' if xtemp else '#a5b4fc'
        return {
            'id':    tid(full_name),
            'label': label,
            'title': full_name,
            'color': {'background': bg,  'border': bdr,
                      'highlight':  {'background': bg, 'border': bdr_h},
                      'hover':      {'background': bg, 'border': bdr_h}},
            'font':  {'color': fc, 'size': 10},
        }

    for proc in processes:
        nid   = sid(proc['name'])
        color = '#0284c7' if proc['cross_db'] else '#1d4ed8'
        nodes_out.append({
            'id':    nid,
            'label': proc['name'] + '\n' + proc['load_strategy'],
            'title': proc['name'],
            'color': {'background': color, 'border': '#6366f1',
                      'highlight':  {'background': color, 'border': '#a78bfa'},
                      'hover':      {'background': color, 'border': '#a78bfa'}},
            'font':  {'color': '#ffffff', 'size': 11},
        })
        search_index.append({'label': proc['name'], 'node_id': nid})

        for src in proc['sources']:
            t = tid(src)
            if t not in added:
                added.add(t)
                nodes_out.append(_tbl_node(src))
                search_index.append({'label': src, 'node_id': t})
            edges_out.append({'id': eid, 'from': t, 'to': nid}); eid += 1

        for tgt in proc['targets']:
            t = tid(tgt)
            if t not in added:
                added.add(t)
                nodes_out.append(_tbl_node(tgt))
                search_index.append({'label': tgt, 'node_id': t})
            edges_out.append({'id': eid, 'from': nid, 'to': t}); eid += 1

    levels = _topo_levels(nodes_out, edges_out)

    # Post-process: when an SP writes to BOTH xtemp/interim tables AND real tables,
    # push the real tables one extra level past the xtemp ones so they appear as a
    # separate column further right instead of stacking in the same column.
    for proc in processes:
        sp_id  = sid(proc['name'])
        sp_lvl = levels.get(sp_id, 0)
        tgts   = proc.get('targets', [])
        if any(_is_staging(t) for t in tgts) and any(not _is_staging(t) for t in tgts):
            for t in tgts:
                if not _is_staging(t):
                    nid = tid(t)
                    levels[nid] = max(levels.get(nid, 0), sp_lvl + 2)

    for node in nodes_out:
        node['level'] = levels.get(node['id'], 0)

    return {'nodes': nodes_out, 'edges': edges_out}, search_index


# ── Routes ────────────────────────────────────────────────────────────────────

@datatrace.route('/refresh-catalog', methods=['POST'])
def refresh_catalog():
    with _refresh_lock:
        if _refresh['status'] == 'running':
            return jsonify({'ok': False, 'message': 'Refresh already in progress.'})
    t = threading.Thread(target=_run_catalog_refresh, daemon=True)
    t.start()
    return jsonify({'ok': True, 'message': 'Catalog refresh started.'})


@datatrace.route('/refresh-catalog/status')
def refresh_catalog_status():
    with _refresh_lock:
        return jsonify(dict(_refresh))


@datatrace.route('/refresh-exec-plan')
def refresh_exec_plan():
    global _exec_plan, _exec_plan_loaded_at
    _exec_plan           = _fetch_exec_plan()
    _exec_plan_loaded_at = datetime.now()
    return jsonify({'ok': True, 'loaded_at': _exec_plan_loaded_at.isoformat(),
                    'count': len(_exec_plan)})


@datatrace.route('/')
def index():
    processes  = load_processes()
    exec_plan  = get_exec_plan()
    q          = request.args.get('q', '').strip().lower()
    ptype      = request.args.get('type', '').strip()
    pwarehouse = request.args.get('warehouse', '').strip()
    pstrategy  = request.args.get('strategy', '').strip()
    ptable     = request.args.get('table', '').strip().lower()
    prole      = request.args.get('role', '').strip()
    cross      = request.args.get('cross_db', '').strip()

    filtered = processes
    if q:
        filtered = [p for p in filtered if q in p['name'].lower()
                    or q in p['description'].lower()
                    or any(q in s.lower() for s in p['sources'])
                    or any(q in t.lower() for t in p['targets'])]
    if ptype:      filtered = [p for p in filtered if p['type'] == ptype]
    if pwarehouse: filtered = [p for p in filtered if p['warehouse'] == pwarehouse]
    if pstrategy:  filtered = [p for p in filtered if p['load_strategy'] == pstrategy]
    if ptable:
        filtered = [p for p in filtered if
                    any(ptable in s.lower() for s in p['sources']) or
                    any(ptable in t.lower() for t in p['targets'])]
    if prole == 'Source':  filtered = [p for p in filtered if p.get('sources')]
    elif prole == 'Target': filtered = [p for p in filtered if p.get('targets')]
    if cross == '1':       filtered = [p for p in filtered if p['cross_db']]

    no_run = {'last_run_date': '—', 'last_run_status': '—', 'last_run_duration': '—'}
    for p in filtered:
        ep = exec_plan.get(p['name'], no_run)
        p['last_run_date']     = ep['last_run_date']
        p['last_run_status']   = ep['last_run_status']
        p['last_run_duration'] = ep['last_run_duration']

    types      = sorted({p['type']         for p in processes})
    warehouses = sorted({p['warehouse']    for p in processes})
    strategies = sorted({p['load_strategy'] for p in processes})
    view       = request.args.get('view', 'sp')

    flat_rows = []
    for p in filtered:
        base = dict(name=p['name'], warehouse=p['warehouse'], type=p['type'],
                    load_strategy=p['load_strategy'], cross_db=p['cross_db'],
                    last_run_date=p['last_run_date'], last_run_status=p['last_run_status'],
                    last_run_duration=p['last_run_duration'])
        sources = p.get('sources', [])
        targets = p.get('targets', [])
        if not sources and not targets:
            flat_rows.append({**base, 'table': '—', 'role': '—'})
        for s in sources:
            if not prole or prole == 'Source':
                flat_rows.append({**base, 'table': s, 'role': 'Source'})
        for t in targets:
            if not prole or prole == 'Target':
                flat_rows.append({**base, 'table': t, 'role': 'Target'})

    exec_plan_loaded_at = (_exec_plan_loaded_at.strftime('%Y-%m-%d %H:%M')
                           if _exec_plan_loaded_at else 'not loaded')

    return render_template('datatrace/index.html',
                           registry=_registry(), current_app_id='datatrace',
                           page_title='DataTrace',
                           processes=filtered, flat_rows=flat_rows,
                           types=types, warehouses=warehouses, strategies=strategies,
                           q=q, ptype=ptype, pwarehouse=pwarehouse, pstrategy=pstrategy,
                           ptable=ptable, prole=prole, cross=cross, view=view,
                           exec_plan_loaded_at=exec_plan_loaded_at,
                           total=len(processes), shown=len(filtered))


@datatrace.route('/process/<name>')
def detail(name):
    processes = load_processes()
    proc      = next((p for p in processes if p['name'] == name), None)
    if proc is None:
        return 'Not found', 404
    chart      = build_mermaid(proc)
    transforms = []
    for tx in proc.get('transformations', []):
        if ': ' in tx:
            field, expr = tx.split(': ', 1)
        else:
            field, expr = tx, ''
        transforms.append({'field': field, 'expression': expr})
    transforms.sort(key=lambda x: x['field'])
    exec_entries = _fetch_exec_plan_entries(name)
    return render_template('datatrace/detail.html',
                           registry=_registry(), current_app_id='datatrace',
                           page_title=name,
                           proc=proc, chart=chart, transforms=transforms,
                           exec_entries=exec_entries)


@datatrace.route('/chain')
def chain():
    all_processes = load_processes()
    warehouses    = sorted({p['warehouse'] for p in all_processes})
    sel_warehouse = request.args.get('warehouse', warehouses[0] if warehouses else '')
    keyword       = request.args.get('q', '').strip().lower()
    filtered      = [p for p in all_processes if p['warehouse'] == sel_warehouse]
    if keyword:
        filtered  = [p for p in filtered if p['name'].lower() == keyword]

    graph, search_index = build_chain_graph(filtered)
    proc_map            = {p['name']: {k: v for k, v in p.items() if k != 'sql'}
                           for p in filtered}
    return render_template('datatrace/chain.html',
                           registry=_registry(), current_app_id='datatrace',
                           page_title='Data Flow Chain',
                           graph=graph, count=len(filtered), total=len(all_processes),
                           warehouses=warehouses, sel_warehouse=sel_warehouse,
                           keyword=keyword, search_index=search_index, proc_map=proc_map)


@datatrace.route('/table/<path:table_name>')
def table_usage(table_name):
    processes = load_processes()
    producers = [p for p in processes if table_name in p['targets']]
    consumers = [p for p in processes if table_name in p['sources']]
    return render_template('datatrace/table.html',
                           registry=_registry(), current_app_id='datatrace',
                           page_title=table_name,
                           table_name=table_name,
                           producers=producers, consumers=consumers)


@datatrace.route('/api/processes')
def api_processes():
    processes = load_processes()
    return jsonify([{k: v for k, v in p.items() if k != 'sql'} for p in processes])


# ── Pipelines & Dataflows ─────────────────────────────────────────────────────

def _load_fabric_items(path: str) -> list:
    """Load a pipelines.json or dataflows.json catalog file, returning [] if absent."""
    try:
        with open(path, encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return []
    except Exception as e:
        print(f'[datatrace] failed to load {path}: {e}')
        return []


def _artifact_view(data_path: str, title: str, icon: str):
    """Shared logic for the Pipelines and Dataflows index pages."""
    all_items = _load_fabric_items(data_path)

    q       = request.args.get('q',      '').strip().lower()
    pfolder = request.args.get('folder', '').strip()

    filtered = all_items
    if q:
        filtered = [r for r in filtered if
                    q in r['name'].lower() or
                    q in r.get('folder_path', '').lower() or
                    q in r.get('description', '').lower()]
    if pfolder:
        filtered = [r for r in filtered if r.get('folder_path', '') == pfolder]

    folders = sorted({r.get('folder_path', '') for r in all_items if r.get('folder_path', '')})

    return render_template('datatrace/artifact_list.html',
                           registry=_registry(), current_app_id='datatrace',
                           page_title=title, icon=icon,
                           items=filtered,
                           folders=folders,
                           q=q, pfolder=pfolder,
                           total=len(all_items), shown=len(filtered))


@datatrace.route('/pipelines')
def pipelines():
    return _artifact_view(PIPELINES_PATH, 'Pipelines', 'bi-lightning-charge')


@datatrace.route('/dataflows')
def dataflows():
    return _artifact_view(DATAFLOWS_PATH, 'Gen2 Dataflows', 'bi-water')
