"""LoadRunner blueprint — Fabric execution plan runner."""
import json
import os
import sys
import time
import threading
import uuid

from flask import (Blueprint, render_template, request, jsonify,
                   Response, stream_with_context)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from config import load_registry, DB_CONFIG
from shared.auth import get_db_token_struct, get_fabric_token

loadrunner = Blueprint('loadrunner', __name__)

# ── DB connection ─────────────────────────────────────────────────────────────

def get_connection(db_name):
    import pyodbc
    server, catalog = DB_CONFIG[db_name]
    ts  = get_db_token_struct()
    conn = pyodbc.connect(
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'SERVER={server};DATABASE={catalog};'
        f'Encrypt=yes;TrustServerCertificate=no;',
        attrs_before={1256: ts})
    conn.autocommit = True
    return conn


# ── Workspace / pipeline helpers ──────────────────────────────────────────────

def _get_workspace_id():
    from config import WORKSPACE_ID, WORKSPACE_NAME, FABRIC_API
    import requests as req
    if WORKSPACE_ID:
        return WORKSPACE_ID
    token   = get_fabric_token()
    headers = {'Authorization': 'Bearer ' + token}
    resp    = req.get(f'{FABRIC_API}/workspaces', headers=headers, timeout=30)
    resp.raise_for_status()
    for ws in resp.json().get('value', []):
        if ws['displayName'] == WORKSPACE_NAME:
            return ws['id']
    raise ValueError(f'Workspace not found: {WORKSPACE_NAME}')


_workspace_id = None
_ws_lock      = threading.Lock()


def get_workspace_id():
    global _workspace_id
    with _ws_lock:
        if _workspace_id is None:
            _workspace_id = _get_workspace_id()
    return _workspace_id


def get_pipeline_id(pipeline_name):
    import requests as req
    from config import FABRIC_API
    ws_id   = get_workspace_id()
    token   = get_fabric_token()
    headers = {'Authorization': 'Bearer ' + token}
    resp    = req.get(f'{FABRIC_API}/workspaces/{ws_id}/items',
                      headers=headers, timeout=30)
    resp.raise_for_status()
    for item in resp.json().get('value', []):
        if item['displayName'] == pipeline_name.strip():
            return item['id']
    raise ValueError(f'Pipeline not found: {pipeline_name.strip()}')


def trigger_pipeline(pipeline_name):
    import requests as req
    from config import FABRIC_API
    pipeline_id = get_pipeline_id(pipeline_name)
    ws_id       = get_workspace_id()
    token       = get_fabric_token()
    headers     = {'Authorization': 'Bearer ' + token,
                   'Content-Type': 'application/json'}
    resp = req.post(
        f'{FABRIC_API}/workspaces/{ws_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline',
        headers=headers, json={}, timeout=30)
    resp.raise_for_status()
    return resp.headers.get('Location', '')


def wait_for_pipeline(location, log_fn, timeout_minutes=10):
    import requests as req
    if not location:
        raise ValueError('No Location URL from pipeline trigger')
    start, last_status = time.time(), None
    while True:
        token   = get_fabric_token()
        headers = {'Authorization': 'Bearer ' + token}
        resp    = req.get(location, headers=headers, timeout=30)
        resp.raise_for_status()
        status  = resp.json().get('status', 'Unknown')
        if status != last_status:
            log_fn(status)
            last_status = status
        if status in ('Succeeded', 'Failed', 'Cancelled', 'Deduped', 'Completed'):
            return status
        if int(time.time() - start) > timeout_minutes * 60:
            return 'Timeout'
        time.sleep(10)


# ── Run state ─────────────────────────────────────────────────────────────────

run_events     = {}
run_status     = {}
run_locks      = {}
cancelled_runs = set()

FILTER_COLS = ['MASTER_ID', 'MASTER_NAME', 'AREA_ID', 'AREA_NAME',
               'GROUP_ID', 'GROUP_NAME', 'STEP_ID', 'EXECUTION_ID']
INT_COLS    = {'MASTER_ID', 'AREA_ID', 'GROUP_ID', 'STEP_ID', 'EXECUTION_ID'}


def coerce_val(col, val):
    return int(val) if col in INT_COLS else val


def push(run_id, event):
    run_events[run_id].append(event)


# ── Background execution ──────────────────────────────────────────────────────

def execute_run(flask_run_id, filter_col, filter_val, triggered_by, start_from=None):
    def log(msg, **kw):
        push(flask_run_id, {'type': 'log', 'msg': msg, **kw})

    try:
        ctrl_conn = get_connection('DB_BI_SUPPORT')
        ctrl_cur  = ctrl_conn.cursor()

        ctrl_cur.execute(
            "SELECT EXECUTION_ID, STEP_NAME, STEP_TYPE, STEP_DATABASE, "
            "ISNULL(NULLIF(STEP_SCHEMA,''),'dbo') AS STEP_SCHEMA, TARGET_OBJECT, "
            "FLAG_ACTIVE, FLAG_FAILURE_HALT, MAX_RETRIES, "
            "AREA_ID, AREA_NAME, GROUP_ID, GROUP_NAME, STEP_ID, "
            "MASTER_ID, MASTER_NAME "
            "FROM control.EXECUTION_PLAN "
            "WHERE " + filter_col + " = ? "
            "ORDER BY MASTER_ID, AREA_ID, GROUP_ID, STEP_ID, EXECUTION_ID",
            filter_val)
        steps = ctrl_cur.fetchall()

        if not steps:
            push(flask_run_id, {'type': 'error',
                                'msg': f'No steps for {filter_col} = {filter_val!r}'})
            push(flask_run_id, {'type': 'done', 'status': 'FAILED',
                                'success': 0, 'failed': 0, 'skipped': 0})
            return

        before = []
        if start_from:
            before = [s for s in steps if s[0] < start_from]
            steps  = [s for s in steps if s[0] >= start_from]
            if not steps:
                push(flask_run_id, {'type': 'error',
                                    'msg': f'No steps at or after EXECUTION_ID {start_from}'})
                push(flask_run_id, {'type': 'done', 'status': 'FAILED',
                                    'success': 0, 'failed': 0, 'skipped': len(before)})
                return
            if before:
                log(f'Skipping {len(before)} step(s) before Exec ID {start_from}')

        all_steps = before + steps
        push(flask_run_id, {
            'type': 'steps',
            'steps': [{'execution_id': s[0], 'step_name': s[1].strip(),
                       'step_type': s[2], 'step_database': s[3],
                       'area_id': s[9], 'area_name': s[10],
                       'group_id': s[11], 'group_name': s[12],
                       'step_id': s[13], 'master_id': s[14], 'master_name': s[15],
                       'pre_skipped': bool(start_from and s[0] < start_from)}
                      for s in all_steps]})

        db_run_id = str(uuid.uuid4())
        ctrl_cur.execute('EXEC control.usp_OpenRunLog ?, ?, ?, ?, ?',
                         db_run_id, steps[0][0], triggered_by, 0, '')
        log(f'DB Run ID: {db_run_id}')

        success = failed = skipped = 0
        halt_at  = None
        db_conns = {}

        def cached_conn(db_name):
            if db_name not in db_conns or db_conns[db_name] is None:
                db_conns[db_name] = get_connection(db_name)
            return db_conns[db_name]

        for step in steps:
            exec_id, step_name, step_type, step_db, step_schema, \
                target_obj, flag_active, halt_on_fail, max_retries = step[:9]
            step_name    = step_name.strip()
            max_attempts = max(1, max_retries + 1)
            attempt      = 1
            step_status  = None
            error_msg    = None
            step_start   = time.time()

            if flask_run_id in cancelled_runs:
                push(flask_run_id, {'type': 'halt', 'msg': 'Run cancelled by user'})
                break

            push(flask_run_id, {'type': 'step_start', 'exec_id': exec_id})

            while attempt <= max_attempts:
                ls = 'RUNNING' if attempt == 1 else 'RETRYING'
                ctrl_cur.execute('EXEC control.usp_OpenStepLog ?, ?, ?, ?',
                                 db_run_id, exec_id, attempt, ls)
                step_start = time.time()
                try:
                    if step_type == 'SP':
                        c   = cached_conn(step_db)
                        cur = c.cursor()
                        cur.execute(f'EXEC [{step_schema}].[{step_name}]')
                        cur.close()
                        step_status = 'SUCCESS'
                        error_msg   = None
                        break
                    elif step_type in ('PL', 'DF'):
                        location   = trigger_pipeline(step_name)
                        final      = wait_for_pipeline(
                            location,
                            lambda s, eid=exec_id: push(flask_run_id,
                                {'type': 'pipeline', 'exec_id': eid, 'msg': s}))
                        step_status = 'SUCCESS' if final in ('Succeeded', 'Completed') else 'FAILED'
                        error_msg   = None if step_status == 'SUCCESS' else f'Pipeline ended: {final}'
                        if step_status == 'SUCCESS':
                            break
                    elif step_type == 'N/A':
                        step_status = 'SKIPPED'
                        error_msg   = None
                        break
                    else:
                        step_status = 'SKIPPED'
                        error_msg   = f'Unknown STEP_TYPE: {step_type}'
                        break
                except Exception as e:
                    step_status = 'FAILED'
                    error_msg   = str(e)[:4000]
                    if step_type == 'SP' and step_db in db_conns:
                        try:   db_conns[step_db].close()
                        except: pass
                        db_conns[step_db] = None

                if step_status == 'FAILED' and attempt < max_attempts:
                    ctrl_cur.execute('EXEC control.usp_CloseStepLog ?, ?, ?, ?, ?, ?, ?',
                                     db_run_id, exec_id, attempt, 'FAILED', None, error_msg, None)
                    attempt += 1
                    push(flask_run_id, {'type': 'retry', 'exec_id': exec_id,
                                        'attempt': attempt})
                    time.sleep(5)
                else:
                    break

            duration = int(time.time() - step_start)
            ctrl_cur.execute('EXEC control.usp_CloseStepLog ?, ?, ?, ?, ?, ?, ?',
                             db_run_id, exec_id, attempt, step_status, None, error_msg, None)
            ctrl_cur.execute('EXEC control.usp_UpdatePlanLastRun ?, ?, ?, ?',
                             db_run_id, exec_id, step_status, duration)

            push(flask_run_id, {'type': 'step_done', 'exec_id': exec_id,
                                 'status': step_status, 'duration': duration,
                                 'error': error_msg})

            if step_status == 'SUCCESS':   success += 1
            elif step_status == 'FAILED':
                failed += 1
                if halt_on_fail:
                    halt_at = exec_id
                    push(flask_run_id, {'type': 'halt',
                                        'msg': f'Halting after step {exec_id}'})
                    break
            else:
                skipped += 1

        if halt_at:
            for s in [s for s in steps if s[0] > halt_at]:
                ctrl_cur.execute('EXEC control.usp_OpenStepLog ?, ?, ?, ?',
                                 db_run_id, s[0], 1, 'SKIPPED')
                ctrl_cur.execute('EXEC control.usp_CloseStepLog ?, ?, ?, ?, ?, ?, ?',
                                 db_run_id, s[0], 1, 'SKIPPED', None,
                                 'Skipped due to upstream failure', None)
                skipped += 1

        for c in db_conns.values():
            try:
                if c: c.close()
            except: pass

        final_status = 'HALTED' if halt_at else ('FAILED' if failed > 0 else 'SUCCESS')
        ctrl_cur.execute('EXEC control.usp_CloseRunLog ?, ?, ?, ?, ?, ?, ?',
                         db_run_id, steps[-1][0], success, failed, skipped,
                         final_status, halt_at)
        ctrl_conn.close()

        run_status[flask_run_id] = final_status
        push(flask_run_id, {'type': 'done', 'status': final_status,
                             'success': success, 'failed': failed, 'skipped': skipped})

    except Exception as e:
        run_status[flask_run_id] = 'ERROR'
        push(flask_run_id, {'type': 'error', 'msg': str(e)})
        push(flask_run_id, {'type': 'done', 'status': 'ERROR',
                             'success': 0, 'failed': 0, 'skipped': 0})


# ── Routes ────────────────────────────────────────────────────────────────────

def _registry():
    return load_registry()


FABRIC_DATABASES = list(DB_CONFIG.keys())


@loadrunner.route('/')
def index():
    return render_template('loadrunner/index.html',
                           registry=_registry(), current_app_id='loadrunner',
                           page_title='LoadRunner',
                           filter_cols=FILTER_COLS)


@loadrunner.route('/api/run-history')
def run_history():
    try:
        conn = get_connection('DB_BI_SUPPORT')
        cur  = conn.cursor()
        cur.execute("SELECT TOP 25 RUN_ID, TRIGGERED_BY, RUN_STATUS, "
                    "RUN_START_DATETIME, STEPS_SUCCESS, STEPS_FAILED "
                    "FROM control.EXECUTION_RUN_LOG "
                    "ORDER BY RUN_START_DATETIME DESC")
        cols = [d[0] for d in cur.description]
        runs = []
        for row in cur.fetchall():
            d = dict(zip(cols, row))
            if d.get('RUN_START_DATETIME'):
                d['RUN_START_DATETIME'] = d['RUN_START_DATETIME'].strftime('%m/%d %H:%M')
            runs.append(d)
        conn.close()
        return jsonify({'runs': runs})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@loadrunner.route('/api/preview')
def preview():
    col = request.args.get('filter_col', '')
    val = request.args.get('filter_val', '')
    if col not in FILTER_COLS:
        return jsonify({'error': 'Invalid filter column'}), 400
    try:
        val = coerce_val(col, val)
    except ValueError:
        return jsonify({'error': f'{col} must be a number'}), 400
    try:
        conn = get_connection('DB_BI_SUPPORT')
        cur  = conn.cursor()
        cur.execute(
            "SELECT EXECUTION_ID, STEP_NAME, STEP_TYPE, STEP_DATABASE, "
            "ISNULL(NULLIF(STEP_SCHEMA,''),'dbo') AS STEP_SCHEMA, "
            "FLAG_FAILURE_HALT, MAX_RETRIES, "
            "AREA_ID, AREA_NAME, GROUP_ID, GROUP_NAME, STEP_ID, "
            "MASTER_ID, MASTER_NAME "
            "FROM control.EXECUTION_PLAN "
            "WHERE " + col + " = ? "
            "ORDER BY MASTER_ID, AREA_ID, GROUP_ID, STEP_ID, EXECUTION_ID", val)
        steps = [
            {'execution_id': r[0], 'step_name': r[1], 'step_type': r[2],
             'step_database': r[3], 'flag_failure_halt': r[5], 'max_retries': r[6],
             'area_id': r[7], 'area_name': r[8], 'group_id': r[9], 'group_name': r[10],
             'step_id': r[11], 'master_id': r[12], 'master_name': r[13]}
            for r in cur.fetchall()]
        conn.close()
        return jsonify({'steps': steps})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@loadrunner.route('/api/filter-values')
def filter_values():
    col      = request.args.get('filter_col', '')
    TEXT_COLS = {'MASTER_NAME', 'AREA_NAME', 'GROUP_NAME'}
    if col not in TEXT_COLS:
        return jsonify({'values': []})
    try:
        conn = get_connection('DB_BI_SUPPORT')
        cur  = conn.cursor()
        cur.execute(f"SELECT DISTINCT [{col}] FROM control.EXECUTION_PLAN "
                    f"WHERE [{col}] IS NOT NULL ORDER BY [{col}]")
        values = [r[0] for r in cur.fetchall() if r[0]]
        conn.close()
        return jsonify({'values': values})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@loadrunner.route('/api/all-steps')
def all_steps():
    try:
        conn = get_connection('DB_BI_SUPPORT')
        cur  = conn.cursor()
        cur.execute(
            "SELECT EXECUTION_ID, STEP_NAME, STEP_TYPE, STEP_DATABASE, "
            "ISNULL(NULLIF(STEP_SCHEMA,''),'dbo') AS STEP_SCHEMA, "
            "FLAG_FAILURE_HALT, MAX_RETRIES, "
            "AREA_ID, AREA_NAME, GROUP_ID, GROUP_NAME, STEP_ID, FLAG_ACTIVE, "
            "MASTER_ID, MASTER_NAME "
            "FROM control.EXECUTION_PLAN "
            "ORDER BY MASTER_ID, AREA_ID, GROUP_ID, STEP_ID, EXECUTION_ID")
        steps = [
            {'execution_id': r[0], 'step_name': r[1], 'step_type': r[2],
             'step_database': r[3], 'flag_failure_halt': r[5], 'max_retries': r[6],
             'area_id': r[7], 'area_name': r[8], 'group_id': r[9], 'group_name': r[10],
             'step_id': r[11], 'flag_active': r[12], 'master_id': r[13], 'master_name': r[14]}
            for r in cur.fetchall()]
        conn.close()
        return jsonify({'steps': steps, 'total': len(steps)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@loadrunner.route('/api/run', methods=['POST'])
def start_run():
    data         = request.json or {}
    filter_col   = data.get('filter_col', '')
    filter_val   = data.get('filter_val', '')
    triggered_by = data.get('triggered_by', 'loadrunner') or 'loadrunner'
    start_from   = data.get('start_from')

    if filter_col not in FILTER_COLS:
        return jsonify({'error': 'Invalid filter column'}), 400
    try:
        filter_val = coerce_val(filter_col, filter_val)
    except ValueError:
        return jsonify({'error': f'{filter_col} must be a number'}), 400

    flask_run_id             = str(uuid.uuid4())
    run_events[flask_run_id] = []
    run_status[flask_run_id] = 'RUNNING'
    run_locks[flask_run_id]  = threading.Lock()

    t = threading.Thread(
        target=execute_run,
        args=(flask_run_id, filter_col, filter_val, triggered_by, start_from),
        daemon=True)
    t.start()
    return jsonify({'run_id': flask_run_id})


@loadrunner.route('/api/run/<run_id>/cancel', methods=['POST'])
def cancel_run(run_id):
    cancelled_runs.add(run_id)
    return jsonify({'cancelled': True})


@loadrunner.route('/run/<run_id>')
def run_detail(run_id):
    return render_template('loadrunner/run.html',
                           registry=_registry(), current_app_id='loadrunner',
                           page_title=f'Run {run_id[:8]}',
                           run_id=run_id)


@loadrunner.route('/run/<run_id>/stream')
def stream_run(run_id):
    cursor = int(request.args.get('from', 0))

    def generate():
        pos = cursor
        while True:
            events = run_events.get(run_id)
            if events is None:
                yield 'data: ' + json.dumps({'type': 'error',
                                             'msg': 'Run not found'}) + '\n\n'
                return
            while pos < len(events):
                yield 'data: ' + json.dumps(events[pos]) + '\n\n'
                pos += 1
            if run_status.get(run_id) not in ('RUNNING', None):
                return
            time.sleep(0.5)

    return Response(stream_with_context(generate()),
                    mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache',
                             'X-Accel-Buffering': 'no'})


@loadrunner.route('/tools')
def tools_page():
    return render_template('loadrunner/tools.html',
                           registry=_registry(), current_app_id='loadrunner',
                           page_title='Tools',
                           fabric_databases=FABRIC_DATABASES)


@loadrunner.route('/api/tools/fabric/schemas')
def fabric_schemas():
    db_name = request.args.get('db', '')
    if db_name not in DB_CONFIG:
        return jsonify({'error': 'Unknown database'}), 400
    try:
        conn = get_connection(db_name)
        cur  = conn.cursor()
        cur.execute("SELECT DISTINCT TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES "
                    "ORDER BY TABLE_SCHEMA")
        schemas = [r[0] for r in cur.fetchall()]
        conn.close()
        return jsonify({'schemas': schemas})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@loadrunner.route('/api/tools/fabric/tables')
def fabric_tables():
    db_name = request.args.get('db', '')
    schema  = request.args.get('schema', '')
    if db_name not in DB_CONFIG:
        return jsonify({'error': 'Unknown database'}), 400
    try:
        conn = get_connection(db_name)
        cur  = conn.cursor()
        cur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                    "WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME", schema)
        tables = [r[0] for r in cur.fetchall()]
        conn.close()
        return jsonify({'tables': tables})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
