"""SyncBuilder blueprint — Fabric Copy Pipeline builder and deployer."""
import sys
import os
from flask import Blueprint, render_template, request, jsonify

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from config import load_registry, DATABASES
from shared.auth import get_fabric_token

syncbuilder = Blueprint('syncbuilder', __name__)


def _registry():
    return load_registry()


def _get_token():
    return get_fabric_token()


# ── Helper: schema / table introspection ──────────────────────────────────────

def _get_schemas(db_key):
    from shared.helpers import get_fabric_connection
    db = DATABASES.get(db_key.upper())
    if not db:
        return []
    conn = get_fabric_connection(db_key.upper())
    cursor = conn.cursor()
    cursor.execute("""
        SELECT SCHEMA_NAME
        FROM INFORMATION_SCHEMA.SCHEMATA
        WHERE SCHEMA_NAME NOT IN ('sys','INFORMATION_SCHEMA','db_owner',
            'db_accessadmin','db_securityadmin','db_ddladmin',
            'db_backupoperator','db_datareader','db_datawriter','db_denydatareader',
            'db_denydatawriter','guest')
        ORDER BY SCHEMA_NAME
    """)
    schemas = [row[0] for row in cursor.fetchall()]
    conn.close()
    return schemas


def _get_tables(db_key, schema):
    from shared.helpers import get_fabric_connection
    if not db_key or not schema:
        return []
    conn = get_fabric_connection(db_key.upper())
    cursor = conn.cursor()
    cursor.execute("""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ?
          AND TABLE_TYPE IN ('BASE TABLE','VIEW')
        ORDER BY TABLE_NAME
    """, schema)
    tables = [row[0] for row in cursor.fetchall()]
    conn.close()
    return tables


# ── Routes ────────────────────────────────────────────────────────────────────

@syncbuilder.route('/')
def index():
    db_keys = list(DATABASES.keys())
    return render_template('syncbuilder/index.html',
                           registry=_registry(), current_app_id='syncbuilder',
                           page_title='SyncBuilder',
                           db_keys=db_keys)


@syncbuilder.route('/pipelines')
def pipelines():
    from apps.syncbuilder.pipeline_core import list_pipelines
    try:
        token     = _get_token()
        pipe_dict = list_pipelines(token)
        pipe_list = sorted(pipe_dict.items())
        error     = None
    except Exception as e:
        pipe_list = []
        error     = str(e)
    return render_template('syncbuilder/pipelines.html',
                           registry=_registry(), current_app_id='syncbuilder',
                           page_title='Pipelines',
                           pipelines=pipe_list, error=error)


# ── API endpoints ─────────────────────────────────────────────────────────────

@syncbuilder.route('/api/schemas')
def api_schemas():
    db_key = request.args.get('db', '').upper()
    if not db_key:
        return jsonify({'error': 'db parameter required'}), 400
    try:
        schemas = _get_schemas(db_key)
        return jsonify({'schemas': schemas})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@syncbuilder.route('/api/tables')
def api_tables():
    db_key = request.args.get('db', '').upper()
    schema = request.args.get('schema', '')
    if not db_key or not schema:
        return jsonify({'error': 'db and schema parameters required'}), 400
    try:
        tables = _get_tables(db_key, schema)
        return jsonify({'tables': tables})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@syncbuilder.route('/api/preview', methods=['POST'])
def api_preview():
    """Return the pipeline definition JSON without deploying."""
    from apps.syncbuilder.pipeline_core import (
        parse_table, build_pipeline, auto_description, get_workspace_id
    )
    data = request.get_json(force=True) or {}
    src_raw  = data.get('source', '').strip()
    tgt_raw  = data.get('target', '').strip()
    pip_name = data.get('pipeline_name', '').strip()
    if not src_raw or not tgt_raw or not pip_name:
        return jsonify({'error': 'source, target, and pipeline_name are required'}), 400
    try:
        token                          = _get_token()
        src_db, src_schema, src_table  = parse_table(src_raw)
        tgt_db, tgt_schema, tgt_table  = parse_table(tgt_raw)
        desc                           = auto_description(src_db, src_schema, src_table,
                                                          tgt_db, tgt_schema, tgt_table)
        definition = build_pipeline(token, pip_name, desc,
                                    src_db, src_schema, src_table,
                                    tgt_db, tgt_schema, tgt_table)
        return jsonify({'definition': definition, 'description': desc})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@syncbuilder.route('/api/build', methods=['POST'])
def api_build():
    """Build and deploy a pipeline to Fabric."""
    from apps.syncbuilder.pipeline_core import (
        parse_table, build_pipeline, deploy_pipeline, auto_description
    )
    data = request.get_json(force=True) or {}
    src_raw  = data.get('source', '').strip()
    tgt_raw  = data.get('target', '').strip()
    pip_name = data.get('pipeline_name', '').strip()
    if not src_raw or not tgt_raw or not pip_name:
        return jsonify({'error': 'source, target, and pipeline_name are required'}), 400
    try:
        token                          = _get_token()
        src_db, src_schema, src_table  = parse_table(src_raw)
        tgt_db, tgt_schema, tgt_table  = parse_table(tgt_raw)
        desc                           = auto_description(src_db, src_schema, src_table,
                                                          tgt_db, tgt_schema, tgt_table)
        definition = build_pipeline(token, pip_name, desc,
                                    src_db, src_schema, src_table,
                                    tgt_db, tgt_schema, tgt_table)
        result = deploy_pipeline(token, pip_name, definition)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@syncbuilder.route('/api/list')
def api_list():
    """List all pipelines in the workspace."""
    from apps.syncbuilder.pipeline_core import list_pipelines
    try:
        token     = _get_token()
        pipe_dict = list_pipelines(token)
        return jsonify({'pipelines': [{'name': k, 'id': v}
                                      for k, v in sorted(pipe_dict.items())]})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@syncbuilder.route('/api/inspect/<path:pipeline_name>')
def api_inspect(pipeline_name):
    """Return the JSON definition of an existing pipeline."""
    from apps.syncbuilder.pipeline_core import export_pipeline
    try:
        token = _get_token()
        defn  = export_pipeline(token, pipeline_name)
        return jsonify({'name': pipeline_name, 'definition': defn})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
