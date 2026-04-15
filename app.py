"""
Fabric Administration Hub
Entry point — registers all blueprints and starts the server.
"""
import os
import sys

from flask import Flask, redirect, url_for, request

from config import load_registry, BASE_DIR

app = Flask(__name__, template_folder='templates', static_folder='static')
app.secret_key = os.environ.get('SECRET_KEY', 'fabric-admin-dev-key')
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0   # never cache static files


@app.after_request
def no_cache_static(response):
    if '/static/' in (request.path or ''):
        response.headers['Cache-Control'] = 'no-store'
        response.headers['Pragma']        = 'no-cache'
    return response


# ── Context processor ─────────────────────────────────────────────────────────

@app.context_processor
def inject_registry():
    return {'registry': load_registry()}


# ── Blueprint registration ────────────────────────────────────────────────────

def register_blueprints():
    """Register all active blueprints from the app registry."""
    registry = load_registry()
    module_map = {
        'hub':         ('blueprints.hub',         'hub'),
        'datatrace':   ('blueprints.datatrace',   'datatrace'),
        'loadrunner':  ('blueprints.loadrunner',  'loadrunner'),
        'syncbuilder': ('blueprints.syncbuilder', 'syncbuilder'),
    }

    # Hub is always registered at /
    from blueprints.hub import hub
    app.register_blueprint(hub, url_prefix='/')

    for entry in registry:
        app_id = entry.get('id')
        if not entry.get('active', True):
            continue
        if app_id == 'hub':
            continue  # already registered
        if app_id in module_map:
            mod_name, bp_name = module_map[app_id]
            mod = __import__(mod_name, fromlist=[bp_name])
            bp  = getattr(mod, bp_name)
            app.register_blueprint(bp, url_prefix=entry.get('url_prefix', f'/{app_id}'))


register_blueprints()


# ── Root redirect ─────────────────────────────────────────────────────────────

@app.route('/favicon.ico')
def favicon():
    return app.send_static_file('favicon.ico') if os.path.exists(
        os.path.join(BASE_DIR, 'static', 'favicon.ico')) else ('', 204)


# ── Dev / prod server ─────────────────────────────────────────────────────────

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Fabric Administration Hub')
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--port', type=int, default=5000)
    parser.add_argument('--dev', action='store_true', help='Run Flask dev server')
    args = parser.parse_args()

    if args.dev:
        app.run(host=args.host, port=args.port, debug=True, threaded=True)
    else:
        from waitress import serve
        print(f'Fabric Administration Hub running on http://{args.host}:{args.port}')
        serve(app, host=args.host, port=args.port, threads=8)
