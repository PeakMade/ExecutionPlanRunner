"""Hub blueprint — dashboard, app registration, and settings."""
import json
import os
from flask import Blueprint, render_template, request, redirect, url_for, jsonify

from config import load_registry, save_registry

hub = Blueprint('hub', __name__)

# Available icon and gradient options surfaced in the settings UI
ICONS = [
    'bi-database-fill', 'bi-table', 'bi-diagram-3-fill', 'bi-file-earmark-code-fill',
    'bi-server', 'bi-hdd-fill', 'bi-layers-fill', 'bi-grid-fill',
    'bi-graph-up-arrow', 'bi-bar-chart-fill', 'bi-pie-chart-fill', 'bi-activity',
    'bi-shield-fill-check', 'bi-gear-fill', 'bi-search', 'bi-cloud-fill',
    'bi-bezier2', 'bi-play-circle-fill', 'bi-lightning-fill', 'bi-cpu-fill',
    'bi-braces', 'bi-clipboard-data-fill', 'bi-funnel-fill', 'bi-check2-circle',
]

GRADIENTS = [
    {'id': 'grad-blue-indigo',  'banner': 'banner-blue-indigo',  'label': 'Blue → Indigo'},
    {'id': 'grad-green-teal',   'banner': 'banner-green-teal',   'label': 'Green → Teal'},
    {'id': 'grad-purple-pink',  'banner': 'banner-purple-pink',  'label': 'Purple → Pink'},
    {'id': 'grad-amber-orange', 'banner': 'banner-amber-orange', 'label': 'Amber → Red'},
    {'id': 'grad-cyan-blue',    'banner': 'banner-cyan-blue',    'label': 'Cyan → Blue'},
    {'id': 'grad-rose-amber',   'banner': 'banner-rose-amber',   'label': 'Rose → Amber'},
    {'id': 'grad-emerald',      'banner': 'banner-emerald',      'label': 'Emerald'},
    {'id': 'grad-violet-indigo','banner': 'banner-violet-indigo','label': 'Violet → Indigo'},
    {'id': 'grad-slate',        'banner': 'banner-slate',        'label': 'Slate (neutral)'},
]


@hub.route('/')
def home():
    registry = load_registry()
    active   = [a for a in registry if a.get('active') and a.get('show_dashboard')]
    return render_template('hub/home.html',
                           registry=registry,
                           dashboard_apps=active,
                           current_app_id='hub',
                           page_title='Dashboard')


@hub.route('/register', methods=['GET', 'POST'])
def register():
    registry = load_registry()
    error    = None
    if request.method == 'POST':
        data = request.form
        app_id = data.get('id', '').strip().lower().replace(' ', '_')
        if not app_id:
            error = 'App ID is required.'
        elif any(a['id'] == app_id for a in registry):
            error = f'An app with ID "{app_id}" already exists.'
        else:
            new_app = {
                'id':               app_id,
                'name':             data.get('name', '').strip(),
                'description':      data.get('description', '').strip(),
                'url_prefix':       '/' + data.get('url_prefix', '').strip().lstrip('/'),
                'blueprint_module': data.get('blueprint_module', '').strip(),
                'blueprint_name':   app_id,
                'icon':             data.get('icon', 'bi-grid-fill'),
                'gradient':         data.get('gradient', 'grad-blue-indigo'),
                'banner':           data.get('banner', 'banner-blue-indigo'),
                'active':           True,
                'show_dashboard':   'show_dashboard' in data,
                'show_sidebar':     'show_sidebar' in data,
                'theme':            'dark',
            }
            registry.append(new_app)
            save_registry(registry)
            return redirect(url_for('hub.settings'))
    return render_template('hub/register.html',
                           registry=registry,
                           error=error,
                           icons=ICONS,
                           gradients=GRADIENTS,
                           current_app_id='hub',
                           page_title='Add New App')


@hub.route('/settings')
def settings():
    registry = load_registry()
    return render_template('hub/settings.html',
                           registry=registry,
                           current_app_id='hub',
                           page_title='Settings')


@hub.route('/settings/<app_id>', methods=['GET', 'POST'])
def app_settings(app_id):
    registry = load_registry()
    app_cfg  = next((a for a in registry if a['id'] == app_id), None)
    if app_cfg is None:
        return 'App not found', 404

    if request.method == 'POST':
        data = request.form
        app_cfg['name']           = data.get('name', app_cfg['name']).strip()
        app_cfg['description']    = data.get('description', app_cfg['description']).strip()
        app_cfg['icon']           = data.get('icon', app_cfg['icon'])
        app_cfg['gradient']       = data.get('gradient', app_cfg['gradient'])
        # Derive banner from gradient
        grad_map = {g['id']: g['banner'] for g in GRADIENTS}
        app_cfg['banner']         = grad_map.get(app_cfg['gradient'], app_cfg['banner'])
        app_cfg['theme']          = data.get('theme', 'dark')
        app_cfg['show_dashboard'] = 'show_dashboard' in data
        app_cfg['show_sidebar']   = 'show_sidebar' in data
        save_registry(registry)
        return redirect(url_for('hub.settings'))

    return render_template('hub/app_settings.html',
                           registry=registry,
                           app=app_cfg,
                           icons=ICONS,
                           gradients=GRADIENTS,
                           current_app_id='hub',
                           page_title=f'Settings — {app_cfg["name"]}')


@hub.route('/settings/<app_id>/deactivate', methods=['POST'])
def deactivate_app(app_id):
    registry = load_registry()
    for a in registry:
        if a['id'] == app_id:
            a['active'] = not a.get('active', True)
            break
    save_registry(registry)
    return redirect(url_for('hub.settings'))


@hub.route('/settings/<app_id>/remove', methods=['POST'])
def remove_app(app_id):
    registry = load_registry()
    registry = [a for a in registry if a['id'] != app_id]
    save_registry(registry)
    return redirect(url_for('hub.settings'))


@hub.route('/api/registry')
def api_registry():
    return jsonify(load_registry())
