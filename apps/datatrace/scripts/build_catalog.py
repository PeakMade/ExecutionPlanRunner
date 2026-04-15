"""Build data/processes.json from all SQL files in sp_samples/."""
import os, json, sys

sys.path.insert(0, os.path.dirname(__file__))
from parse_sp import parse_sp

SP_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'sp_samples')
OUT    = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'processes.json')

os.makedirs(os.path.dirname(OUT), exist_ok=True)

processes = []
for fname in sorted(os.listdir(SP_DIR)):
    if fname.endswith('.sql'):
        path = os.path.join(SP_DIR, fname)
        try:
            p = parse_sp(path)
            processes.append(p)
            print(f'  parsed: {p["name"]}')
        except Exception as e:
            print(f'  ERROR {fname}: {e}')

with open(OUT, 'w', encoding='utf-8') as f:
    json.dump(processes, f, indent=2)

print(f'\nWrote {len(processes)} processes to {OUT}')
