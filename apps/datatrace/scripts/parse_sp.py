"""Parse a stored procedure SQL file into structured metadata."""
import re, json, os

# Regex patterns for table references
_TABLE_RE = re.compile(
    r'\[?(\w+)\]?\.\[?(\w+)\]?\.\[?(\w+)\]?'  # 3-part: db.schema.table
    r'|\[?(\w+)\]?\.\[?(\w+)\]?'               # 2-part: schema.table
)

HOME_DB = 'WH_STAGING'

def _normalize(ref: str, expand_home: bool = False) -> str:
    """Strip brackets. Optionally expand 2-part refs to 3-part using HOME_DB."""
    ref = ref.replace('[', '').replace(']', '')
    if expand_home:
        parts = ref.split('.')
        if len(parts) == 2:
            ref = f'{HOME_DB}.{ref}'
    return ref

def _extract_tables(sql: str, clause: str) -> list[str]:
    """Find all table references after a given clause keyword."""
    pattern = re.compile(
        rf'(?i){clause}\s+(?:TABLE\s+)?(\[?\w+\]?(?:\.\[?\w+\]?){{1,2}})',
    )
    return [_normalize(m.group(1), expand_home=True) for m in pattern.finditer(sql)]

def _extract_from_join(sql: str) -> list[str]:
    """Find all tables in FROM / JOIN clauses (excluding subqueries)."""
    pattern = re.compile(
        r'(?i)(?:FROM|JOIN)\s+(\[?\w+\]?(?:\.\[?\w+\]?){1,2})\b'
    )
    results = []
    for m in pattern.finditer(sql):
        ref = _normalize(m.group(1), expand_home=True)
        if ref.upper() not in ('SELECT', 'WITH', 'AS'):
            results.append(ref)
    return results

def detect_load_strategy(sql: str) -> str:
    sql_upper = sql.upper()
    if 'MERGE ' in sql_upper:
        return 'MERGE (upsert)'
    if 'TRUNCATE TABLE' in sql_upper and 'INSERT INTO' in sql_upper:
        return 'TRUNCATE + INSERT'
    if 'DELETE FROM' in sql_upper and 'INSERT INTO' in sql_upper:
        return 'DELETE + INSERT'
    if 'INSERT INTO' in sql_upper:
        return 'INSERT'
    return 'Unknown'

def detect_cross_db(sql: str, home_db: str = HOME_DB) -> bool:
    """Returns True if the SP reads from or writes to a database other than home_db."""
    all_refs = (
        _extract_tables(sql, 'INSERT INTO') +
        _extract_tables(sql, 'TRUNCATE') +
        _extract_tables(sql, 'MERGE') +
        _extract_from_join(sql)
    )
    for ref in all_refs:
        parts = ref.split('.')
        if len(parts) >= 3 and parts[0].upper() != home_db.upper():
            return True
    return False

def _find_matching_end(sql: str, start: int) -> int:
    """Given the position of a CASE keyword, return the index just after its matching END.
    Handles nested CASE...END blocks by counting depth."""
    depth = 0
    i = start
    upper = sql.upper()
    while i < len(upper):
        if upper[i:i+4] == 'CASE' and not upper[i-1:i].isalnum() and not upper[i+4:i+5].isalnum():
            depth += 1
            i += 4
        elif upper[i:i+3] == 'END' and not upper[i-1:i].isalnum() and not upper[i+3:i+4].isalnum():
            depth -= 1
            if depth == 0:
                return i + 3
            i += 3
        else:
            i += 1
    return -1  # unmatched

def extract_transformations(sql: str) -> list[str]:
    """Pull CASE expressions used directly as column transformations (CASE...END AS alias).
    Uses a depth-counting approach to correctly handle nested CASE blocks."""
    results = []
    upper = sql.upper()
    # Find all top-level CASE...END AS [alias] patterns
    case_re = re.compile(r'\bCASE\b', re.IGNORECASE)
    alias_re = re.compile(r'\s+AS\s+\[?(\w+)\]?', re.IGNORECASE)

    for m in case_re.finditer(sql):
        start = m.start()
        # Skip if preceded by a word char (part of another token)
        if start > 0 and (sql[start-1].isalnum() or sql[start-1] == '_'):
            continue
        end = _find_matching_end(sql, start)
        if end == -1:
            continue
        # Check what immediately follows END — must be whitespace then AS alias
        # Allow optional wrapping like ISNULL(CASE...END, default) AS alias by checking
        # the character before this CASE: if it's '(' we skip (it's wrapped, not a direct column)
        pre = sql[:start].rstrip()
        if pre.endswith('('):
            continue  # wrapped in a function call — not a direct column alias
        tail = sql[end:]
        alias_m = alias_re.match(tail)
        if not alias_m:
            continue
        alias = alias_m.group(1)
        expr = re.sub(r'\s+', ' ', sql[start:end]).strip()
        results.append(f'{alias}: {expr}')

    return results

def auto_describe(name: str, sources: list, targets: list, strategy: str, cross_db: bool) -> str:
    src_str = ', '.join(sources[:2]) if sources else 'unknown source'
    tgt_str = ', '.join(targets[:2]) if targets else 'unknown target'
    note = ' (cross-database)' if cross_db else ''
    return f'{strategy} from {src_str} into {tgt_str}{note}.'

def parse_sp(sql_path: str) -> dict:
    with open(sql_path, 'rb') as f:
        raw_bytes = f.read()
    raw_bytes = raw_bytes.replace(b'\r\r\n', b'\n').replace(b'\r\n', b'\n').replace(b'\r', b'\n')
    sql = raw_bytes.decode('utf-8', errors='replace')
    sql = re.sub(r'\n{3,}', '\n\n', sql).strip()

    # Extract warehouse from header comment if present
    warehouse = HOME_DB
    warehouse_match = re.match(r'^-- WAREHOUSE:\s*(\S+)', sql)
    if warehouse_match:
        warehouse = warehouse_match.group(1)
        sql = sql[warehouse_match.end():].lstrip('\n')

    # Filename format: WAREHOUSE__SPNAME.sql or legacy SPNAME.sql
    basename = os.path.splitext(os.path.basename(sql_path))[0]
    name = basename.split('__', 1)[-1] if '__' in basename else basename

    targets_insert  = _extract_tables(sql, 'INSERT INTO')
    targets_truncate = _extract_tables(sql, 'TRUNCATE')
    targets_merge   = _extract_tables(sql, 'MERGE')
    targets = list(dict.fromkeys(targets_insert + targets_truncate + targets_merge))

    sources_raw = _extract_from_join(sql)
    # Remove anything that's also a target (intermediate CTEs etc.)
    target_set = {t.split('.')[-1].upper() for t in targets}
    sources = list(dict.fromkeys([
        s for s in sources_raw
        if s.split('.')[-1].upper() not in target_set
    ]))

    strategy = detect_load_strategy(sql)
    cross_db = detect_cross_db(sql, home_db=warehouse)
    transformations = extract_transformations(sql)

    return {
        'name': name,
        'type': 'SP',
        'warehouse': warehouse,
        'load_strategy': strategy,
        'sources': sources,
        'targets': targets,
        'cross_db': cross_db,
        'transformations': transformations,
        'description': auto_describe(name, sources, targets, strategy, cross_db),
        'sql': sql,
    }

if __name__ == '__main__':
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else None
    if path:
        print(json.dumps(parse_sp(path), indent=2, default=str))
