#!/usr/bin/env python3 
"""
ClickHouse Standalone -> 2-Node Replicated Cluster — SINGLE MERGED SCRIPT
=========================================================================
Converts every MergeTree-family table and MaterializedView in a database into
its Replicated* equivalent so both nodes hold identical data and stay in sync
automatically via ClickHouse Keeper.

ONE script. Two phases. Reads ONLY from the two new nodes.
  PHASE 1  base tables -> Replicated* (rename + backfill + Keeper sync)
  PHASE 2  MaterializedViews -> replicated TO-table pattern
           (.inner storage becomes a named, replicated <mv>_data table)

SAFETY PROPERTIES (all learned the hard way on the dev run):
  * Refuses to run unless writes are stopped (--writes-are-stopped).
  * HARD BLOCK: refuses to start if the live/source host appears in the node
    list. By default it will not even accept a source host — it only ever talks
    to NODE1 and NODE2.
  * Keeper-collision guard: refuses to run if Keeper already holds replica
    metadata it didn't create (the AMI-clone-points-at-live-Keeper disaster),
    unless --i-know-keeper-is-fresh.
  * EXPLAIN AST pre-validation of every MV CREATE/INSERT BEFORE any DROP, so a
    malformed statement can never strand a dropped MV.
  * Backfill happens BEFORE the MV trigger is created (no double counting).
  * Automatic column-order fix: if an MV's declared column order doesn't match
    its SELECT output order, the column list is reordered positionally (fixes
    the popular_items Code 70 generically — no hardcoding).
  * Idempotent + re-runnable: every object starts with DROP IF EXISTS; a failure
    on one table/MV doesn't stop the others; re-running retries cleanly.

OPTIONAL RECOVERY (off by default):
  --recover-mvs-from-source  +  CH_SOURCE=<host>
  Only for the case where a previous run dropped MVs and failed to recreate
  them, leaving no definitions on the new cluster to read. This reads the
  ORIGINAL MV definitions from CH_SOURCE (READ-ONLY) and rebuilds them. You must
  pass the flag explicitly; otherwise the live node is never contacted.

USAGE
  export CH_NODE1=...  CH_NODE2=...  CH_PASSWORD=...
  export CH_DATABASE=xyz  CH_CLUSTER=xyz_cluster
  python3 ch_migrate.py --dry-run
  python3 ch_migrate.py --writes-are-stopped
"""
import os
import re
import sys
import time
import argparse
import logging
from clickhouse_driver import Client

# ───────────────────────── CONFIG ─────────────────────────
NODE1     = os.environ.get('CH_NODE1', '')
NODE2     = os.environ.get('CH_NODE2', '')
PASSWORD  = os.environ.get('CH_PASSWORD', '')
DATABASE  = os.environ.get('CH_DATABASE', '')
CLUSTER   = os.environ.get('CH_CLUSTER', '')
SOURCE    = os.environ.get('CH_SOURCE', '')          # only used with --recover-mvs-from-source

# Known live/application hosts that must NEVER be migrated against. Add any
# production/dev application node IPs here as a hard safety net.
FORBIDDEN_NODES = {
    h.strip() for h in os.environ.get('CH_FORBIDDEN', '').split(',') if h.strip()
}

# Minimum sync timeout floor (minutes) regardless of row count, for slow networks.
MIN_SYNC_MINUTES = 15
ROWS_PER_MINUTE  = 200000     # rough sync-rate assumption for timeout sizing

MV_SKIP = {'hourly_query_counts'}     # node-local system reads; never replicate
FINAL_SUPPORTED = ('ReplacingMergeTree', 'AggregatingMergeTree', 'SummingMergeTree')
ENGINE_MAP = {
    'MergeTree':            'ReplicatedMergeTree',
    'ReplacingMergeTree':   'ReplicatedReplacingMergeTree',
    'SummingMergeTree':     'ReplicatedSummingMergeTree',
    'AggregatingMergeTree': 'ReplicatedAggregatingMergeTree',
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-7s  %(message)s',
    handlers=[logging.FileHandler('/tmp/ch_migrate.log'), logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("migrate")

DRY_RUN = False
ASSUME_QUIESCED = False
_node1_replica = [None]


def get_client(host, read_only=False):
    settings = {'insert_deduplicate': 0, 'max_execution_time': 0}
    if read_only:
        settings['readonly'] = 1
    return Client(host=host, password=PASSWORD, connect_timeout=10,
                  send_receive_timeout=7200, settings=settings)


def zk_path(table):
    return f"/clickhouse/tables/{{shard}}/{DATABASE}/{table}"


# ───────────────────────── SAFETY GUARDS ─────────────────────────
def guard_nodes():
    if not NODE1 or not NODE2:
        sys.exit("ABORT: CH_NODE1 and CH_NODE2 must both be set.")
    if NODE1 == NODE2:
        sys.exit("ABORT: NODE1 == NODE2; need two distinct hosts.")
    for n in (NODE1, NODE2):
        if n in FORBIDDEN_NODES:
            sys.exit(f"ABORT: node {n} is in the forbidden/live list. This script "
                     f"must only run against the NEW cluster, never a live node.")
    if not PASSWORD:
        log.warning("CH_PASSWORD is empty; if the server requires auth this fails.")


def guard_keeper_fresh(client, override):
    if override:
        log.info("  (Keeper freshness check bypassed by flag)")
        return True
    try:
        existing = client.execute(
            f"SELECT table, zookeeper_path, total_replicas, active_replicas "
            f"FROM system.replicas WHERE database='{DATABASE}'")
    except Exception as e:
        log.info(f"  (no system.replicas rows / {e}) — fresh")
        return True
    if not existing:
        log.info("  Keeper has no existing replicas for this DB — fresh.")
        return True
    log.warning("  Keeper ALREADY contains replica metadata for this database:")
    for t, zp, tr, ar in existing:
        log.warning(f"    - {t}  path={zp}  replicas={ar}/{tr}")
    log.warning("  If this is a resume on the SAME new cluster, re-run with "
                "--i-know-keeper-is-fresh. Otherwise you may be pointed at a "
                "live Keeper — STOP and check.")
    return False


# ───────────────────────── PREFLIGHT ─────────────────────────
def preflight(keeper_override):
    log.info("=" * 70); log.info("PREFLIGHT"); log.info("=" * 70)
    ok = True
    first = None
    for label, host in [("Node1", NODE1), ("Node2", NODE2)]:
        try:
            c = get_client(host)
            if first is None:
                first = c
            ver = c.execute("SELECT version()")[0][0]
            log.info(f"  [{label}] {host} connected — ClickHouse {ver}")
            macros = {r[0]: r[1] for r in c.execute("SELECT macro, substitution FROM system.macros")}
            missing = [m for m in ('shard', 'replica') if m not in macros]
            if missing:
                log.error(f"  [{label}] MISSING macros {missing}"); ok = False
            else:
                log.info(f"  [{label}] macros = {macros}")
                if label == "Node1":
                    _node1_replica[0] = macros.get('replica')
                if label == "Node2" and macros.get('replica') == _node1_replica[0]:
                    log.error(f"  [{label}] replica macro equals Node1's "
                              f"('{macros.get('replica')}') — they MUST differ"); ok = False
            try:
                if not c.execute("SELECT * FROM system.zookeeper WHERE path='/' LIMIT 1"):
                    log.error(f"  [{label}] Keeper returned nothing"); ok = False
                else:
                    log.info(f"  [{label}] Keeper reachable")
            except Exception as e:
                log.error(f"  [{label}] Keeper UNREACHABLE: {e}"); ok = False
            nodes = c.execute(f"SELECT host_name FROM system.clusters WHERE cluster='{CLUSTER}'")
            if len(nodes) < 2:
                log.error(f"  [{label}] cluster '{CLUSTER}' < 2 nodes"); ok = False
            else:
                log.info(f"  [{label}] cluster nodes {[r[0] for r in nodes]}")
                for r in nodes:
                    if r[0] in FORBIDDEN_NODES:
                        log.error(f"  [{label}] cluster contains forbidden host {r[0]}"); ok = False
        except Exception as e:
            log.error(f"  [{label}] CONNECTION FAILED: {e}"); ok = False

    if first is not None and not guard_keeper_fresh(first, keeper_override):
        ok = False
    if not ASSUME_QUIESCED:
        log.error("  WRITE-FENCING NOT CONFIRMED. Re-run with --writes-are-stopped "
                  "once all application writers to the source DB are stopped.")
        ok = False
    if not ok:
        log.error("PREFLIGHT FAILED — aborting (nothing changed)."); sys.exit(1)
    log.info("PREFLIGHT OK\n")


# ───────────────────────── HELPERS ─────────────────────────
def engine_on(client, name):
    r = client.execute(f"SELECT engine FROM system.tables WHERE database='{DATABASE}' AND name='{name}'")
    return r[0][0] if r else None


def exists_on(client, name):
    return engine_on(client, name) is not None


def count_rows(client, name, final=False):
    fkw = "FINAL" if final else ""
    return client.execute(f"SELECT count() FROM `{DATABASE}`.`{name}` {fkw}")[0][0]


def sync_timeout(expected):
    return max(MIN_SYNC_MINUTES, expected // ROWS_PER_MINUTE)


def wait_for_sync(node2, name, expected, max_minutes, ratio):
    deadline = time.time() + max_minutes * 60
    last = -1
    while time.time() < deadline:
        time.sleep(10)
        try:
            q = node2.execute(f"SELECT count() FROM system.replication_queue "
                              f"WHERE database='{DATABASE}' AND table='{name}'")[0][0]
            c = count_rows(node2, name)
            if c != last:
                log.info(f"    [sync] {name}: rows {c}/{expected}, queue {q}"); last = c
            if q == 0 and c >= int(expected * ratio):
                return True, c
        except Exception as e:
            log.warning(f"    [sync] {e}")
    return False, last


def verify_two_replicas(name, tries=12):
    n1 = get_client(NODE1)
    for _ in range(tries):
        r = n1.execute(f"SELECT total_replicas, active_replicas FROM system.replicas "
                       f"WHERE database='{DATABASE}' AND table='{name}'")
        if r and r[0][0] >= 2 and r[0][1] >= 2:
            return True
        time.sleep(5)
    return False


# ───────────────────────── COLUMN / SELECT PARSERS ─────────────────────────
def split_top_level(s, sep=','):
    parts = []; depth = 0; cur = ''
    for ch in s:
        if ch == '(':
            depth += 1; cur += ch
        elif ch == ')':
            depth -= 1; cur += ch
        elif ch == sep and depth == 0:
            parts.append(cur); cur = ''
        else:
            cur += ch
    if cur.strip():
        parts.append(cur)
    return parts


def parse_declared_columns(cols_block):
    inner = cols_block.strip()
    if inner.startswith('('):
        inner = inner[1:]
    if inner.endswith(')'):
        inner = inner[:-1]
    out = []
    for p in split_top_level(inner):
        p = p.strip()
        m = re.match(r'`([^`]+)`\s+(.*)', p)
        if m:
            out.append((m.group(1), m.group(2).strip()))
        else:
            sp = p.split(None, 1)
            out.append((sp[0].strip('`'), sp[1] if len(sp) > 1 else ''))
    return out


def select_output_names(select_body):
    m = re.search(r'\bSELECT\b(.*)', select_body, re.IGNORECASE | re.DOTALL)
    if not m:
        return None
    body = m.group(1)
    depth = 0; from_pos = -1
    for t in re.finditer(r'\(|\)|\bFROM\b', body, re.IGNORECASE):
        g = t.group()
        if g == '(':
            depth += 1
        elif g == ')':
            depth -= 1
        elif depth == 0:
            from_pos = t.start(); break
    if from_pos == -1:
        return None
    proj = body[:from_pos]
    names = []
    for p in split_top_level(proj):
        p = p.strip()
        am = re.search(r'\bAS\s+([`\w]+)\s*$', p, re.IGNORECASE)
        if am:
            names.append(am.group(1).strip('`'))
        else:
            names.append(p.split('.')[-1].strip('`'))
    return names


def maybe_reorder_columns(name, cols_block, select_only):
    """If declared column order != SELECT output order, reorder the column list
    positionally to match the SELECT. Returns (cols_block, reordered_bool)."""
    declared = parse_declared_columns(cols_block)
    declared_names = [c[0] for c in declared]
    out_names = select_output_names(select_only)
    if not out_names or len(out_names) != len(declared_names):
        return cols_block, False
    if out_names == declared_names:
        return cols_block, False
    if set(out_names) != set(declared_names):
        # names don't line up by set — can't safely reorder; leave as-is
        log.warning(f"  [{name}] column names differ from SELECT aliases; not reordering")
        return cols_block, False
    tmap = dict(declared)
    new_block = "(" + ", ".join(f"`{n}` {tmap[n]}" for n in out_names) + ")"
    log.info(f"  [{name}] reordering columns to match SELECT output "
             f"({declared_names} -> {out_names})")
    return new_block, True


# ───────────────────────── PHASE 1: BASE TABLES ─────────────────────────
def get_base_tables(client):
    return client.execute(f"""
        SELECT name, engine, create_table_query
        FROM system.tables
        WHERE database='{DATABASE}'
          AND engine IN ('MergeTree','ReplacingMergeTree','SummingMergeTree','AggregatingMergeTree')
          AND name NOT LIKE '.inner_id.%'
          AND name NOT LIKE '%\\_rep\\_backup'
        ORDER BY total_bytes DESC
    """)


def build_replicated_ddl(name, engine, create_query):
    if engine not in ENGINE_MAP:
        return None, f"unmapped engine {engine}"
    zk = zk_path(name)
    create_query = re.sub(
        r'CREATE TABLE\s+(`?' + re.escape(DATABASE) + r'`?\.`?' + re.escape(name) + r'`?)',
        f"CREATE TABLE IF NOT EXISTS `{DATABASE}`.`{name}` ON CLUSTER {CLUSTER}",
        create_query, count=1)
    hit = [False]

    def with_args(repl):
        def fn(m):
            hit[0] = True
            inner = (m.group(1) or '').strip()
            return f"ENGINE = {repl}('{zk}', '{{replica}}'" + (f", {inner})" if inner else ")")
        return fn

    def no_args(repl):
        def fn(m):
            hit[0] = True
            return f"ENGINE = {repl}('{zk}', '{{replica}}')"
        return fn

    if engine == 'ReplacingMergeTree':
        out = re.sub(r'ENGINE\s*=\s*ReplacingMergeTree(?:\(([^)]*)\))?',
                     with_args('ReplicatedReplacingMergeTree'), create_query)
    elif engine == 'SummingMergeTree':
        out = re.sub(r'ENGINE\s*=\s*SummingMergeTree(?:\(([^)]*)\))?',
                     with_args('ReplicatedSummingMergeTree'), create_query)
    elif engine == 'MergeTree':
        out = re.sub(r'ENGINE\s*=\s*MergeTree(?:\([^)]*\))?',
                     no_args('ReplicatedMergeTree'), create_query)
    elif engine == 'AggregatingMergeTree':
        out = re.sub(r'ENGINE\s*=\s*AggregatingMergeTree(?:\([^)]*\))?',
                     no_args('ReplicatedAggregatingMergeTree'), create_query)
    else:
        return None, f"no branch for {engine}"
    if not hit[0]:
        return None, f"engine regex no-match for '{name}' — refusing non-replicated create"
    return out, None


def migrate_base_table(name, engine, create_query):
    log.info(""); log.info("-" * 70)
    log.info(f"[BASE] {name}  ({engine})"); log.info("-" * 70)
    n1 = get_client(NODE1); n2 = get_client(NODE2)
    backup = f"{name}_rep_backup"
    final = "FINAL" if engine in FINAL_SUPPORTED else ""

    ddl, err = build_replicated_ddl(name, engine, create_query)
    if err:
        log.error(f"  ABORT: {err}"); return False

    eng1 = engine_on(n1, name); eng2 = engine_on(n2, name)
    backup_exists = exists_on(n1, backup)

    if eng1 and eng1.startswith('Replicated'):
        if eng2 and not eng2.startswith('Replicated'):
            log.warning(f"  Node1 replicated, Node2={eng2} — drop+recreate")
            if DRY_RUN:
                log.info("  [DRY RUN] fix mismatch"); return True
            n1.execute(f"DROP TABLE IF EXISTS `{DATABASE}`.`{name}` ON CLUSTER {CLUSTER} SYNC")
            n2.execute(f"DROP TABLE IF EXISTS `{DATABASE}`.`{name}`")
            time.sleep(3); n1.execute(ddl)
            if backup_exists:
                n1.execute(f"INSERT INTO `{DATABASE}`.`{name}` SELECT * FROM `{DATABASE}`.`{backup}` {final}")
        else:
            log.info("  already Replicated on both — verifying sync only")
    else:
        if DRY_RUN:
            orig = count_rows(n1, name, final=bool(final))
            log.info(f"  [DRY RUN] {name}: {orig} rows -> {ENGINE_MAP[engine]}"); return True
        if backup_exists:
            log.info("  resume: backup exists — dropping partial real table")
            n1.execute(f"DROP TABLE IF EXISTS `{DATABASE}`.`{name}` ON CLUSTER {CLUSTER} SYNC")
        else:
            orig = count_rows(n1, name, final=bool(final))
            log.info(f"  original rows (post-FINAL where applicable): {orig}")
            log.info(f"  renaming {name} -> {backup}")
            n1.execute(f"RENAME TABLE `{DATABASE}`.`{name}` TO `{DATABASE}`.`{backup}`")
        log.info("  creating replicated table ON CLUSTER")
        try:
            n1.execute(ddl)
        except Exception as e:
            log.error(f"  CREATE failed: {e} — rolling back rename")
            n1.execute(f"RENAME TABLE `{DATABASE}`.`{backup}` TO `{DATABASE}`.`{name}`")
            return False
        log.info(f"  backfilling {final or '(no FINAL)'}")
        n1.execute(f"INSERT INTO `{DATABASE}`.`{name}` SELECT * FROM `{DATABASE}`.`{backup}` {final}")

    expected = count_rows(n1, name)
    t = sync_timeout(expected)
    log.info(f"  waiting Node2 sync (timeout {t}m, expect {expected})")
    ok, n2c = wait_for_sync(n2, name, expected, t, 1.0)
    if not ok:
        log.warning(f"  INCOMPLETE {name} (Node2 {n2c}/{expected}) — backup kept"); return False
    if not verify_two_replicas(name):
        log.warning(f"  {name} not 2/2 — backup kept"); return False
    log.info(f"  OK {name} synced 2/2 (Node1 {expected}, Node2 {n2c})")
    if exists_on(n1, backup):
        log.info(f"  dropping backup {backup}")
        n1.execute(f"DROP TABLE IF EXISTS `{DATABASE}`.`{backup}`")
    return True


# ───────────────────────── PHASE 2: MATERIALIZED VIEWS ─────────────────────────
def get_mvs(client):
    return client.execute(f"""
        SELECT name, create_table_query, as_select
        FROM system.tables
        WHERE database='{DATABASE}' AND engine='MaterializedView'
        ORDER BY name
    """)


def parse_mv(create_query):
    m = re.search(r'(?<![A-Za-z0-9_])AS\s*(SELECT|WITH)\b', create_query, re.IGNORECASE)
    if m:
        as_start, kw_start = m.start(), m.start(1)
    else:
        m = re.search(r'\d\s*(AS)\s*(SELECT|WITH)\b', create_query, re.IGNORECASE)
        if not m:
            return None, "no 'AS SELECT'/'AS WITH' boundary"
        as_start, kw_start = m.start(1), m.start(2)
    select_with_as = "AS " + create_query[kw_start:].strip()
    select_only    = create_query[kw_start:].strip()
    head = create_query[:as_start]
    to_m = re.search(r'\bTO\s+`?[\w.]+`?', head, re.IGNORECASE)
    cs = head.find('(')
    if to_m and (cs == -1 or to_m.start() < cs):
        return {'kind': 'to', 'select_with_as': select_with_as,
                'select_only': select_only, 'to_target': to_m.group(0)}, None
    if cs == -1:
        return None, "implicit MV but no columns block"
    depth = 0; ce = -1
    for i in range(cs, len(head)):
        if head[i] == '(':
            depth += 1
        elif head[i] == ')':
            depth -= 1
            if depth == 0:
                ce = i; break
    if ce == -1:
        return None, "unbalanced columns block"
    return {'kind': 'implicit', 'columns': head[cs:ce + 1],
            'engine_block': head[ce + 1:].strip(),
            'select_with_as': select_with_as, 'select_only': select_only}, None


def replicate_engine_block(eng_block, data_table):
    zk = zk_path(data_table); hit = [False]

    def with_args(repl):
        def fn(m):
            hit[0] = True
            inner = (m.group(1) or '').strip()
            return f"ENGINE = {repl}('{zk}', '{{replica}}'" + (f", {inner})" if inner else ")")
        return fn

    def no_args(repl):
        def fn(m):
            hit[0] = True
            return f"ENGINE = {repl}('{zk}', '{{replica}}')"
        return fn

    eng_block = re.sub(r'ENGINE\s*=\s*ReplacingMergeTree(?:\(([^)]*)\))?',
                       with_args('ReplicatedReplacingMergeTree'), eng_block)
    eng_block = re.sub(r'ENGINE\s*=\s*SummingMergeTree(?:\(([^)]*)\))?',
                       with_args('ReplicatedSummingMergeTree'), eng_block)
    eng_block = re.sub(r'ENGINE\s*=\s*AggregatingMergeTree(?:\([^)]*\))?',
                       no_args('ReplicatedAggregatingMergeTree'), eng_block)
    eng_block = re.sub(r'ENGINE\s*=\s*MergeTree(?:\([^)]*\))?',
                       no_args('ReplicatedMergeTree'), eng_block)
    return eng_block, hit[0]


def migrate_mv(name, create_query, as_select):
    log.info(""); log.info("-" * 70)
    log.info(f"[MV] {name}"); log.info("-" * 70)
    n1 = get_client(NODE1); n2 = get_client(NODE2)

    if name in MV_SKIP:
        log.info("  SKIP (MV_SKIP)"); return True
    if re.search(r'\bsystem\.', as_select or '', re.IGNORECASE) or \
       re.search(r'\bsystem\.', create_query, re.IGNORECASE):
        log.info("  SKIP (reads system.* — node-local)"); return True

    parsed, err = parse_mv(create_query)
    if err:
        log.error(f"  parse failed: {err}"); return False

    if parsed['kind'] == 'to':
        log.info(f"  TO-target MV ({parsed['to_target']}); target handled in Phase 1. "
                 f"Recreating trigger ON CLUSTER.")
        if DRY_RUN:
            log.info("  [DRY RUN] drop+create trigger"); return True
        create_mv = (f"CREATE MATERIALIZED VIEW `{DATABASE}`.`{name}` ON CLUSTER {CLUSTER}\n"
                     f"{parsed['to_target']}\n{parsed['select_with_as']}")
        try:
            n1.execute("EXPLAIN AST " + create_mv)
        except Exception as e:
            log.error(f"  PRE-VALIDATION failed: {e}; leaving {name} untouched"); return False
        n1.execute(f"DROP TABLE IF EXISTS `{DATABASE}`.`{name}` ON CLUSTER {CLUSTER} SYNC")
        time.sleep(2)
        n1.execute(create_mv)
        if not exists_on(n2, name):
            log.warning(f"  MV {name} missing on Node2"); return False
        log.info(f"  OK {name} trigger on both nodes"); return True

    # implicit-storage MV
    data_table = f"{name}_data"
    columns, _ = maybe_reorder_columns(name, parsed['columns'], parsed['select_only'])
    repl_eng, hit = replicate_engine_block(parsed['engine_block'], data_table)
    if not hit:
        log.error("  could not build replicated engine block"); return False

    create_data = (f"CREATE TABLE IF NOT EXISTS `{DATABASE}`.`{data_table}` ON CLUSTER {CLUSTER}\n"
                   f"{columns}\n{repl_eng}")
    drop_mv   = f"DROP TABLE IF EXISTS `{DATABASE}`.`{name}` ON CLUSTER {CLUSTER} SYNC"
    drop_data = f"DROP TABLE IF EXISTS `{DATABASE}`.`{data_table}` ON CLUSTER {CLUSTER} SYNC"
    backfill  = f"INSERT INTO `{DATABASE}`.`{data_table}`\n{parsed['select_only']}"
    create_mv = (f"CREATE MATERIALIZED VIEW `{DATABASE}`.`{name}` ON CLUSTER {CLUSTER}\n"
                 f"TO `{DATABASE}`.`{data_table}`\n{parsed['select_with_as']}")

    if DRY_RUN:
        log.info("  [DRY RUN] would run in order:")
        for q in (drop_mv, drop_data, create_data, backfill, create_mv):
            log.info("    ---\n" + q)
        return True

    for label, stmt in (("create_data", create_data), ("create_mv", create_mv), ("backfill", backfill)):
        try:
            n1.execute("EXPLAIN AST " + stmt)
        except Exception as e:
            log.error(f"  PRE-VALIDATION failed on {label}: {e}")
            log.error(f"  Leaving {name} untouched. Skipping."); return False

    try:
        log.info("  1/5 drop old MV trigger ON CLUSTER"); n1.execute(drop_mv)
        log.info("  2/5 drop old _data ON CLUSTER"); n1.execute(drop_data); time.sleep(2)
        log.info("  3/5 create replicated _data ON CLUSTER"); n1.execute(create_data)
        log.info("  4/5 backfill ONCE (before trigger)"); n1.execute(backfill)
        log.info("  5/5 create MV trigger TO _data ON CLUSTER"); n1.execute(create_mv)
    except Exception as e:
        log.error(f"  MV migration failed: {e}"); return False

    expected = count_rows(n1, data_table)
    t = sync_timeout(expected)
    log.info(f"  waiting Node2 sync of {data_table} (timeout {t}m, expect {expected})")
    ok, n2c = wait_for_sync(n2, data_table, expected, t, 0.999)
    if not ok:
        log.warning(f"  INCOMPLETE {name} (Node2 {n2c}/{expected})"); return False
    if not verify_two_replicas(data_table):
        log.warning(f"  {data_table} not 2/2"); return False
    if not exists_on(n2, name):
        log.warning(f"  MV {name} trigger missing on Node2"); return False
    log.info(f"  OK {name} synced 2/2 (Node1 {expected}, Node2 {n2c})")
    return True


# ───────────────────────── FINAL REPORT ─────────────────────────
def final_report():
    n1 = get_client(NODE1); n2 = get_client(NODE2)
    one = lambda c, q: c.execute(q)[0][0]
    rep1 = one(n1, f"SELECT count() FROM system.tables WHERE database='{DATABASE}' AND engine LIKE 'Replicated%'")
    rep2 = one(n2, f"SELECT count() FROM system.tables WHERE database='{DATABASE}' AND engine LIKE 'Replicated%'")
    nonrep = one(n1, f"""SELECT count() FROM system.tables WHERE database='{DATABASE}'
        AND engine IN ('MergeTree','ReplacingMergeTree','SummingMergeTree','AggregatingMergeTree')
        AND name NOT LIKE '.inner%' AND name NOT LIKE '%\\_rep\\_backup'""")
    unhealthy = one(n1, f"""SELECT count() FROM system.replicas WHERE database='{DATABASE}'
        AND (total_replicas<2 OR active_replicas<2 OR queue_size>0)""")
    mv1 = one(n1, f"SELECT count() FROM system.tables WHERE database='{DATABASE}' AND engine='MaterializedView'")
    mv2 = one(n2, f"SELECT count() FROM system.tables WHERE database='{DATABASE}' AND engine='MaterializedView'")
    log.info("\n" + "=" * 70); log.info("FINAL CLUSTER STATE"); log.info("=" * 70)
    log.info(f"  Replicated tables   Node1={rep1}  Node2={rep2}   (should match)")
    log.info(f"  Non-replicated base tables: {nonrep}   (should be 0)")
    log.info(f"  Unhealthy replicas:         {unhealthy}   (should be 0)")
    log.info(f"  MaterializedViews   Node1={mv1}  Node2={mv2}   (mismatch OK only for node-local skips)")
    healthy = (rep1 == rep2 and nonrep == 0 and unhealthy == 0)
    log.info(f"\n  CLUSTER {'HEALTHY' if healthy else 'NEEDS ATTENTION'}")


def _summary(res):
    log.info("\n" + "=" * 70); log.info("SUMMARY"); log.info("=" * 70)
    log.info(f"  BASE ok={len(res['base_ok'])} fail={len(res['base_fail'])} {res['base_fail']}")
    log.info(f"  MV   ok={len(res['mv_ok'])} fail={len(res['mv_fail'])} {res['mv_fail']}")
    log.info("  Log: /tmp/ch_migrate.log")


# ───────────────────────── MAIN ─────────────────────────
def main():
    global DRY_RUN, ASSUME_QUIESCED
    ap = argparse.ArgumentParser(description="ClickHouse standalone -> replicated (single script)")
    ap.add_argument('--dry-run', action='store_true', help="Print plan, change nothing.")
    ap.add_argument('--writes-are-stopped', action='store_true', help="Confirm no writers. Required to apply.")
    ap.add_argument('--i-know-keeper-is-fresh', action='store_true', help="Bypass Keeper-in-use guard (resume on SAME new cluster).")
    ap.add_argument('--recover-mvs-from-source', action='store_true',
                    help="ONLY for recovery: read MV defs from CH_SOURCE (read-only) when the new cluster has lost them.")
    args = ap.parse_args()
    DRY_RUN = args.dry_run
    ASSUME_QUIESCED = args.writes_are_stopped or args.dry_run

    log.info("=" * 70)
    log.info("ClickHouse Standalone -> 2-Node Replicated (single script)")
    log.info(f"Node1={NODE1}  Node2={NODE2}  DB={DATABASE}  Cluster={CLUSTER}")
    log.info(f"DryRun={DRY_RUN}  WritesStopped={args.writes_are_stopped}  "
             f"RecoverFromSource={args.recover_mvs_from_source}")
    log.info("=" * 70)

    guard_nodes()
    if args.recover_mvs_from_source:
        if not SOURCE:
            sys.exit("ABORT: --recover-mvs-from-source needs CH_SOURCE set.")
        if SOURCE in (NODE1, NODE2):
            sys.exit("ABORT: CH_SOURCE must differ from the new nodes.")
        log.warning(f"RECOVERY MODE: MV definitions will be read (READ-ONLY) from {SOURCE}")

    preflight(keeper_override=args.i_know_keeper_is_fresh)
    get_client(NODE1).execute(f"CREATE DATABASE IF NOT EXISTS `{DATABASE}` ON CLUSTER {CLUSTER}")

    res = {'base_ok': [], 'base_fail': [], 'mv_ok': [], 'mv_fail': []}

    # PHASE 1
    log.info("\n" + "#" * 70); log.info("# PHASE 1 — BASE TABLES"); log.info("#" * 70)
    base = get_base_tables(get_client(NODE1))
    log.info(f"Base tables to migrate: {len(base)}")
    for n, e, _ in base:
        log.info(f"  - {n} ({e})")
    if not DRY_RUN and base:
        log.info("Starting Phase 1 in 8s... Ctrl+C to abort"); time.sleep(8)
    for n, e, q in base:
        try:
            (res['base_ok'] if migrate_base_table(n, e, q) else res['base_fail']).append(n)
        except KeyboardInterrupt:
            log.warning("aborted"); _summary(res); return
        except Exception as ex:
            log.error(f"  error {n}: {ex}"); res['base_fail'].append(n)
    if res['base_fail']:
        log.error(f"\nPhase 1 failures: {res['base_fail']} — NOT proceeding to MVs.")
        _summary(res); final_report(); return

    # PHASE 2
    log.info("\n" + "#" * 70); log.info("# PHASE 2 — MATERIALIZED VIEWS"); log.info("#" * 70)
    if args.recover_mvs_from_source:
        mvs = get_mvs(get_client(SOURCE, read_only=True))
        log.info(f"(recovery) read {len(mvs)} MV defs from {SOURCE}")
    else:
        mvs = get_mvs(get_client(NODE1))
    log.info(f"MaterializedViews: {len(mvs)}")
    for n, _, _ in mvs:
        log.info(f"  - {n}")
    if not DRY_RUN and mvs:
        log.info("Starting Phase 2 in 8s... Ctrl+C to abort"); time.sleep(8)
    for n, q, sel in mvs:
        try:
            (res['mv_ok'] if migrate_mv(n, q, sel) else res['mv_fail']).append(n)
        except KeyboardInterrupt:
            log.warning("aborted"); break
        except Exception as ex:
            log.error(f"  error {n}: {ex}"); res['mv_fail'].append(n)

    _summary(res)
    final_report()


if __name__ == '__main__':
    main()