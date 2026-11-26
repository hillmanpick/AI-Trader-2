"""SSH-only local-first training coordination. No exchange or trading API calls."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import threading
import time
import uuid

INTERVAL = 21600
LEASE = 180
LOCAL_GRACE = 600
MAX_JOB = 14400


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    temp.write_text(json.dumps(value, ensure_ascii=False, allow_nan=False), encoding='utf-8')
    os.replace(temp, path)


class Coordinator:
    def __init__(self, lab):
        self.lab = Path(lab)
        self.root = self.lab/'hybrid'
        self.root.mkdir(parents=True, exist_ok=True)
        for shared in (self.root, self.root/'results'):
            shared.mkdir(parents=True, exist_ok=True)
            if hasattr(os, 'geteuid') and os.geteuid() == 0:
                owner = self.lab.stat()
                os.chown(shared, owner.st_uid, owner.st_gid)
                shared.chmod(0o775)
        self.path = self.root/'coordinator.db'
        with self.connect() as c:
            c.execute('CREATE TABLE IF NOT EXISTS control (id INTEGER PRIMARY KEY, body TEXT NOT NULL)')
            c.execute('INSERT OR IGNORE INTO control VALUES (1, ?)', ('{}',))

    def connect(self):
        c = sqlite3.connect(self.path, timeout=15)
        c.execute('PRAGMA busy_timeout=15000')
        return c

    def operation(self, action, owner='local', token=None, success=False):
        # All timestamps come from the server, never the Windows clock.
        with self.connect() as c:
            c.execute('BEGIN IMMEDIATE')
            d = json.loads(c.execute('SELECT body FROM control WHERE id=1').fetchone()[0])
            now = time.time()
            if action == 'presence':
                d['local_seen'] = now
            elif action == 'claim':
                if owner not in ('local', 'server'):
                    raise ValueError('Unknown worker')
                if d.get('expires', 0) > now or d.get('next_due', 0) > now:
                    return {'claimed': False, 'state': d}
                if owner == 'server' and now-d.get('local_seen', 0) < LOCAL_GRACE:
                    return {'claimed': False, 'state': d}
                d.update(owner=owner, token=uuid.uuid4().hex, started=now, expires=now+LEASE)
            elif action in ('renew', 'finish'):
                if (not token or d.get('token') != token or d.get('owner') != owner
                        or d.get('expires', 0) <= now or now-d.get('started', 0) > MAX_JOB):
                    raise ValueError('Lease expired or superseded; result rejected')
                if action == 'renew':
                    d['expires'] = now+LEASE
                else:
                    d.update(expires=0, finished=now, next_due=now+(INTERVAL if success else 900), success=success)
            elif action != 'status':
                raise ValueError('Unknown action')
            c.execute('UPDATE control SET body=? WHERE id=1', (json.dumps(d),))
            return {'claimed': action == 'claim', 'state': d}

    def assert_lease(self, token, owner='local'):
        return self.operation('renew', owner, token)

    def cleanup_stale_snapshots(self):
        state = self.operation('status')['state']
        active = state.get('token') if state.get('expires', 0) > time.time() else None
        # Only coordinator-created UUID names, never caller-supplied paths.
        for p in self.root.iterdir():
            token = p.name.removeprefix('snapshot-').removesuffix('.tar')
            try:
                if (len(token) != 32 or any(c not in '0123456789abcdef' for c in token)
                        or token == active or time.time()-p.stat().st_mtime < LEASE):
                    continue
                if p.is_dir() and p.name == 'snapshot-'+token:
                    shutil.rmtree(p)
                elif p.is_file() and p.name == token+'.tar':
                    p.unlink()
            except OSError:
                # An interrupted root-owned export is cleaned on the next
                # authenticated RPC; it must not kill the unprivileged fallback.
                continue


def backup_database(source, target):
    source, target = Path(source), Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    # SQLite backup includes committed WAL pages. Copying the live .db alone
    # would silently drop recent labels and can produce an inconsistent DB.
    with sqlite3.connect(source.as_uri()+'?mode=ro', uri=True, timeout=30) as src:
        src.execute('BEGIN')
        src.execute('SELECT COUNT(*) FROM sqlite_master').fetchone()
        with sqlite3.connect(target) as dst:
            src.backup(dst, pages=256, sleep=.02)


def snapshot(coordinator, token, code, environment):
    coordinator.assert_lease(token)
    coordinator.cleanup_stale_snapshots()
    target = coordinator.root/('snapshot-'+token)
    source = coordinator.lab/'features.db'
    if shutil.disk_usage(coordinator.root).free < source.stat().st_size*1.15 + 1024**3:
        raise ValueError('Insufficient snapshot disk reserve; source data left intact')
    target.mkdir(exist_ok=False)
    package = coordinator.root/(token+'.tar')
    try:
        backup_database(source, target/'lab/features.db')
        # Compress the DB first, then remove its scratch copy. Parquet is
        # already compressed: wrapping the whole multi-GB archive in gzip
        # wastes CPU and can exhaust this small server's temporary disk.
        with (target/'lab/features.db').open('rb') as src, gzip.open(
                target/'lab/features.db.gz', 'wb', compresslevel=1) as dst:
            shutil.copyfileobj(src, dst, length=1024*1024)
        (target/'lab/features.db').unlink()
        # Hard links pin only immutable, committed event segments while pruning
        # continues. A missing segment aborts the snapshot instead of fabricating
        # complete holdout coverage.
        event_source = coordinator.lab/'events'
        if (event_source/'catalog.db').exists():
            backup_database(event_source/'catalog.db', target/'lab/events/catalog.db')
            with sqlite3.connect(target/'lab/events/catalog.db') as c:
                paths = [r[0] for r in c.execute('SELECT path FROM segments')]
            for name in paths:
                relative = Path(name)
                if relative.is_absolute() or '..' in relative.parts:
                    raise ValueError('Invalid event segment path')
                dst = target/'lab/events'/relative
                dst.parent.mkdir(parents=True, exist_ok=True)
                os.link(event_source/relative, dst)
        shutil.copytree(code, target/'code', ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
        atomic_json(target/'environment.json', environment)
        atomic_json(target/'manifest.json', {'token': token, 'at': time.time(),
                    'source': 'SQLite online backup and pinned event catalog', 'trade_access': False})
        with tarfile.open(package, 'w') as tf:
            for item in target.iterdir():
                tf.add(item, arcname=item.name)
        with package.open('rb') as f:
            digest = hashlib.file_digest(f, 'sha256').hexdigest()
        coordinator.assert_lease(token)
        return {'path': str(package), 'sha256': digest, 'bytes': package.stat().st_size}
    except BaseException:
        package.unlink(missing_ok=True)
        raise
    finally:
        shutil.rmtree(target)


def proof_passes(model, report):
    if not model or model.get('approved') is not True or report.get('status') != 'approved':
        return False
    proof = model.get('execution_validation', {})
    if (proof.get('passed') is not True or proof.get('model_version') != model.get('version')
            or report.get('model_version') != model.get('version')
            or report.get('execution_validation') != proof):
        return False
    keys = ('schema','features','classes','mean','scale','coef','intercept',
            'vector_model','threshold','label_schema','training_end_ms')
    def fingerprint(value):
        return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
    if (model.get('version') != fingerprint({k:model.get(k) for k in keys})[:24]
            or proof.get('schema') != 'portfolio-individual-events-v4'
            or proof.get('policy_hash') != fingerprint(report.get('policy'))
            or not (model.get('training_end_ms', float('inf')) < proof.get('validation_start_ms', 0)
                    < proof.get('test_start_ms', 0) < proof.get('test_end_ms', 0))):
        return False
    folds = report.get('folds', [])
    if len(folds) != 3:
        return False
    for fold in folds:
        r = fold.get('test', {})
        if (r.get('valid') is not True or r.get('trades', 0) < 30 or r.get('net_pnl', 0) <= 0
                or (r.get('profit_factor') or 0) < 1.1 or r.get('max_drawdown', float('inf')) > 3000
                or fold.get('validation', {}).get('net_pnl', 0) <= 0
                or fold.get('stress', {}).get('net_pnl', 0) <= 0):
            return False
    for name in ('validation', 'test', 'stress'):
        r = proof.get(name, {})
        if (r.get('valid') is not True or r.get('trades', 0) < 30 or r.get('net_pnl', 0) <= 0
                or (r.get('profit_factor') is not None and r['profit_factor'] < 1.1)
                or r.get('max_drawdown', float('inf')) > 3000):
            return False
    return True


def publish(coordinator, token, owner, report, model=None):
    # Keep the coordinator transaction open through publication. A stale local
    # worker cannot overwrite a newer server result after losing its lease.
    with coordinator.connect() as c:
        c.execute('BEGIN IMMEDIATE')
        d = json.loads(c.execute('SELECT body FROM control WHERE id=1').fetchone()[0])
        now = time.time()
        if (d.get('token') != token or d.get('owner') != owner or d.get('expires', 0) <= now
                or now-d.get('started', 0) > MAX_JOB):
            raise ValueError('Stale worker cannot publish')
        archived = coordinator.root/'results'/token
        atomic_json(archived/'report.json', report)
        if model:
            atomic_json(archived/'model.json', model)
        promoted = proof_passes(model, report)
        if promoted:
            old = coordinator.lab/'model.json'
            if old.exists():
                shutil.copy2(old, archived/'previous-model.json')
            atomic_json(old, model)
        # Rejections remain diagnostics and never replace the active model.
        atomic_json(coordinator.lab/'latest-report.json', report)
        with sqlite3.connect(coordinator.lab/'features.db', timeout=15) as features:
            features.execute('INSERT INTO reports(ts,report) VALUES (?,?)',
                             (int(now*1000), json.dumps(report, allow_nan=False)))
        d.update(expires=0, finished=now, next_due=now+INTERVAL, success=True,
                 result=report.get('status'), promoted=promoted)
        c.execute('UPDATE control SET body=? WHERE id=1', (json.dumps(d),))
    return {'published': True, 'promoted': promoted, 'status': report.get('status')}


def clean_environment(environment, lab):
    env = {k: v for k, v in environment.items() if k.startswith('QUANT_')
           and not any(s in k for s in ('TOKEN', 'KEY', 'SECRET', 'PASSWORD', 'CLICKHOUSE'))}
    env.update(QUANT_LAB_DIR=str(lab), QUANT_DB_PATH=str(Path(lab)/'unused-live.db'),
               QUANT_PAPER_DB_PATH=str(Path(lab)/'unused-paper.db'),
               QUANT_MODE='paper', QUANT_ENABLED='false', QUANT_LIVE_ENABLED='false',
               QUANT_AI_ENABLED='false', QUANT_PAPER_AI_AUTOPILOT='false',
               QUANT_OBSERVATION_ONLY='true', BINANCE_API_KEY='', BINANCE_API_SECRET='',
               DEEPSEEK_API_KEY='', AI4TRADE_TOKEN='', OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1')
    return env


def fallback_watch(coordinator):
    while True:
        coordinator.cleanup_stale_snapshots()
        claim = coordinator.operation('claim', 'server')
        if claim['claimed']:
            token = claim['state']['token']
            output = coordinator.root/'candidates'/token
            output.mkdir(parents=True)
            process = subprocess.Popen([sys.executable, '-m', 'quant.lab_train',
                      '--output-directory', str(output)], env={**os.environ, **clean_environment(os.environ, coordinator.lab)},
                      stdout=(output/'training.log').open('w'), stderr=subprocess.STDOUT)
            try:
                while process.poll() is None:
                    coordinator.assert_lease(token, 'server')
                    time.sleep(20)
                if process.returncode or not (output/'latest-report.json').exists():
                    raise ValueError('Fallback training did not produce a report')
                model = json.loads((output/'model.json').read_text()) if (output/'model.json').exists() else None
                result = publish(coordinator, token, 'server', json.loads((output/'latest-report.json').read_text()), model)
                print(json.dumps(result), flush=True)
            except Exception as exc:
                process.terminate()
                process.wait(timeout=30)
                try:
                    coordinator.operation('finish', 'server', token, False)
                except ValueError:
                    pass
                print(json.dumps({'fallback_error': str(exc)}), flush=True)
        time.sleep(30)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['rpc', 'fallback-watch'])
    parser.add_argument('--lab', required=True)
    parser.add_argument('--code')
    parser.add_argument('--environment')
    args = parser.parse_args()
    coordinator = Coordinator(args.lab)
    if args.mode == 'fallback-watch':
        fallback_watch(coordinator)
        return
    request = json.loads(sys.stdin.read(4*1024*1024))
    action, token = request['action'], request.get('token')
    if action == 'snapshot':
        result = snapshot(coordinator, token, args.code, json.loads(Path(args.environment).read_text()))
    elif action == 'downloaded':
        coordinator.assert_lease(token)
        (coordinator.root/(token+'.tar')).unlink(missing_ok=True)
        result = {'released_snapshot': True}
    elif action == 'publish':
        result = publish(coordinator, token, 'local', request['report'], request.get('model'))
        (coordinator.root/(token+'.tar')).unlink(missing_ok=True)
    else:
        result = coordinator.operation(action, 'local', token, request.get('success', False))
        if action == 'finish' and token:
            (coordinator.root/(token+'.tar')).unlink(missing_ok=True)
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
