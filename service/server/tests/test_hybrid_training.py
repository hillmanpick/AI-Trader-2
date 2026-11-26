"""Fencing, offline takeover and no promotion of unvalidated research."""
import importlib.util
import json
from pathlib import Path
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
path = Path(__file__).resolve().parents[3]/'deploy'/'hybrid_training.py'
spec = importlib.util.spec_from_file_location('hybrid_training_test', path)
hybrid = importlib.util.module_from_spec(spec)
spec.loader.exec_module(hybrid)


def test_only_one_worker_claims(tmp_path):
    coordinator = hybrid.Coordinator(tmp_path)
    with ThreadPoolExecutor(max_workers=8) as pool:
        claims = list(pool.map(lambda _: coordinator.operation('claim'), range(8)))
    assert sum(r['claimed'] for r in claims) == 1


def test_local_priority_and_server_takeover(tmp_path, monkeypatch):
    coordinator = hybrid.Coordinator(tmp_path)
    now = [10000]
    monkeypatch.setattr(hybrid.time, 'time', lambda: now[0])
    coordinator.operation('presence')
    assert not coordinator.operation('claim', 'server')['claimed']
    claim = coordinator.operation('claim', 'local')
    now[0] += hybrid.LOCAL_GRACE+1
    replacement = coordinator.operation('claim', 'server')
    assert replacement['claimed']
    with pytest.raises(ValueError, match='expired'):
        coordinator.assert_lease(claim['state']['token'])
    assert replacement['state']['owner'] == 'server'


def test_heartbeat_extends_lease_but_never_forever(tmp_path, monkeypatch):
    now = [10000]
    monkeypatch.setattr(hybrid.time, 'time', lambda: now[0])
    coordinator = hybrid.Coordinator(tmp_path)
    token = coordinator.operation('claim')['state']['token']
    now[0] += 100
    assert coordinator.assert_lease(token)['state']['expires'] == now[0]+hybrid.LEASE
    now[0] += hybrid.MAX_JOB
    with pytest.raises(ValueError):
        coordinator.assert_lease(token)


def test_rejected_report_cannot_replace_active_model(tmp_path):
    coordinator = hybrid.Coordinator(tmp_path)
    with sqlite3.connect(tmp_path/'features.db') as c:
        c.execute('CREATE TABLE reports (id INTEGER PRIMARY KEY,ts INTEGER,report TEXT)')
    (tmp_path/'model.json').write_text('{"version":"keep-me"}')
    token = coordinator.operation('claim')['state']['token']
    report = {'status': 'rejected', 'at_ms': 123}
    result = hybrid.publish(coordinator, token, 'local', report, {'approved':False})
    assert not result['promoted']
    assert json.loads((tmp_path/'model.json').read_text())['version'] == 'keep-me'
    assert json.loads((tmp_path/'latest-report.json').read_text()) == report
    with pytest.raises(ValueError, match='Stale'):
        hybrid.publish(coordinator, token, 'local', report)


def test_approved_boolean_is_not_validation_proof():
    assert not hybrid.proof_passes({'approved': True}, {'status':'approved'})


def test_complete_proof_is_required_for_promotion(tmp_path):
    import hashlib
    def fingerprint(d):
        return hashlib.sha256(json.dumps(d,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
    model = {k:None for k in ('schema','features','classes','mean','scale','coef','intercept',
             'vector_model','threshold','label_schema','training_end_ms')}
    model['training_end_ms'] = 1
    version = fingerprint(model)[:24]
    metric = {'valid':True,'trades':30,'net_pnl':1,'profit_factor':1.2,'max_drawdown':1}
    proof = {'passed':True,'model_version':version,'schema':'portfolio-individual-events-v4',
             'policy_hash':fingerprint({}),'validation_start_ms':2,'test_start_ms':3,'test_end_ms':4,
             **{k:dict(metric) for k in ('validation','test','stress')}}
    model.update(approved=True,version=version,execution_validation=proof)
    report = {'status':'approved','model_version':version,'policy':{},'execution_validation':proof,
              'folds':[{k:dict(metric) for k in ('validation','test','stress')} for _ in range(3)]}
    assert hybrid.proof_passes(model,report)
    coordinator = hybrid.Coordinator(tmp_path)
    with sqlite3.connect(tmp_path/'features.db') as c:
        c.execute('CREATE TABLE reports (id INTEGER PRIMARY KEY,ts INTEGER,report TEXT)')
    token = coordinator.operation('claim')['state']['token']
    assert hybrid.publish(coordinator,token,'local',report,model)['promoted']
    assert json.loads((tmp_path/'model.json').read_text())['version'] == version
    proof['stress']['net_pnl'] = -1
    assert not hybrid.proof_passes(model,report)


def test_snapshot_online_backup_keeps_committed_wal(tmp_path):
    source = tmp_path/'source.db'
    with sqlite3.connect(source) as c:
        c.execute('PRAGMA journal_mode=WAL')
        c.execute('CREATE TABLE sample(value TEXT)')
        c.execute("INSERT INTO sample VALUES ('committed')")
        c.commit()
        hybrid.backup_database(source, tmp_path/'copy.db')
        with sqlite3.connect(tmp_path/'copy.db') as copy:
            assert copy.execute('SELECT value FROM sample').fetchone() == ('committed',)


def test_research_env_has_no_trade_keys(tmp_path):
    env = hybrid.clean_environment({'BINANCE_API_KEY':'secret','QUANT_API_TOKEN':'secret',
          'QUANT_LIVE_ENABLED':'true','QUANT_PAPER_LEVERAGE':'20'}, tmp_path)
    assert env['BINANCE_API_KEY'] == ''
    assert 'QUANT_API_TOKEN' not in env
    assert env['QUANT_LIVE_ENABLED'] == 'false'
    assert env['QUANT_PAPER_LEVERAGE'] == '20'


def test_isolated_training_does_not_write_source_report(tmp_path):
    from quant.lab_config import LabConfig
    from quant.lab_store import LabStore
    from quant.lab_train import train
    source, output = tmp_path/'source', tmp_path/'candidate'
    store = LabStore(source)
    result = train(LabConfig(directory=str(source)), output_directory=output)
    assert result['status'] == 'insufficient_data'
    with store.connect() as c:
        assert c.execute('SELECT COUNT(*) FROM reports').fetchone()[0] == 0
    with LabStore(output).connect() as c:
        assert c.execute('SELECT COUNT(*) FROM reports').fetchone()[0] == 1
