"""Frozen-hypothesis selection, purged forward tests and paper-only artifacts.

Sampled quotes are conservative preliminary research, not tick replay or live
profitability certification. A passing artifact permits bounded paper trading
only. Strategy definitions and costs are bound to the exact runtime code.
"""
import argparse
import gzip
import hashlib
import json
import math
import os
import random
import time
from collections import Counter
from pathlib import Path

from .execution_policy import fingerprint
from .lab_features import FEATURES, SCHEMA
from .lab_replay import replay
from .quant_strategy import VERSION, STRATEGIES, GRID_MS, HORIZON_MS, signal, policy_for_quant

REPORT_SCHEMA = 'systematic-research-v1'
CODE_FILES = ('quant_strategy.py', 'quant_research.py', 'quant_runtime.py', 'lab_replay.py',
              'lab_features.py', 'lab_worker.py', 'order_book.py', 'paper_worker.py',
              'paper_execution.py', 'execution_state.py', 'engine.py')


def code_hash():
    root = Path(__file__).parent
    return fingerprint({f: hashlib.sha256((root / f).read_bytes()).hexdigest() for f in CODE_FILES})


def compact(result):
    return {k: v for k, v in result.items() if k not in ('fills', 'policy', 'limitations')}


def metrics_pass(result, minimum=20):
    pf = result.get('profit_factor')
    return bool(result.get('valid') and result.get('trades', 0) >= minimum
                and result.get('net_pnl', 0) > 0 and (pf is None or pf >= 1.15))


def block_confidence(trades, start, end):
    """Resample three-hour PnL blocks including zero-trade hours, not IID fills."""
    width = 10800000
    bins = [0.] * max(1, math.ceil((end - start) / width))
    for t in trades:
        bins[min(len(bins) - 1, max(0, (t['at'] - start) // width))] += t['net']
    rng = random.Random(170018)
    samples = sorted(sum(rng.choices(bins, k=len(bins))) / len(bins) for _ in range(1000))
    # Fixed family of three hypotheses: conservative Bonferroni tail.
    tail = .05 / len(STRATEGIES)
    return {'blocks': len(bins), 'block_hours': 3, 'resamples': 1000,
            'lower_mean_usdt': samples[int(len(samples) * tail)],
            'tail_probability': tail, 'method': 'seeded nonoverlapping time-block bootstrap',
            'limitation': 'Finite historical blocks and strategy selection do not establish future returns'}


def qualification(report):
    reasons = []
    if report.get('selected') not in STRATEGIES:
        reasons.append('没有候选策略通过验证区间的扣费后门槛')
    folds = report.get('folds', [])
    if len(folds) != 3:
        reasons.append('缺少三个按时间隔离的样本外区间')
    for i, fold in enumerate(folds):
        if not metrics_pass(fold.get('test', {})):
            reasons.append(f'样本外区间 {i+1}：交易数、净收益、盈亏比或数据连续性不合格')
        if not metrics_pass(fold.get('stress', {})):
            reasons.append(f'样本外区间 {i+1}：双倍手续费与滑点压力测试不合格')
        if fold.get('test', {}).get('max_drawdown', float('inf')) > report['policy']['capital'] * .1:
            reasons.append(f'样本外区间 {i+1}：回撤超过研究资金的 10%')
    summary = report.get('oos', {})
    if summary.get('trades', 0) < 80 or summary.get('span_hours', 0) < 72:
        reasons.append('样本外证据不足：至少 80 笔且覆盖 72 小时')
    confidence = report.get('confidence', {})
    if confidence.get('blocks', 0) < 12 or confidence.get('lower_mean_usdt', -1) <= 0:
        reasons.append('按时间分块的净收益置信下界未大于零')
    return {'passed': not reasons, 'reasons': reasons}


def evaluate(rows, policy, manifest, now_ms=None):
    if len(rows) < 1000:
        raise ValueError('Insufficient chronological quotes: need at least 1000')
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    start, end = rows[0]['ts'], rows[-1]['ts'] + 1
    if end > now_ms:
        raise ValueError('Research contains future observations')
    if end - start < 24 * 3600000:
        raise ValueError('Insufficient historical coverage: need at least 24 hours to evaluate')
    previous_ts, seen = -1, set()
    for row in rows:
        meta = row['meta']
        identity = (row['symbol'], row['ts'])
        if row['ts'] < previous_ts or identity in seen:
            raise ValueError('Quotes must be chronological and unique')
        previous_ts = row['ts']
        seen.add(identity)
        if (not all(math.isfinite(row.get(k, float('nan'))) for k in ('price', 'ts'))
                or row['price'] <= 0 or not 0 < meta['bid'] < meta['ask']
                or not all(math.isfinite(meta[k]) and meta[k] >= 0 for k in ('bid_depth', 'ask_depth', 'spread_bps'))):
            raise ValueError('Invalid historical executable quote')
        if row.get('vector') and (meta.get('schema') != SCHEMA or len(row['vector']) != len(FEATURES)
                or not all(math.isfinite(v) for v in row['vector'])):
            raise ValueError('Invalid historical feature schema')
    boundary = int(start + .4 * (end - start))

    def run(name, first, last, stress=False):
        # Start flat after a full holding-horizon embargo at every boundary.
        subset = [r for r in rows if first + HORIZON_MS <= r['ts'] < last]
        return replay(subset, (), policy=policy, quant=True,
                      horizon_ms=HORIZON_MS, latency_ms=500,
                      cost_multiplier=2. if stress else 1.,
                      signal_fn=lambda r: signal(name, r, policy)['side'])

    validation = {name: compact(run(name, start, boundary)) for name in STRATEGIES}
    available = [n for n in STRATEGIES if metrics_pass(validation[n], 30)]
    # Freeze ONE choice before looking at ANY forward test result.
    selected = max(available, key=lambda n: validation[n]['net_pnl']) if available else None
    folds, completed = [], []
    if selected:
        edges = [int(start + fraction * (end - start)) for fraction in (.4, .6, .8, 1.)]
        edges[-1] = end
        for a, b in zip(edges, edges[1:]):
            result, stress = run(selected, a, b), run(selected, a, b, True)
            completed.extend(result['fills'])
            folds.append({'start_ms': a, 'entry_start_ms': a + HORIZON_MS, 'end_ms': b,
                          'test': compact(result), 'stress': compact(stress)})
    coverage = Counter(r['ts'] // 3600000 for r in rows if r.get('vector'))
    report = {'schema': REPORT_SCHEMA, 'version': VERSION, 'generated_ms': now_ms,
              'evaluated_until_ms': end - 1, 'expires_ms': end - 1 + 48 * 3600000,
              'selected': selected, 'validation': validation, 'folds': folds,
              'policy': policy, 'policy_hash': fingerprint(policy), 'code_hash': code_hash(),
              'manifest': manifest, 'hypotheses': list(STRATEGIES),
              'coverage': {'quotes': len(rows), 'feature_hours': len(coverage),
                           'start_ms': start, 'end_ms': end},
              'oos': {'trades': len(completed), 'net_pnl': sum(t['net'] for t in completed),
                      'costs': sum(t['cost'] for t in completed),
                      'gross': sum(t['gross'] for t in completed),
                      'span_hours': (end - boundary) / 3600000},
              'confidence': block_confidence(completed, boundary, end),
              'paper_only': True, 'live_execution_enabled': False,
              'profitability_proven': False,
              'method': 'frozen rules; earliest 40% validation selection; three 20% OOS folds; full-horizon boundary embargo',
              'limitations': ['30-second sampled quotes cannot recover intrasecond paths or full depth queues',
                  'Prices use bid/ask, adverse slippage, fees and conservative prorated funding',
                  'Exit depth and queue priority remain approximations; current paper fills use the IOC depth journal',
                  'Same-direction position limit is a conservative concentration proxy, not historical covariance',
                  'Candidate history reflects the collector universe; cannot establish an all-market historical result',
                  'Repeated rolling tests are monitored research, not independent new discoveries']}
    report['qualification'] = qualification(report)
    report['status'] = 'paper_qualified' if report['qualification']['passed'] else 'rejected'
    report['experiment_id'] = fingerprint(report)
    return report


def report_path(directory):
    return Path(directory) / 'quant-system' / 'latest.json'


def atomic_report(directory, report):
    path = report_path(directory)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False), encoding='utf-8')
    os.replace(tmp, path)


def artifact_status(directory, policy, now_ms=None):
    """Fail closed on stale data, changed costs/code or incomplete evidence."""
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    try:
        path = report_path(directory)
        if path.stat().st_size > 2 * 1024 * 1024:
            raise ValueError('Research report exceeds bound')
        report = json.loads(path.read_text(encoding='utf-8'))
        if report.get('schema') == REPORT_SCHEMA and report.get('status') == 'research_error':
            return {'ready': False, 'approved': False, 'paper_qualified': False,
                    'decision_source': VERSION, 'report': report,
                    'reason': '历史评估未完成：' + str(report.get('reason', '数据不足'))[:240]}
        identity = report.get('experiment_id')
        if identity != fingerprint({k: v for k, v in report.items() if k != 'experiment_id'}):
            raise ValueError('Research artifact integrity mismatch')
        if report.get('schema') != REPORT_SCHEMA or report.get('version') != VERSION:
            raise ValueError('Research version mismatch')
        if report.get('code_hash') != code_hash() or report.get('policy_hash') != fingerprint(policy):
            raise ValueError('Research code or execution costs/risk policy changed; re-evaluation required')
        if (not 0 <= now_ms - report['generated_ms'] <= 48 * 3600000
                or not report['evaluated_until_ms'] <= report['generated_ms']
                or not now_ms < report['expires_ms'] <= report['evaluated_until_ms'] + 48 * 3600000):
            raise ValueError('Research artifact stale or future-dated')
        passed = qualification(report)
        return {'ready': passed['passed'], 'approved': False, 'paper_qualified': passed['passed'],
                'reason': '历史报价样本外门槛通过，仅允许模拟前向检验' if passed['passed'] else '；'.join(passed['reasons']),
                'decision_source': VERSION, 'report': report}
    except (OSError, ValueError, TypeError, KeyError, OverflowError):
        return {'ready': False, 'approved': False, 'paper_qualified': False,
                'decision_source': VERSION, 'reason': '研究报告缺失、过期或与当前代码/风控不一致，等待重新评估'}


def load_history(directory, start_ms, end_ms, max_rows=300000):
    from .lab_store import LabStore
    rows, digest, filters = [], hashlib.sha256(), {}
    # No future labels filter the execution path. Unlabeled exit quotes stay.
    for row in LabStore(directory).market_rows(start_ms, end_ms, stride_ms=GRID_MS):
        digest.update(json.dumps(row, sort_keys=True, separators=(',', ':')).encode())
        if 'filters' in row['meta']:
            identity = (row['symbol'], json.dumps(row['meta']['filters'], sort_keys=True))
            row['meta']['filters'] = filters.setdefault(identity, row['meta']['filters'])
        rows.append(row)
        if len(rows) > max_rows:
            raise ValueError('Historical row bound exceeded; explicitly reduce the research window')
    return rows, {'sha256': digest.hexdigest(), 'quotes': len(rows), 'stride_ms': GRID_MS,
                  'start_ms': start_ms, 'end_ms': end_ms, 'source': 'causal_features_and_exit_quotes'}


def main():
    import config  # noqa: F401: load env before settings
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--watch', action='store_true')
    parser.add_argument('--input', type=Path)
    parser.add_argument('--policy-json', type=Path)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--window-hours', type=int, default=168)
    args = parser.parse_args()
    from .settings import Settings
    from .paper import paper_settings
    from .lab_config import LabConfig
    cfg, lab = paper_settings(Settings()), LabConfig()
    policy = json.loads(args.policy_json.read_text()) if args.policy_json else policy_for_quant(cfg)
    while True:
        now = int(time.time() * 1000)
        rows = []
        try:
            if args.input:
                raw = args.input.read_bytes()
                rows = [json.loads(line) for line in gzip.decompress(raw).splitlines()]
                manifest = {'source': 'frozen_gzip', 'sha256': hashlib.sha256(raw).hexdigest(), 'quotes': len(rows)}
            else:
                rows, manifest = load_history(lab.directory, now - args.window_hours * 3600000, now - 60000)
            report = evaluate(rows, policy, manifest, now)
        except Exception as exc:
            report = {'schema': REPORT_SCHEMA, 'status': 'research_error', 'generated_ms': now,
                      'paper_only': True, 'profitability_proven': False,
                      'reason': type(exc).__name__ + ': ' + str(exc)[:240]}
            # Replace previous approvals on data failure; protection is separate.
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
        else:
            atomic_report(lab.directory, report)
        print(json.dumps({k: report.get(k) for k in ('status', 'selected', 'experiment_id', 'qualification', 'reason')}, ensure_ascii=False), flush=True)
        if not args.watch:
            break
        del rows
        interval = max(1800, int(os.getenv('QUANT_SYSTEM_RESEARCH_INTERVAL_SECONDS', '21600')))
        # Bounded sleeps allow service stop signals to work normally.
        for _ in range(math.ceil(interval / 30)):
            time.sleep(30)


if __name__ == '__main__':
    main()
