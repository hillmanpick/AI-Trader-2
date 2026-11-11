"""Audit forward trials in the existing paper ledger; never create any fills."""
import json
import time
from uuid import uuid4

from .store import utcnow

KEY = 'paper:forward_trial'


def sync_forward_trial(store, cfg):
    """Keep one baseline across worker restarts, without resetting the account."""
    enabled = cfg.enabled and cfg.paper_exploration_mode and not cfg.paper_ai_autopilot
    if enabled:
        cfg.validate()
        if cfg.mode != 'paper' or cfg.live_enabled or cfg.api_key or cfg.api_secret:
            raise ValueError('Forward trials require credential-free paper execution')
    with store.connect() as c:
        c.execute('BEGIN IMMEDIATE')
        saved = c.execute('SELECT value FROM kv WHERE key=?', (KEY,)).fetchone()
        trial = json.loads(saved[0]) if saved else None
        if not enabled:
            if not trial or not trial.get('active'):
                return trial
            trial = trial | {'active':False, 'ended_at':utcnow()}
        elif trial and trial.get('active'):
            return trial
        else:
            balance = c.execute("SELECT value FROM kv WHERE key='paper_balance'").fetchone()
            trial = {'id':uuid4().hex, 'active':True, 'started_ns':time.time_ns(),
                     'started_at':utcnow(), 'baseline_wallet':json.loads(balance[0]) if balance else cfg.paper_cash,
                     'baseline_fill_count':c.execute('SELECT COUNT(*) FROM execution_fills').fetchone()[0],
                     'scope':'existing_paper_only', 'decision_source':'numeric_model',
                     'note':'未验证模型的前向试验；保留原余额和历史，不代表盈利验证通过'}
        c.execute('INSERT INTO kv VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                  (KEY,json.dumps(trial,ensure_ascii=False)))
        c.execute('INSERT INTO events(kind,body,created_at) VALUES (?,?,?)',
                  ('模拟前向试验开始' if enabled else '模拟前向试验结束',
                   json.dumps(trial,ensure_ascii=False),utcnow()))
        return trial


def forward_trial_status(store):
    """Count durable fills belonging to orders submitted after the trial began."""
    with store.connect() as c:
        saved = c.execute('SELECT value FROM kv WHERE key=?', (KEY,)).fetchone()
        if not saved:
            return None
        trial = json.loads(saved[0])
        end = trial.get('ended_at')
        # A closed trial must not absorb later orders from a different mode.
        from datetime import datetime
        end_ns = int(datetime.fromisoformat(end).timestamp()*1e9) if end else time.time_ns()
        bounds = (trial['started_ns'], end_ns)
        orders = c.execute("SELECT COUNT(*) FROM execution_orders WHERE "
                           "json_extract(body,'$.submitted_ns') BETWEEN ? AND ?",bounds).fetchone()[0]
        source = (" FROM execution_fills f JOIN execution_orders o ON o.id=f.order_id "
                  "WHERE json_extract(o.body,'$.submitted_ns') BETWEEN ? AND ?")
        totals = c.execute("SELECT COUNT(*), COALESCE(SUM(CAST(f.fee AS REAL)),0), "
            "COUNT(DISTINCT CASE WHEN json_extract(o.body,'$.closing')=0 THEN o.id END), "
            "COUNT(DISTINCT CASE WHEN json_extract(o.body,'$.closing')=1 THEN o.id END)"+source,bounds).fetchone()
        row = c.execute("SELECT f.*,o.body"+source+" ORDER BY f.at_ns DESC,f.id DESC LIMIT 1",bounds).fetchone()
        last = None
        if row:
            order = json.loads(row['body'])
            context = order.get('paper_context',{})
            leverage = context.get('leverage')
            last = {'id':row['id'],'order_id':row['order_id'],'at_ns':row['at_ns'],
                    'symbol':order['symbol'],'side':order['side'],'closing':order['closing'],
                    'quantity':float(row['quantity']),'price':float(row['price']),
                    'fee':float(row['fee']),'leverage':leverage,
                    'model_version':context.get('model_version')}
        return trial | {'submitted_orders':orders,'filled_entry_orders':totals[2],
                        'filled_exit_orders':totals[3],'fill_count':totals[0],'fees_usdt':totals[1],
                        'last_fill':last,'evidence':'execution_orders + execution_fills in the original paper ledger'}
