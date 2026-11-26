"""Durable private order observations, separate from the paper money ledger.

Exchange order/trade IDs deduplicate recovery and WebSocket delivery. Observed
manual orders never become strategy-owned orders. No credentials are persisted.
"""
from contextlib import contextmanager
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import sqlite3
import time


TERMINAL = {'FILLED', 'CANCELED', 'EXPIRED', 'EXPIRED_IN_MATCH', 'REJECTED'}
STATUSES = TERMINAL | {'NEW', 'PARTIALLY_FILLED', 'PENDING_CANCEL'}


def decimal_text(value):
    try:
        result = Decimal(str(value))
    except InvalidOperation:
        raise ValueError('Invalid private order number') from None
    if not result.is_finite():
        raise ValueError('Invalid private order number')
    return format(result, 'f')


class PrivateJournal:
    def __init__(self, path, environment, api_key):
        if environment not in {'production', 'testnet'} or not api_key:
            raise ValueError('Private journal requires an explicit environment and account')
        self.path = str(path)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        identity = environment + ':' + hashlib.sha256(api_key.encode()).hexdigest()
        with self.connect() as c:
            c.executescript('''
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS private_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS private_orders(
                    symbol TEXT NOT NULL, order_id TEXT NOT NULL, client_id TEXT NOT NULL,
                    status TEXT NOT NULL, quantity TEXT NOT NULL, executed TEXT NOT NULL,
                    updated_ms INTEGER NOT NULL, source TEXT NOT NULL, body TEXT NOT NULL,
                    PRIMARY KEY(symbol,order_id));
                CREATE INDEX IF NOT EXISTS private_order_client ON private_orders(client_id);
                CREATE TABLE IF NOT EXISTS private_fills(
                    symbol TEXT NOT NULL, trade_id TEXT NOT NULL, order_id TEXT NOT NULL,
                    at_ms INTEGER NOT NULL, body TEXT NOT NULL, PRIMARY KEY(symbol,trade_id));
                CREATE TABLE IF NOT EXISTS private_intents(
                    client_id TEXT PRIMARY KEY, symbol TEXT NOT NULL, state TEXT NOT NULL,
                    created_ms INTEGER NOT NULL, body TEXT NOT NULL);
            ''')
            saved = c.execute("SELECT value FROM private_meta WHERE key='identity'").fetchone()
            if saved and saved[0] != identity:
                raise ValueError('Private journal account/environment mismatch; choose another path')
            c.execute("INSERT OR IGNORE INTO private_meta VALUES ('identity',?)", (identity,))
            c.execute("INSERT OR IGNORE INTO private_meta VALUES ('first_seen_ms',?)", (str(time.time_ns()//1_000_000),))
        self.environment = environment

    @contextmanager
    def connect(self):
        c = sqlite3.connect(self.path, timeout=5)
        c.row_factory = sqlite3.Row
        c.execute('PRAGMA synchronous=FULL')
        try:
            with c:
                yield c
        finally:
            c.close()

    def set(self, key, value):
        with self.connect() as c:
            c.execute('INSERT INTO private_meta VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                      (key, json.dumps(value, allow_nan=False)))

    def get(self, key, default=None):
        with self.connect() as c:
            row = c.execute('SELECT value FROM private_meta WHERE key=?', (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def fence(self, reason):
        self.set('reconciliation', {'ready': False, 'reason': reason, 'at_ms': time.time_ns()//1_000_000})

    def kill(self, enabled=True):
        self.set('kill_switch', bool(enabled))

    def reserve(self, client_id, params):
        allowed = {'symbol', 'side', 'type', 'quantity', 'price', 'timeInForce', 'reduceOnly', 'positionSide'}
        if set(params) - allowed:
            raise ValueError('Unsupported order parameter')
        body = json.dumps(params, sort_keys=True, allow_nan=False)
        with self.connect() as c:
            cursor = c.execute('INSERT OR IGNORE INTO private_intents VALUES (?,?,?,?,?)',
                               (client_id, params['symbol'], 'SUBMITTING', time.time_ns()//1_000_000, body))
            old = c.execute('SELECT body FROM private_intents WHERE client_id=?', (client_id,)).fetchone()
            if old[0] != body:
                raise ValueError('Client order ID reused with different parameters')
            return cursor.rowcount == 1

    def intent(self, client_id):
        with self.connect() as c:
            row = c.execute('SELECT * FROM private_intents WHERE client_id=?', (client_id,)).fetchone()
        return dict(row) if row else None

    def intent_state(self, client_id, state):
        with self.connect() as c:
            # A late timeout must not overwrite a concurrent terminal user event.
            c.execute("UPDATE private_intents SET state=? WHERE client_id=? AND state NOT IN "
                      "('FILLED','CANCELED','EXPIRED','EXPIRED_IN_MATCH','REJECTED','NOT_SENT')", (state, client_id))

    def unresolved(self):
        with self.connect() as c:
            return [dict(r) for r in c.execute("SELECT * FROM private_intents WHERE state NOT IN "
                "('FILLED','CANCELED','EXPIRED','EXPIRED_IN_MATCH','REJECTED','NOT_SENT') ORDER BY created_ms")]

    def open_orders(self):
        with self.connect() as c:
            return [dict(r) for r in c.execute("SELECT * FROM private_orders WHERE status NOT IN "
                "('FILLED','CANCELED','EXPIRED','EXPIRED_IN_MATCH','REJECTED')")]

    def apply_order(self, row, source='rest'):
        symbol, order_id = str(row['symbol']), str(row['orderId'])
        status = str(row['status'])
        if not symbol or not order_id or status not in STATUSES:
            raise ValueError('Invalid private order identity/status')
        quantity, executed = decimal_text(row['origQty']), decimal_text(row['executedQty'])
        if Decimal(quantity) < 0 or not 0 <= Decimal(executed) <= Decimal(quantity):
            raise ValueError('Invalid private order cumulative quantity')
        updated = int(row.get('updateTime', row.get('time', 0)))
        if updated < 0:
            raise ValueError('Invalid private order timestamp')
        client = str(row.get('clientOrderId', ''))
        body = {key: row[key] for key in ('symbol','orderId','clientOrderId','status','side','type',
            'origQty','executedQty','avgPrice','price','positionSide','reduceOnly','updateTime') if key in row}
        with self.connect() as c:
            c.execute('BEGIN IMMEDIATE')
            old = c.execute('SELECT * FROM private_orders WHERE symbol=? AND order_id=?', (symbol,order_id)).fetchone()
            if old:
                if (Decimal(executed) < Decimal(old['executed']) or
                    (updated < old['updated_ms'] and Decimal(executed) <= Decimal(old['executed'])) or
                    (old['status'] == 'FILLED' and status != 'FILLED') or
                    (old['status'] in TERMINAL and status not in TERMINAL)):
                    return False
                if client and old['client_id'] and client != old['client_id']:
                    raise ValueError('Exchange order client identity changed')
                client = client or old['client_id']
            c.execute('INSERT INTO private_orders VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(symbol,order_id) '
                      'DO UPDATE SET client_id=excluded.client_id,status=excluded.status,quantity=excluded.quantity,'
                      'executed=excluded.executed,updated_ms=excluded.updated_ms,source=excluded.source,body=excluded.body',
                      (symbol,order_id,client,status,quantity,executed,updated,source,json.dumps(body,allow_nan=False)))
            # No matching durable local intent means observation only, even if
            # somebody used a client ID starting with our usual prefix.
            c.execute('UPDATE private_intents SET state=? WHERE client_id=? AND symbol=?', (status,client,symbol))
        return True

    def _fill_values(self, row):
        symbol, trade_id = str(row['symbol']), str(row['id'])
        if int(trade_id) < 0 or int(row['time']) < 0 or Decimal(decimal_text(row['qty'])) <= 0:
            raise ValueError('Invalid private fill')
        body = {k: row[k] for k in ('symbol','id','orderId','time','qty','price','commission',
            'commissionAsset','realizedPnl','side','positionSide','maker') if k in row}
        for key in ('price','commission','realizedPnl'):
            if key in body and body[key] is not None:
                decimal_text(body[key])
        return symbol,trade_id,str(row['orderId']),int(row['time']),json.dumps(body,allow_nan=False)

    def apply_fills(self, rows):
        # One durable transaction per bounded recovery page, not one fsync per
        # historical trade. Live single-fill delivery still commits immediately.
        values = [self._fill_values(row) for row in rows]
        with self.connect() as c:
            before = c.total_changes
            c.executemany('INSERT OR IGNORE INTO private_fills VALUES (?,?,?,?,?)',values)
            return c.total_changes-before

    def apply_fill(self, row):
        return self.apply_fills([row]) == 1

    def event(self, payload):
        if payload.get('e') != 'ORDER_TRADE_UPDATE':
            return False
        order = payload['o']
        self.apply_order({'symbol':order['s'],'orderId':order['i'],'clientOrderId':order['c'],
            'status':order['X'],'origQty':order['q'],'executedQty':order['z'],
            'side':order['S'],'type':order['o'],'avgPrice':order.get('ap','0'),
            'positionSide':order.get('ps','BOTH'),'reduceOnly':order.get('R',False),
            'updateTime':order.get('T',payload.get('T',payload['E']))}, 'user_stream')
        if order.get('x') == 'TRADE' and Decimal(decimal_text(order.get('l','0'))) > 0:
            self.apply_fill({'symbol':order['s'],'id':order['t'],'orderId':order['i'],
                'time':order.get('T',payload.get('T',payload['E'])),'qty':order['l'],
                'price':order['L'],'commission':order.get('n'),'commissionAsset':order.get('N'),
                'realizedPnl':order.get('rp','0'),'side':order['S'],
                'positionSide':order.get('ps','BOTH'),'maker':order.get('m',False)})
        return True

    def summary(self):
        with self.connect() as c:
            states = dict(c.execute('SELECT status,COUNT(*) FROM private_orders GROUP BY status'))
            fills = c.execute('SELECT COUNT(*) FROM private_fills').fetchone()[0]
        return {'environment':self.environment,'order_states':states,'fills':fills,
                'unresolved_intents':len(self.unresolved()),'kill_switch':self.get('kill_switch',False),
                'reconciliation':self.get('reconciliation',{'ready':False,'reason':'not_started'})}
