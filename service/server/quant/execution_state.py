"""Persistent simulated order state and event-driven depth/queue matching.

No exchange client, credentials, network calls or real order routes exist here.
All execution timestamps are explicitly simulation-clock observations; local
processing time is measured separately with perf_counter_ns.
"""
import hashlib
import json
import math
import sqlite3
import time
import threading
from contextlib import contextmanager
from decimal import Decimal, localcontext, ROUND_DOWN

TERMINAL = {'FILLED','CANCELED','REJECTED','EXPIRED'}
TRANSITIONS = {
    'SUBMITTED':{'ACKNOWLEDGED','CANCEL_PENDING','REJECTED','UNKNOWN','EXPIRED'},
    'ACKNOWLEDGED':{'PARTIALLY_FILLED','FILLED','CANCEL_PENDING','CANCELED','EXPIRED','UNKNOWN','REJECTED'},
    'PARTIALLY_FILLED':{'PARTIALLY_FILLED','FILLED','CANCEL_PENDING','CANCELED','EXPIRED','UNKNOWN'},
    'CANCEL_PENDING':{'CANCEL_PENDING','PARTIALLY_FILLED','FILLED','CANCELED','UNKNOWN','EXPIRED'},
    'UNKNOWN':{'ACKNOWLEDGED','PARTIALLY_FILLED','FILLED','CANCELED','REJECTED','EXPIRED'},
}


def number(value):
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError('Non-finite order value')
    return result


def percentile(values,p):
    if not values:
        return None
    values = sorted(values)
    index = (len(values)-1)*p
    low,high = math.floor(index),math.ceil(index)
    return values[low]+(values[high]-values[low])*(index-low)


class OrderJournal:
    def __init__(self,path):
        self.path = str(path)
        self._local = threading.local()
        with self.connect() as c:
            c.executescript('''PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS execution_orders (
                    id TEXT PRIMARY KEY, fingerprint TEXT, state TEXT, body TEXT);
                CREATE TABLE IF NOT EXISTS execution_events (
                    seq INTEGER PRIMARY KEY, order_id TEXT, at_ns INTEGER, state TEXT, detail TEXT);
                CREATE TABLE IF NOT EXISTS execution_fills (
                    id TEXT PRIMARY KEY, order_id TEXT, at_ns INTEGER, quantity TEXT, price TEXT, fee TEXT);
                CREATE TABLE IF NOT EXISTS execution_match_state (
                    id INTEGER PRIMARY KEY CHECK(id=1), body TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS execution_active_orders ON execution_orders(state)
                    WHERE state NOT IN ('FILLED','CANCELED','REJECTED','EXPIRED');
            ''')

    @contextmanager
    def connect(self):
        active = getattr(self._local,'connection',None)
        if active is not None:
            yield active
            return
        c = sqlite3.connect(self.path,timeout=15)
        c.row_factory = sqlite3.Row
        try:
            with c:
                yield c
        finally:
            c.close()

    @contextmanager
    def transaction(self):
        """One event commits its fills, queue updates and matcher cursor together."""
        if getattr(self._local,'connection',None) is not None:
            raise RuntimeError('Nested matcher transaction')
        with self.connect() as c:
            c.execute('BEGIN IMMEDIATE')
            self._local.connection = c
            try:
                yield c
            finally:
                self._local.connection = None

    def match_state(self):
        with self.connect() as c:
            row = c.execute('SELECT body FROM execution_match_state WHERE id=1').fetchone()
        return json.loads(row[0]) if row else {'consumed':[],'last_trade':{}}

    def save_match_state(self,body):
        with self.connect() as c:
            c.execute('INSERT INTO execution_match_state VALUES (1,?) '
                      'ON CONFLICT(id) DO UPDATE SET body=excluded.body',(body,))

    def get(self,identity):
        with self.connect() as c:
            row = c.execute('SELECT body FROM execution_orders WHERE id=?',(identity,)).fetchone()
        return json.loads(row[0]) if row else None

    def orders(self,active=False):
        with self.connect() as c:
            query = 'SELECT body FROM execution_orders'
            if active:
                query += " WHERE state NOT IN ('FILLED','CANCELED','REJECTED','EXPIRED')"
            rows = c.execute(query).fetchall()
        return [json.loads(r[0]) for r in rows]

    def create(self,order):
        request = {k:order[k] for k in ('id','symbol','side','quantity','type','limit_price','tif','reduce_only')}
        if order.get('max_quote') is not None:
            request['max_quote'] = order['max_quote']
        fingerprint = hashlib.sha256(json.dumps(request,sort_keys=True).encode()).hexdigest()
        with self.connect() as c:
            old = c.execute('SELECT fingerprint,body FROM execution_orders WHERE id=?',(order['id'],)).fetchone()
            if old:
                if old[0] != fingerprint:
                    raise ValueError('Order ID reused for a different request')
                return json.loads(old[1])
            c.execute('INSERT INTO execution_orders VALUES (?,?,?,?)',
                      (order['id'],fingerprint,order['state'],json.dumps(order,allow_nan=False)))
            c.execute('INSERT INTO execution_events(order_id,at_ns,state,detail) VALUES (?,?,?,?)',
                      (order['id'],order['submitted_ns'],'SUBMITTED','{}'))
        return order

    def change(self,order,new_state,at_ns,detail=None,fill=None,on_fill=None):
        """CAS state transition and optional fill/ledger callback in one transaction."""
        with self.connect() as c:
            if not c.in_transaction:
                c.execute('BEGIN IMMEDIATE')
            stored = c.execute('SELECT body FROM execution_orders WHERE id=?',(order['id'],)).fetchone()
            if not stored:
                raise ValueError('Order does not exist')
            previous = json.loads(stored[0])
            if fill and c.execute('SELECT 1 FROM execution_fills WHERE id=?',(fill['id'],)).fetchone():
                return previous
            if previous['state'] in TERMINAL or new_state not in TRANSITIONS.get(previous['state'],set()):
                raise ValueError('Invalid order state transition')
            if at_ns < previous.get('updated_ns',previous['submitted_ns']):
                raise ValueError('Non-monotonic order event')
            if number(order['filled']) < number(previous['filled']):
                raise ValueError('Cumulative fill cannot decrease')
            if number(order['filled']) > number(order['quantity']):
                raise ValueError('Cumulative fill exceeds order quantity')
            if new_state=='FILLED' and number(order['filled']) != number(order['quantity']):
                raise ValueError('FILLED requires the full order quantity')
            if new_state=='PARTIALLY_FILLED' and not 0<number(order['filled'])<number(order['quantity']):
                raise ValueError('PARTIALLY_FILLED requires an actual partial quantity')
            if fill:
                if (number(fill['quantity'])<=0 or number(fill['price'])<=0
                        or number(order['filled'])-number(previous['filled']) != number(fill['quantity'])):
                    raise ValueError('Fill does not match cumulative order quantity')
                c.execute('INSERT INTO execution_fills VALUES (?,?,?,?,?,?)',
                          (fill['id'],order['id'],at_ns,fill['quantity'],fill['price'],fill['fee']))
                if on_fill:
                    on_fill(c,fill,order)
            order = order | {'state':new_state,'updated_ns':at_ns}
            c.execute('UPDATE execution_orders SET state=?,body=? WHERE id=?',
                      (new_state,json.dumps(order,allow_nan=False),order['id']))
            c.execute('INSERT INTO execution_events(order_id,at_ns,state,detail) VALUES (?,?,?,?)',
                      (order['id'],at_ns,new_state,json.dumps(detail or {},allow_nan=False)))
        return order

    def fills(self):
        with self.connect() as c:
            return [dict(r) for r in c.execute('SELECT * FROM execution_fills ORDER BY at_ns,id')]

    def metadata(self,identity,values):
        with self.connect() as c:
            if not c.in_transaction:
                c.execute('BEGIN IMMEDIATE')
            order = json.loads(c.execute('SELECT body FROM execution_orders WHERE id=?',(identity,)).fetchone()[0])
            if any(k in values for k in ('id','state','filled','quantity','notional','submitted_ns','updated_ns','max_quote')):
                raise ValueError('Execution invariants cannot be changed by metadata')
            order.update(values)
            c.execute('UPDATE execution_orders SET body=? WHERE id=?',(json.dumps(order,allow_nan=False),identity))
        return order

    def statistics(self,limit=None):
        if limit is None:
            orders = self.orders()
            states = {s:sum(o['state']==s for o in orders) for s in sorted({o['state'] for o in orders})}
        else:
            with self.connect() as c:
                states = dict(c.execute('SELECT state,COUNT(*) FROM execution_orders GROUP BY state'))
                orders = [json.loads(r[0]) for r in c.execute(
                    'SELECT body FROM execution_orders ORDER BY rowid DESC LIMIT ?',(int(limit),))]
        ack = [(o['ack_ns']-o['submitted_ns'])/1e6 for o in orders if o.get('ack_ns') is not None]
        first = [(o['first_fill_ns']-o['submitted_ns'])/1e6 for o in orders if o.get('first_fill_ns') is not None]
        cancel = [(o['updated_ns']-o['cancel_requested_ns'])/1e6 for o in orders if o['state']=='CANCELED' and o.get('cancel_requested_ns') is not None]
        def distribution(values):
            return {'count':len(values),'p50_ms':percentile(values,.5),'p95_ms':percentile(values,.95),
                    'p99_ms':percentile(values,.99),'max_ms':max(values) if values else None}
        return {'clock':'simulation receive clock; not exchange execution SLA',
                'orders':sum(states.values()),'states':states,
                'sampled_orders':len(orders),
                'zero_fill_orders':sum(number(o['filled'])==0 for o in orders),
                'zero_fill_ratio':(sum(number(o['filled'])==0 for o in orders)/len(orders)) if orders else None,
                'latency_scope':'all orders' if limit is None else f'latest {len(orders)} orders',
                'partial_orders_scope':'all orders' if limit is None else f'latest {len(orders)} orders',
                'acknowledgement':distribution(ack),'first_fill':distribution(first),'cancel':distribution(cancel),
                'partial_orders':sum(0<number(o['filled'])<number(o['quantity']) for o in orders)}


class SimulatedBroker:
    def __init__(self,journal,ack_ms=20,cancel_ms=20,fee_rate=.0005,participation=.1,on_fill=None,slippage=0):
        if any(not math.isfinite(v) or v<0 for v in (ack_ms,cancel_ms,fee_rate,slippage)) or not 0<participation<=1:
            raise ValueError('Invalid simulated execution parameters')
        self.journal,self.on_fill = journal,on_fill
        self.ack_ns,self.cancel_ns = int(ack_ms*1e6),int(cancel_ms*1e6)
        self.fee_rate,self.participation = number(fee_rate),number(participation)
        self.slippage = number(slippage)
        self.has_active = bool(journal.orders(active=True))
        state = journal.match_state()
        self.consumed = {(s,revision,side,price):number(qty)
                         for s,revision,side,price,qty in state['consumed']}
        self.last_trade = state['last_trade']
        self._checkpoint = self._match_state_json()

    def _match_state_json(self):
        return json.dumps({'consumed':[[*key,str(qty)] for key,qty in sorted(self.consumed.items())],
                           'last_trade':self.last_trade},sort_keys=True,allow_nan=False)

    def submit(self,identity,symbol,side,quantity,at_ns,order_type='MARKET',limit_price=None,tif='IOC',reduce_only=False,metadata=None,max_quote=None):
        if side not in (-1,1) or number(quantity)<=0 or order_type not in ('MARKET','LIMIT') or tif not in ('IOC','GTC'):
            raise ValueError('Invalid simulated order')
        if order_type=='LIMIT' and (limit_price is None or number(limit_price)<=0):
            raise ValueError('A limit order requires a positive price')
        if max_quote is not None and number(max_quote)<=0:
            raise ValueError('Quote budget must be positive')
        order = {'id':identity,'symbol':symbol,'side':side,'quantity':str(number(quantity)),
                 'filled':'0','notional':'0','type':order_type,'limit_price':str(number(limit_price)) if limit_price is not None else None,
                 'tif':tif,'reduce_only':bool(reduce_only),'submitted_ns':int(at_ns),'updated_ns':int(at_ns),
                 'ack_due_ns':int(at_ns)+self.ack_ns,'ack_ns':None,'first_fill_ns':None,'state':'SUBMITTED',
                 'queue_ahead':None,'cancel_requested_ns':None,'cancel_due_ns':None}
        if max_quote is not None:
            order['max_quote'] = str(number(max_quote))
        if metadata:
            if any(k in order for k in metadata):
                raise ValueError('Metadata cannot override order execution fields')
            order.update(metadata)
        result = self.journal.create(order)
        self.has_active = self.has_active or result['state'] not in TERMINAL
        return result

    def cancel(self,identity,at_ns):
        order = self.journal.get(identity)
        if not order or order['state'] in TERMINAL or order['cancel_requested_ns'] is not None:
            return order
        order |= {'cancel_requested_ns':int(at_ns),'cancel_due_ns':int(at_ns)+self.cancel_ns}
        return self.journal.change(order,'CANCEL_PENDING',int(at_ns))

    def fill(self,order,qty,price,at_ns,liquidity_id):
        qty,price = number(qty),number(price)
        remaining = number(order['quantity'])-number(order['filled'])
        qty = min(qty,remaining)
        if order.get('max_quote') is not None:
            # Include actual spread, price impact and adverse slippage in the
            # budget. Decimal floor avoids an over-budget fractional fill.
            with localcontext() as context:
                context.rounding = ROUND_DOWN
                affordable = (max(number(0),number(order['max_quote'])-number(order['notional']))/price).quantize(
                    Decimal('0.000000000001'),rounding=ROUND_DOWN)
                qty = min(qty,affordable)
        if qty<=0:
            return order
        filled = number(order['filled'])+qty
        order = order | {'filled':str(filled),'notional':str(number(order['notional'])+qty*price),
                         'first_fill_ns':order['first_fill_ns'] if order['first_fill_ns'] is not None else at_ns}
        fill = {'id':hashlib.sha256(f"{order['id']}:{liquidity_id}".encode()).hexdigest(),
                'quantity':str(qty),'price':str(price),'fee':str(qty*price*self.fee_rate),'at_ns':at_ns}
        new_state = 'FILLED' if filled==number(order['quantity']) else 'PARTIALLY_FILLED'
        return self.journal.change(order,new_state,at_ns,{'liquidity_id':str(liquidity_id)},fill,self.on_fill)

    def on_event(self,at_ns,books,trade=None):
        started = time.perf_counter_ns()
        if not self.has_active:
            return time.perf_counter_ns()-started
        previous = self.consumed.copy(),self.last_trade.copy(),self.has_active
        try:
            with self.journal.transaction():
                self._match_event(at_ns,books,trade)
                checkpoint = self._match_state_json()
                if checkpoint != self._checkpoint:
                    self.journal.save_match_state(checkpoint)
            self._checkpoint = checkpoint
        except BaseException:
            # SQLite rolled back all callbacks and journal changes. Restore the
            # in-memory mirror too, so retrying cannot lose or reuse liquidity.
            self.consumed,self.last_trade,self.has_active = previous
            raise
        return time.perf_counter_ns()-started

    def _match_event(self,at_ns,books,trade=None):
        trade_remaining = number(trade['q']) if trade else number(0)
        trade_identity = (trade['s'],int(trade['t'])) if trade else None
        if trade_identity and trade_identity[1]<=self.last_trade.get(trade_identity[0],-1):
            trade = None
            trade_remaining = number(0)
        if trade:
            self.last_trade[trade['s']] = int(trade['t'])
        orders = sorted(self.journal.orders(active=True),key=lambda o:(o['submitted_ns'],o['id']))
        if not orders:
            self.has_active = False
            return
        for order in orders:
            if at_ns < order['submitted_ns'] or order['state']=='UNKNOWN':
                continue
            if order['tif']=='IOC' and order['ack_ns'] is None and at_ns-order['submitted_ns']>2000000000:
                self.journal.change(order,'EXPIRED',at_ns,{'reason':'IOC intent expired before acknowledgement'})
                continue
            if order['cancel_due_ns'] is not None and at_ns>=order['cancel_due_ns']:
                self.journal.change(order,'CANCELED',at_ns)
                continue
            book = books.get(order['symbol'])
            if order['ack_ns'] is None:
                if at_ns<order['ack_due_ns']:
                    continue
                order['ack_ns'] = at_ns
                if order['state']=='CANCEL_PENDING':
                    # Preserve an in-flight cancellation after acknowledgement.
                    order = self.journal.change(order,'CANCEL_PENDING',at_ns,{'acknowledged':True})
                else:
                    order = self.journal.change(order,'ACKNOWLEDGED',at_ns)
            if not book or not book.ready or at_ns//1000000-book.updated_ms>2000:
                if order['tif']=='IOC':
                    self.journal.change(order,'EXPIRED',at_ns,{'reason':'No fresh reconstructed depth'})
                continue
            levels = book.asks if order['side']>0 else book.bids
            prices = sorted(levels,reverse=order['side']<0)
            limit = number(order['limit_price']) if order['limit_price'] is not None else None
            crossed = bool(prices and (limit is None or (number(prices[0])<=limit if order['side']>0 else number(prices[0])>=limit)))
            if crossed:
                for price in prices:
                    p = number(price)
                    if limit is not None and (p>limit if order['side']>0 else p<limit):
                        break
                    key = (order['symbol'],book.last,order['side'],price)
                    available = max(number(0),number(levels[price])*self.participation-self.consumed.get(key,number(0)))
                    qty = min(available,number(order['quantity'])-number(order['filled']))
                    if qty>0:
                        fill_price = p*(1+order['side']*self.slippage)
                        if limit is not None and (fill_price>limit if order['side']>0 else fill_price<limit):
                            break
                        before = number(order['filled'])
                        order = self.fill(order,qty,fill_price,at_ns,f'depth:{book.last}:{price}')
                        self.consumed[key] = self.consumed.get(key,number(0))+number(order['filled'])-before
                    if order['state']=='FILLED':
                        break
                if order['state']=='FILLED':
                    continue
            if order['tif']=='IOC' or order['type']=='MARKET':
                self.journal.change(order,'EXPIRED',at_ns,{'unfilled':str(number(order['quantity'])-number(order['filled']))})
                continue
            if order['queue_ahead'] is None:
                own_levels = book.bids if order['side']>0 else book.asks
                order['queue_ahead'] = str(number(own_levels.get(float(limit),0)))
                # Persist queue establishment without pretending a fill occurred.
                with self.journal.connect() as c:
                    c.execute('UPDATE execution_orders SET body=? WHERE id=?',(json.dumps(order),order['id']))
                continue
            if trade and trade['s']==order['symbol'] and trade_remaining>0:
                aggressor = -1 if trade['m'] else 1
                p = number(trade['p'])
                touches = aggressor == -order['side'] and (p<=limit if order['side']>0 else p>=limit)
                if touches:
                    ahead = number(order['queue_ahead'])
                    consumed = min(ahead,trade_remaining)
                    ahead -= consumed
                    trade_remaining -= consumed
                    order['queue_ahead'] = str(ahead)
                    if trade_remaining>0:
                        qty = min(trade_remaining,number(order['quantity'])-number(order['filled']))
                        before = number(order['filled'])
                        order = self.fill(order,qty,limit,at_ns,f"trade:{trade['t']}")
                        trade_remaining -= number(order['filled'])-before
                    else:
                        with self.journal.connect() as c:
                            c.execute('UPDATE execution_orders SET body=? WHERE id=?',(json.dumps(order),order['id']))
        # A temporarily missing book must not erase its consumption checkpoint.
        # Drop an old revision only after observing a strictly newer one.
        self.consumed = {k:v for k,v in self.consumed.items()
                         if k[0] not in books or books[k[0]].last is None or k[1]>=books[k[0]].last}
