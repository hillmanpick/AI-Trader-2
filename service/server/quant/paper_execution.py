"""Real-time paper IOC fills through the same journal and depth matcher as replay."""
import json
import math
import time
from datetime import datetime,timezone

from .execution_state import OrderJournal,SimulatedBroker,number,TERMINAL
from .order_book import DepthBook


def utc(stamp):
    return datetime.fromtimestamp(stamp/1e9,timezone.utc).isoformat()


class PaperExecution:
    def __init__(self,engine,bus):
        self.engine,self.bus = engine,bus
        self.journal = OrderJournal(engine.store.path)
        self.broker = SimulatedBroker(self.journal,ack_ms=20,cancel_ms=20,
                                     fee_rate=engine.cfg.fee_rate,
                                     participation=engine.cfg.paper_depth_participation,on_fill=self.apply_fill,
                                     slippage=engine.cfg.paper_slippage)
        # An interrupted paper intent is known virtual state; recover it through
        # fresh depth rather than submit a second exchange or simulated order.

    def pending(self,symbol):
        return any(o['symbol']==symbol for o in self.journal.orders(active=True))

    def submit(self,symbol,amount,price,reason,key):
        e = self.engine
        positions = e.store.get('positions',{})
        existing = positions.get(symbol)
        if self.pending(symbol):
            return
        closing = bool(existing)
        oid = e.order_id(symbol,reason,key)
        at = time.time_ns()
        context = e.store.get('entry_context:'+symbol,{})
        e.store.intent(oid,symbol,{'amount':amount,'price':price,'reason':reason,'execution':'paper_depth_ioc'})
        self.broker.submit(oid,symbol,1 if amount>0 else -1,abs(amount),at,reduce_only=closing,
                           metadata={'paper_context':context,'reason':reason,'closing':closing})
        return oid

    def apply_fill(self,c,fill,order):
        e = self.engine
        def get(key,default):
            row = c.execute('SELECT value FROM kv WHERE key=?',(key,)).fetchone()
            return json.loads(row[0]) if row else default
        def put(key,value):
            c.execute('INSERT INTO kv VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                      (key,json.dumps(value,allow_nan=False)))
        positions = get('positions',{})
        old = positions.get(order['symbol'])
        qty,price,fee = float(fill['quantity']),float(fill['price']),float(fill['fee'])
        amount = qty*order['side']
        wallet = float(get('paper_balance',e.cfg.paper_cash))
        if order['closing']:
            if not old or old['amount']*amount>=0 or qty>abs(old['amount'])+1e-10:
                raise ValueError('Paper reduce-only fill cannot open or reverse a position')
            pnl = (price-old['entry'])*(-amount)
            if order.get('reason')=='paper_liquidation':
                allocated_margin = old['initial_margin']*qty/abs(old['amount'])
                pnl = max(pnl-fee,-allocated_margin)+fee
            wallet += pnl-fee
            remaining = old['amount']+amount
            if abs(remaining)<max(1e-10,abs(old['amount'])*1e-10):
                del positions[order['symbol']]
            else:
                factor = abs(remaining/old['amount'])
                positions[order['symbol']] = old | {'amount':remaining,'initial_margin':old['initial_margin']*factor,
                    'entry_fee':old.get('entry_fee',0)*factor,'pnl':(old.get('mark',price)-old['entry'])*remaining}
            closed = get('execution_closed_fills',[])
            closed.append({'at':utc(fill['at_ns']),'symbol':order['symbol'],
                           'fill_id':fill['id'],'entry_fee':old.get('entry_fee',0)*qty/abs(old['amount']),
                           'quantity':qty,'gross_pnl':pnl,'fee':fee,'reason':order.get('reason'),
                           'model_version':old.get('model_version')})
            put('execution_closed_fills',closed[-1000:])
        else:
            if old and old['amount']*amount<=0:
                raise ValueError('Paper entry fill cannot reverse a position')
            context = order.get('paper_context',{})
            plan = context.get('entry_profile',{})
            leverage = int(context.get('leverage',e.cfg.leverage))
            total = (abs(old['amount']) if old else 0)+qty
            entry = ((abs(old['amount'])*old['entry'] if old else 0)+qty*price)/total
            side = order['side']
            p = old or {'leverage':leverage,'stop_pct':plan['stop_pct'],'take_pct':plan['take_pct'],
                        'opened_at':utc(order['submitted_ns']),'maintenance_rate':e.cfg.paper_maintenance_rate,
                        'dynamic_exits':True,'exit_plan':plan,'model_version':context.get('model_version'),
                        'feature_ts':context.get('feature_ts'),'strategy_version':e.reflection.active().get('version',1),
                        'entry_regime':context.get('regime','microstructure'),'mae_pct':0,'mfe_pct':0}
            p = p | {'amount':side*total,'entry':entry,'mark':price,'pnl':0,
                     'initial_margin':(old.get('initial_margin',0) if old else 0)+qty*price/leverage,
                     'entry_fee':(old.get('entry_fee',0) if old else 0)+fee,
                     'stop_price':entry*(1-side*plan['stop_pct']),
                     'take_price':entry*(1+side*plan['take_pct'])}
            positions[order['symbol']] = p
            wallet -= fee
        equity = wallet+sum((p.get('mark',p['entry'])-p['entry'])*p['amount'] for p in positions.values())
        margin = sum(p['initial_margin'] for p in positions.values())
        put('positions',positions)
        put('paper_balance',wallet)
        put('account',{'wallet':wallet,'equity':equity,'available':equity-margin,'updated_at':utc(time.time_ns())})
        c.execute('INSERT INTO events(kind,body,created_at) VALUES (?,?,?)',
                  ('清仓成交' if order['closing'] else '开仓成交',
                   f"{order['symbol']} 模拟部分/全部成交 数量={qty} 价格={price} 手续费={fee} USDT",utc(fill['at_ns'])))

    def tick(self):
        orders = self.journal.orders(active=True)
        if not orders:
            return
        now = time.time_ns()
        books = {}
        for symbol in {o['symbol'] for o in orders}:
            try:
                raw = self.bus.get('book:'+symbol)
                if raw and 0<=now//1000000-raw['updated_ms']<=2000:
                    book = DepthBook()
                    book.seed(raw,raw['updated_ms'])
                    book.ready = bool(book.bids and book.asks and max(book.bids)<min(book.asks))
                    books[symbol] = book
            except Exception:
                pass
        self.broker.on_event(now,books)
        for previous in orders:
            order = self.journal.get(previous['id'])
            if order['state'] in TERMINAL:
                self.engine.store.order_result(order['id'],order['state'],{
                    'executedQty':order['filled'],'quoteQty':order['notional'],'execution':'paper_depth_ioc'})
        self.engine.store.set('execution_statistics',self.journal.statistics())
        seen = set(self.engine.store.get('reflection:execution_processed',[]))
        for closed in self.engine.store.get('execution_closed_fills',[]):
            if closed['fill_id'] not in seen:
                self.engine.reflection.record_closed(closed | {
                    'exit_reason':closed['reason'],'cost_usdt':closed['entry_fee']+closed['fee'],
                    'net_pnl':closed['gross_pnl']-closed['entry_fee']-closed['fee']})
                seen.add(closed['fill_id'])
        recent_ids = [t['fill_id'] for t in self.engine.store.get('execution_closed_fills',[]) if t['fill_id'] in seen]
        self.engine.store.set('reflection:execution_processed',recent_ids[-1000:])
