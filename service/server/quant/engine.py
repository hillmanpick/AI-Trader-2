import hashlib
import json
import math
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from .notifier import local_time
from .strategies import signal, market_regime, volatility_plan, update_dynamic_exit
from .store import utcnow
from .ai_review import DeepSeekReviewer


def paper_margin_used(position):
    return position.get('initial_margin', abs(position['amount'])*position['entry']/position.get('leverage',1))


class Engine:
    def __init__(self,cfg,store,exchange):
        self.cfg,self.store,self.exchange = cfg,store,exchange
        self.ready = False
        self.candle_cache = {}
        self.reviewer = DeepSeekReviewer(cfg)
        self.review_jobs = {}
        self.review_pool = None

    def review_entry(self,symbol,action,price,bars,bar_id):
        if not self.cfg.ai_enabled:
            self.execute(symbol,action,price,self.cfg.strategy,str(bar_id))
            return
        slots = 4 if self.cfg.mode == 'paper' and self.cfg.strategy == 'adaptive' else 2
        if symbol in self.review_jobs or len(self.review_jobs) >= slots:
            return False
        candidate = {'symbol':symbol,'action':action,'strategy':self.cfg.strategy,'interval':self.cfg.interval,
                     'price':price,'closed_prices':[float(r[4]) for r in bars[-40:]],'bar':bar_id,
                     'valid_until':2*int(bars[-1][6])-int(bars[-1][0])+1}
        if self.cfg.strategy in ('sma_cross', 'adaptive'):
            closes = [float(r[4]) for r in bars]
            candidate['sma'] = {'fast_period': self.cfg.fast, 'slow_period': self.cfg.slow,
                                'fast': sum(closes[-self.cfg.fast:])/self.cfg.fast,
                                'slow': sum(closes[-self.cfg.slow:])/self.cfg.slow,
                                'previous_fast': sum(closes[-self.cfg.fast-1:-1])/self.cfg.fast,
                                'previous_slow': sum(closes[-self.cfg.slow-1:-1])/self.cfg.slow}
            if self.cfg.strategy == 'adaptive':
                from statistics import fmean, pstdev
                candidate['regime'] = market_regime(closes)
                window = closes[-self.cfg.boll_period:]
                candidate['range'] = {'mean': fmean(window), 'width': pstdev(window)*self.cfg.boll_deviation,
                                      'period': self.cfg.boll_period}
        if not self.cfg.deepseek_api_key:
            self.store.set(f'ai:{symbol}',self.reviewer.review(candidate,{}) | {'at':utcnow(),'action':action,'bar':bar_id})
            return
        if self.review_pool is None:
            self.review_pool = ThreadPoolExecutor(max_workers=slots,thread_name_prefix='entry-review')
        snapshot = self.store.get('community',{}) if self.cfg.community_enabled else {}
        future = self.review_pool.submit(self.reviewer.review,candidate,snapshot)
        self.review_jobs[symbol] = (future,time.monotonic(),candidate)
        self.store.set(f'ai:{symbol}',{'status':'pending','reason':'DeepSeek 正在审核开仓信号','at':utcnow(),'action':action,'bar':bar_id})
        return True

    def finish_review(self,symbol,price):
        job = self.review_jobs.get(symbol)
        if not job:
            return
        future,started,candidate = job
        expired = (time.monotonic()-started > self.cfg.ai_review_max_age
                   or int(time.time()*1000)+self.exchange.offset >= candidate.get('valid_until',float('inf')))
        if not expired and not future.done():
            return
        # A timed-out task stays attached until finished, bounding queued model calls.
        if expired:
            future.cancel()
            decision = {'status':'expired','decision':'reject','confidence':0,'reason':'审核超过有效期，本次不开仓'}
        else:
            try:
                decision = future.result()
            except Exception:
                decision = {'status':'error','decision':'reject','confidence':0,'reason':'审核失败，本次不开仓'}
        previous = self.store.get(f'ai:{symbol}',{})
        self.store.set(f'ai:{symbol}',decision | {'at':utcnow(),'action':candidate['action'],'bar':candidate['bar']})
        if previous.get('status') != 'expired':
            self.store.event('AI 开仓审核',f"交易对：{symbol}\n候选方向：{'做多' if candidate['action']=='long' else '做空'}\n审核结果：{'放行' if decision.get('decision')=='allow' else '不放行'}\n原因：{decision['reason']}")
        if future.done():
            del self.review_jobs[symbol]
        if not expired and decision.get('decision') == 'allow' and not self.store.get('positions',{}).get(symbol):
            # Recheck current funds/exposure/pausing and use the current market price.
            self.execute(symbol,candidate['action'],price,self.cfg.strategy,str(candidate['bar']))

    def initialize(self):
        if self.cfg.observation_only:
            raise RuntimeError('Observation mode cannot initialize the execution engine')
        self.cfg.validate()
        fingerprint = {'mode':self.cfg.mode,'key_id':hashlib.sha256(self.cfg.api_key.encode()).hexdigest() if self.cfg.mode!='paper' else 'paper'}
        saved = self.store.get('identity')
        if saved and saved != fingerprint:
            raise ValueError('Mode/account changed: use a separate QUANT_DB_PATH; preserve the old journal')
        self.store.set('identity',fingerprint)
        if self.cfg.mode == 'paper':
            previous = self.store.get('config',{})
            positions = self.store.get('positions',{})
            for p in positions.values():
                # Freeze legacy risk before publishing the new entry configuration.
                p.setdefault('leverage', previous.get('leverage',1))
                p.setdefault('initial_margin', abs(p['amount'])*p['entry']/p['leverage'])
                p.setdefault('stop_pct', previous.get('stop_pct',self.cfg.stop_pct))
                p.setdefault('take_pct', previous.get('take_pct',self.cfg.take_pct))
                p.setdefault('dynamic_exits',False)
            self.store.set('positions',positions)
        self.store.set('config',self.cfg.public())
        self.exchange.sync_time()
        self.exchange.load_rules()
        if self.cfg.mode != 'paper':
            # Read and reconcile before changing any exchange settings.
            self.reconcile()
            for symbol,p in self.store.get('positions',{}).items():
                self.ensure_protection(symbol,p)
            self.recover_orders()
            self.exchange.prepare_account()
        elif self.store.get('paper_balance') is None:
            self.store.set('paper_balance',self.cfg.paper_cash)
        if self.cfg.mode == 'paper':
            for order in self.store.orders(pending=True):
                self.store.order_result(order['id'],'REJECTED',{'reason':'Paper intent interrupted before atomic fill'})
        self.ready = True

    def recover_orders(self):
        for order in self.store.orders(pending=True):
            result = self.exchange.query_order(order['id'],order['symbol'])
            status = result.get('status')
            if status in {'NEW','PARTIALLY_FILLED'}:
                self.exchange.cancel_order(order['id'],order['symbol'])
                result = self.exchange.query_order(order['id'],order['symbol'])
                status = result.get('status')
            if status not in {'FILLED','CANCELED','EXPIRED','EXPIRED_IN_MATCH','REJECTED'}:
                raise RuntimeError('Unresolved order; new orders are blocked')
            self.store.order_result(order['id'],status,result)

    def reconcile(self):
        rows = self.exchange.positions()
        old = self.store.get('positions',{})
        managed = set(self.store.get('managed_symbols',[]))
        current = {}
        unknown = []
        for r in rows:
            amount = float(r['positionAmt'])
            if not amount:
                continue
            symbol = r['symbol']
            if r.get('positionSide') != 'BOTH':
                raise ValueError('Hedge-mode position detected; one-way mode is required')
            current[symbol] = {'amount':amount,'entry':float(r['entryPrice']),'mark':float(r['markPrice']),'pnl':float(r['unRealizedProfit'])}
            if symbol not in self.cfg.symbols or symbol not in managed:
                unknown.append(symbol)
        # All position changes (including stop fills and manual flattening) generate mail.
        for symbol in set(old)|set(current):
            before,after = old.get(symbol,{}).get('amount',0),current.get(symbol,{}).get('amount',0)
            if before == after:
                continue
            kind = '开仓' if not before else '清仓' if not after else '持仓变更'
            self.store.event(kind,f'{symbol}: {before} → {after}; 当前={json.dumps(current.get(symbol,{}),ensure_ascii=False)}')
        self.store.set('positions',current)
        account = self.exchange.account()
        self.store.set('account',{'equity':float(account['totalMarginBalance']),'available':float(account['availableBalance']),'wallet':float(account['totalWalletBalance']),'updated_at':utcnow()})
        if unknown:
            raise ValueError('Unmanaged positions detected: '+','.join(unknown)+'; use a dedicated account/subaccount, no automatic takeover')
        pending_symbols = {o['symbol'] for o in self.store.orders(pending=True)}
        # Exchange position snapshots may lag a just-filled order. Keep ownership
        # until a previously observed open position is observed flat.
        self.store.set('managed_symbols',sorted(managed - ((set(old)-set(current))-pending_symbols)))
        # Clear bot-owned stale close-all triggers before any future entry.
        for symbol in set(old)-set(current):
            self.exchange.clear_protection(symbol)
        return current

    def daily_blocked(self,equity):
        today = local_time().date().isoformat()
        day = self.store.get('risk_day',{})
        if day.get('date') != today:
            day = {'date':today,'equity':equity,'blocked':False}
            self.store.set('risk_day',day)
        if day['equity']-equity >= self.cfg.daily_loss and not day.get('blocked'):
            day['blocked'] = True
            self.store.set('risk_day',day)
            self.store.event('风控告警','每日权益亏损上限已触发，北京时间次日之前停止开新仓，已有保护继续运行')
        return day.get('blocked',False)

    def order_id(self,symbol,action,key):
        return 'qt_' + hashlib.sha256(f'{self.cfg.mode}:{symbol}:{action}:{key}'.encode()).hexdigest()[:28]

    def live_order(self,symbol,amount,reason,key):
        # amount > 0 => buy, amount < 0 => sell. Never reverse in one order.
        positions = self.store.get('positions',{})
        position = positions.get(symbol)
        params = {'symbol':symbol,'side':'BUY' if amount>0 else 'SELL','quantity':format(abs(amount),'.12f').rstrip('0').rstrip('.')}
        if position:
            params['reduceOnly'] = 'true'
        oid = self.order_id(symbol,reason,key)
        self.store.intent(oid,symbol,params | {'reason':reason})
        if not position:
            self.store.set('managed_symbols',sorted(set(self.store.get('managed_symbols',[]))|{symbol}))
        # A timeout leaves PENDING; only exchange query can resolve it.
        result = self.exchange.order(oid,params)
        if result.get('status') not in {'FILLED','CANCELED','EXPIRED','REJECTED'}:
            self.recover_orders()
        else:
            self.store.order_result(oid,result['status'],result)
        self.reconcile()
        latest = self.store.get('positions',{}).get(symbol)
        if not position and float(result.get('executedQty',0)) > 0 and not latest:
            self.store.set('paused',True)
            raise RuntimeError('Filled order not yet visible in position snapshot; entry paused pending reconciliation')

    def paper_order(self,symbol,amount,price,reason,key):
        # Adverse fill for both entries and exits; marks remain unslipped.
        price *= 1+self.cfg.paper_slippage*(1 if amount > 0 else -1)
        positions = self.store.get('positions',{})
        old = positions.get(symbol)
        risk_plan = self.store.get(f'volatility:{symbol}',{}) if self.cfg.paper_dynamic_exits else {}
        balance = self.store.get('paper_balance',self.cfg.paper_cash)
        pnl = (price-old['entry'])*old['amount'] if old else 0
        fee = abs(amount)*price*self.cfg.fee_rate
        oid = self.order_id(symbol,reason,key)
        self.store.intent(oid,symbol,{'amount':amount,'price':price,'reason':reason})
        if old:
            del positions[symbol]
        else:
            positions[symbol] = {'amount':amount,'entry':price,'mark':price,'pnl':0,
                                 'leverage':self.cfg.leverage, 'stop_pct':risk_plan.get('stop_pct',self.cfg.stop_pct),
                                 'take_pct':risk_plan.get('take_pct',self.cfg.take_pct), 'opened_at':utcnow(),
                                 'initial_margin': self.cfg.paper_margin or abs(amount)*price/self.cfg.leverage,
                                 'maintenance_rate':self.cfg.paper_maintenance_rate,
                                 'dynamic_exits':bool(risk_plan), 'exit_plan':risk_plan}
        # Single transaction: simulated fill, wallet, position, and notification.
        with self.store.connect() as c:
            # Simplified isolated liquidation: loss cannot draw other positions' collateral.
            if old and reason == 'paper_liquidation':
                pnl = max(pnl-fee,-paper_margin_used(old))+fee
            wallet = balance+pnl-fee
            equity = wallet+sum((p.get('mark',p['entry'])-p['entry'])*p['amount'] for p in positions.values())
            margin = sum(paper_margin_used(p) for p in positions.values())
            account = {'wallet':wallet,'equity':equity,'available':equity-margin,'updated_at':utcnow()}
            for k,v in [('positions',positions),('paper_balance',wallet),('account',account)]:
                c.execute('INSERT INTO kv VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',(k,json.dumps(v)))
            c.execute('UPDATE orders SET status=?,response=? WHERE id=?',('FILLED',json.dumps({'price':price,'amount':amount,'pnl':pnl,'fee':fee}),oid))
            c.execute('INSERT INTO events(kind,body,created_at) VALUES (?,?,?)',('清仓' if old else '开仓',f'{symbol} 数量={amount} 价格={price} 原因={reason} 已实现盈亏={pnl} 手续费={fee} USDT（模拟，不含资金费）',utcnow()))

    def execute(self,symbol,action,price,reason,key):
        if self.cfg.observation_only:
            raise RuntimeError('Observation mode cannot execute trades')
        positions = self.store.get('positions',{})
        old = positions.get(symbol)
        if action == 'close':
            if not old:
                return
            amount = -old['amount']
        else:
            if old:
                return
            account = self.store.get('account',{})
            equity = float(account.get('equity',0))
            if not self.cfg.enabled or self.store.get('paused',False) or self.daily_blocked(equity):
                return
            if len(positions) >= self.cfg.max_positions:
                return
            exposure = sum(abs(p['amount'])*p['mark'] for p in positions.values())
            if exposure+self.cfg.order_notional > self.cfg.max_notional:
                return
            if self.cfg.mode == 'paper' and self.cfg.paper_max_margin:
                if sum(paper_margin_used(p) for p in positions.values())+self.cfg.paper_margin > self.cfg.paper_max_margin:
                    return
            required = self.cfg.order_notional/self.cfg.leverage + self.cfg.order_notional*0.002
            if float(account.get('available',0)) < required:
                return
            amount = float(self.exchange.quantity(symbol,self.cfg.order_notional,price)) * (1 if action=='long' else -1)
        if self.cfg.mode == 'paper':
            self.paper_order(symbol,amount,price,reason,key)
        else:
            if not old:
                # No old protective orders may survive into a new position.
                self.exchange.clear_protection(symbol)
            self.live_order(symbol,amount,reason,key)
            p = self.store.get('positions',{}).get(symbol)
            if p:
                self.ensure_protection(symbol,p)
            else:
                self.exchange.clear_protection(symbol)

    def ensure_protection(self,symbol,p):
        try:
            self.exchange.protect(symbol,p['amount'],p['entry'])
        except Exception:
            self.store.set('paused',True)
            self.store.event('风控告警',f'{symbol} 保护单校验/创建失败，暂停开新仓并尝试 reduceOnly 清仓')
            self.live_order(symbol,-p['amount'],'protection_failure',utcnow())
            raise RuntimeError('Protective order failed; emergency close requested, operator review required') from None

    def step(self):
        if self.cfg.observation_only:
            return 'observing'
        if not self.ready:
            self.initialize()
        if self.exchange.last_sync < datetime.now(timezone.utc).timestamp()-1800:
            self.exchange.sync_time()
        if self.cfg.mode != 'paper':
            self.reconcile()
            for symbol,p in self.store.get('positions',{}).items():
                self.ensure_protection(symbol,p)
            self.recover_orders()
        positions = self.store.get('positions',{})
        marks = {s:self.exchange.mark(s) for s in self.cfg.symbols}
        if any(not math.isfinite(price) or price <= 0 for price in marks.values()):
            raise ValueError('Invalid mark price')
        for symbol,p in list(positions.items()):
            if symbol not in marks:
                raise ValueError('Configured symbols changed while positions exist; restore symbols before running')
            p['mark'] = marks[symbol]
            p['pnl'] = (p['mark']-p['entry'])*p['amount']
            if (self.cfg.mode == 'paper' and p.get('leverage',1) > 1
                    and paper_margin_used(p)+p['pnl'] <= abs(p['amount'])*p['mark']*p.get('maintenance_rate',self.cfg.paper_maintenance_rate)):
                self.paper_order(symbol,-p['amount'],p['mark'],'paper_liquidation',utcnow())
                del positions[symbol]
        self.store.set('positions',positions)
        if self.cfg.mode == 'paper':
            wallet = self.store.get('paper_balance',self.cfg.paper_cash)
            margin = sum(paper_margin_used(p) for p in positions.values())
            equity = wallet+sum(p['pnl'] for p in positions.values())
            self.store.set('account',{'wallet':wallet,'equity':equity,'available':equity-margin,'updated_at':utcnow()})
        else:
            equity = self.store.get('account')['equity']
        loss_blocked = self.daily_blocked(equity)
        self.store.set('daily_loss_blocked',loss_blocked)
        for symbol in self.cfg.symbols:
            price = marks[symbol]
            if not math.isfinite(price) or price <= 0:
                raise ValueError('Invalid mark price')
            p = self.store.get('positions',{}).get(symbol)
            if p:
                if self.cfg.mode == 'paper' and p.get('dynamic_exits'):
                    plan = self.store.get(f'volatility:{symbol}', p['exit_plan'])
                    exit_reason = update_dynamic_exit(p,plan,self.cfg.fee_rate,self.cfg.paper_slippage)
                    positions = self.store.get('positions',{})
                    positions[symbol] = p
                    self.store.set('positions',positions)
                    if exit_reason:
                        self.execute(symbol,'close',price,exit_reason,utcnow())
                        continue
                move = (price/p['entry']-1)*(1 if p['amount']>0 else -1)
                stop_pct = p.get('stop_pct', self.cfg.stop_pct) if self.cfg.mode == 'paper' else self.cfg.stop_pct
                take_pct = p.get('take_pct', self.cfg.take_pct) if self.cfg.mode == 'paper' else self.cfg.take_pct
                if not p.get('dynamic_exits') and (move <= -stop_pct or move >= take_pct):
                    self.execute(symbol,'close',price,'stop_loss' if move<0 else 'take_profit',utcnow())
                    continue
                if self.cfg.mode != 'paper':
                    self.ensure_protection(symbol,p)
            self.finish_review(symbol,price)
            p = self.store.get('positions',{}).get(symbol)
            # Cache closed candles until next expected close; poll mark prices separately.
            now = int(datetime.now(timezone.utc).timestamp()*1000)+self.exchange.offset
            cached = self.candle_cache.get(symbol)
            if cached and now < cached[0]:
                bars = cached[1]
            else:
                bars = self.exchange.closed_candles(symbol)
                if len(bars) < max(self.cfg.slow,self.cfg.boll_period)+2:
                    raise ValueError('Insufficient closed candles')
                duration = int(bars[-1][6])-int(bars[-1][0])+1
                if now-int(bars[-1][6]) > duration+5000:
                    raise ValueError('Stale closed candles; new orders blocked')
                self.candle_cache[symbol] = (int(bars[-1][6])+duration+1500,bars)
            bar_id = int(bars[-1][0])
            if self.cfg.mode == 'paper' and self.cfg.paper_dynamic_exits:
                self.store.set(f'volatility:{symbol}',volatility_plan(bars,price,self.cfg.leverage,self.cfg.paper_maintenance_rate))
            last = self.store.get(f'bar:{symbol}')
            if last is None:
                # Startup observes current candle, never replays historical entries.
                self.store.set(f'bar:{symbol}',bar_id)
                continue
            if last >= bar_id:
                continue
            # Persist consumption before execution. Ambiguous failures never replay a signal.
            self.store.set(f'bar:{symbol}',bar_id)
            action = signal([float(r[4]) for r in bars],p['amount'] if p else 0,self.cfg)
            self.store.set(f'signal:{symbol}',{'action':action,'bar':bar_id,'price':price,'at':utcnow()})
            if action in ('long','short'):
                # Avoid sending model requests when deterministic entry gates already block trading.
                account = self.store.get('account',{})
                positions = self.store.get('positions',{})
                exposure = sum(abs(pos['amount'])*pos['mark'] for pos in positions.values())
                if (self.cfg.enabled and not self.store.get('paused',False) and not loss_blocked
                        and len(positions)<self.cfg.max_positions and exposure+self.cfg.order_notional<=self.cfg.max_notional
                        and (not self.cfg.paper_max_margin or self.cfg.mode != 'paper'
                             or sum(paper_margin_used(pos) for pos in positions.values())+self.cfg.paper_margin<=self.cfg.paper_max_margin)
                        and float(account.get('available',0)) >= self.cfg.order_notional/self.cfg.leverage+self.cfg.order_notional*0.002):
                    if self.review_entry(symbol,action,price,bars,bar_id) is False:
                        # Retry a busy review slot within this candle, without losing the signal.
                        self.store.set(f'bar:{symbol}',last)
            elif action == 'close':
                self.execute(symbol,action,price,self.cfg.strategy,str(bar_id))
        return 'running' if self.cfg.enabled and not self.store.get('paused',False) else 'paused_entries'
