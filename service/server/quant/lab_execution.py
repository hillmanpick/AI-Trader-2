"""Paper-only model gate and dynamically selected public-data universe."""
import time
import threading
from dataclasses import replace

from .lab_config import LabConfig
from .lab_model import model_status, decide
from .lab_store import RealtimeBus
from .store import utcnow
from .lab_features import SCHEMA, FEATURES
import math


class LabExecution:
    def __init__(self, engine):
        self.engine = engine
        self.cfg = LabConfig().validate()
        self.bus = RealtimeBus(self.cfg.redis_url)
        self.last_decision = {}
        self.model, self.status = None, {}
        self.last_refresh = 0
        self.event_cursor = '0-0'
        self.autopilot = None
        if engine.cfg.paper_ai_autopilot:
            from .paper_autopilot import PaperAutopilot
            self.autopilot = PaperAutopilot(engine,self)

    def start_event_listener(self, wake):
        def listen():
            while True:
                try:
                    events = self.bus.client.xread({'quant:lab:events':self.event_cursor},count=100,block=500)
                    if events:
                        self.event_cursor = events[-1][1][-1][0]
                        wake.set()
                except Exception:
                    time.sleep(1)
        threading.Thread(target=listen,name='paper-redis-events',daemon=True).start()

    def refresh(self):
        if time.time()-self.last_refresh < 5:
            return
        self.last_refresh = time.time()
        self.status,self.model = model_status(self.cfg)
        e = self.engine
        if e.cfg.mode != 'paper' or e.cfg.api_key or e.cfg.api_secret or e.cfg.live_enabled:
            raise RuntimeError('Model execution cannot access a real account')
        candidate_ok = False
        try:
            candidate = self.bus.get('candidates') or {}
            if not 0 <= time.time()*1000-candidate.get('ts',0) <= 650000:
                raise ValueError('Candidate pool stale')
            candidate_ok = True
            symbols = tuple(candidate.get('symbols',[]))
            # Preserve open-position exits and in-flight reviews on pool rotation.
            symbols = tuple(dict.fromkeys(symbols+tuple(e.store.get('positions',{}))+tuple(e.review_jobs)))
            if symbols:
                e.base_cfg = replace(e.base_cfg,symbols=symbols)
                e.cfg = replace(e.cfg,symbols=symbols)
                if getattr(e.exchange,'market_stream',None):
                    e.exchange.market_stream.set_symbols(symbols)
                missing = [s for s in symbols if s not in e.exchange.rules]
                if e.ready and missing:
                    e.exchange.load_rules()
                e.store.set('config',e.cfg.public())
        except Exception:
            self.status = {'ready':False,'reason':'Redis 或动态币池暂不可用，禁止新仓'}
            self.model = None
        if self.autopilot:
            state = self.bus.get('state') or {}
            self.status = {'ready':bool(candidate_ok and self.cfg.enabled and 0 <= time.time()*1000-state.get('ts',0) <= 15000
                                       and state.get('ready_symbols')),
                'approved':False,'exploration':True,'decision_source':'deepseek_autopilot',
                'reason':'DS 自主模拟：使用实时盘口数据；不代表策略已通过盈利验证'}
        e.store.set('lab_gate',self.status | {'at':utcnow()})

    def row(self,symbol):
        row = self.bus.get('feature:'+symbol)
        if (not row or not 0 <= time.time()*1000-row['ts'] <= 10000
                or not row['meta'].get('quality') or row['meta'].get('schema') != SCHEMA
                or not row['meta'].get('entry_allowed',True)
                or row.get('symbol') != symbol or len(row.get('vector',[])) != len(FEATURES)
                or not all(math.isfinite(v) for v in row['vector'])
                or not math.isfinite(row['price']) or row['price'] <= 0):
            raise ValueError('Features stale or invalid')
        return row

    def risk_cap(self,symbol,side,maximum):
        from .correlation_risk import cap_notional
        e = self.engine
        snapshot = self.bus.get('portfolio_returns')
        return cap_notional(snapshot,e.store.get('positions',{}),symbol,side,maximum,
                            e.cfg.paper_max_margin*e.cfg.paper_risk_fraction,
                            int(time.time()*1000),self.cfg.horizon_seconds)

    def gate(self,symbol):
        self.refresh()
        if not self.cfg.enabled or not self.status.get('ready') or (not self.model and not getattr(self,'autopilot',None)):
            return False
        if self.status.get('exploration') and not self.engine.cfg.paper_exploration_mode:
            return False
        try:
            state = self.bus.get('state') or {}
            candidates = self.bus.get('candidates') or {}
            if symbol not in candidates.get('symbols',[]):
                return False
            if not 0 <= time.time()*1000-state.get('ts',0) <= 15000 or symbol not in state.get('ready_symbols',[]):
                return False
            self.row(symbol)
            return True
        except Exception:
            return False

    def signal(self,symbol):
        e = self.engine
        if not self.gate(symbol):
            e.store.set(f'signal:{symbol}',{'action':'hold','reason':self.status.get('reason') if not self.status.get('ready') else '实时特征或盘口尚未就绪',
                                          'at':utcnow(),'source':'numeric_model'})
            return 'hold'
        try:
            row = self.row(symbol)
            if getattr(self,'autopilot',None):
                return self.autopilot.signal(symbol)
            if self.last_decision.get(symbol) == row['ts']:
                return 'hold'
            self.last_decision[symbol] = row['ts']
            decision = decide(self.model,row)
            # Expected target must cover both fees, spread and adverse slippage.
            target = max(.0026,row['meta']['atr_proxy']*3)
            cost = 2*(e.cfg.fee_rate+e.cfg.paper_slippage)+row['meta']['spread_bps']/10000
            if decision['action'] in ('long','short') and target <= cost*1.5:
                decision['action'] = 'hold'
                decision['reason'] = 'Cost exceeds expected target space'
            action = decision['action']
            # Bound correlated directional concentration across the shared pool.
            direction = 1 if action == 'long' else -1
            same_side = sum((p['amount']>0) == (direction>0) for p in e.store.get('positions',{}).values())
            if action in ('long','short') and same_side >= 2:
                action = decision['action'] = 'hold'
                decision['reason'] = 'Directional concentration limit'
            e.store.set(f'signal:{symbol}',decision | {'price':row['price'],'at':utcnow(),'source':'numeric_model'})
            e.store.set(f'entry_context:{symbol}',{'model_version':self.model['version'],
                        'exploration':bool(self.status.get('exploration')),'model_approved':bool(self.model.get('approved')),
                        'confidence':decision['confidence'],'feature_ts':row['ts'],
                        'regime':'microstructure','strategy':'lab_model'})
            return action
        except Exception as exc:
            e.store.set(f'signal:{symbol}',{'action':'hold','reason':'Model signal unavailable: '+type(exc).__name__,
                                          'at':utcnow(),'source':'numeric_model'})
            return 'hold'
