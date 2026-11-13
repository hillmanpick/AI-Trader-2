"""One bounded portfolio request at a time, operating on the existing paper ledger."""
import json
import logging
import math
import time
import uuid
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

import requests

from .ai_review import usage_telemetry
from .lab_features import FEATURES
from .prompt_context import compact_json, community_context
from .store import utcnow


SYSTEM = '''你是币安 USDT 永续模拟盘的自主交易经理。根据数据自己从候选中选币、判断方向、开仓、平仓、持仓和调整保护参数；无需规则信号或数值模型批准。不能操作实盘。
只能在 current.markets 给出的合约中决策，每币最多一条；不必每币都交易，没有扣费后优势就 hold，可省略观望币，减少无用输出。已有仓位反向要先 close，下一轮才能反向开仓；不加仓，不马丁，不删除保护。
采用双层判断：potential 给出 1-4 小时方向/极端波动背景，5m/15m 变化判断入场时机，5s-60s 订单流和盘口用于临近执行。新开多仓只能选择 long_score >= 20 且高于 short_score 的合约；新开空仓只能选择 short_score >= 20 且高于 long_score 的合约。缺少新鲜 potential 时不要新开仓；已有仓位仍须逐轮判断退出。
potential 的 long_score/short_score 是 0-100 规则共振分数，不是概率；return_* 是小数比例回报。current.markets 可能没有某币的慢周期 potential，不能因此自行补造。AI 置信度也不是盈利概率。
只返回 JSON：{"summary":"简短中文组合判断","decisions":[{"symbol":"BTCUSDT","action":"hold","confidence":0.0,"reason":"简短中文理由","margin_usdt":0,"leverage":0,"stop_loss_pct":0,"take_profit_pct":0,"hold_seconds":0}]}。
action 只能为 hold/long/short/close/update。long/short 必须给出正数保证金、整数杠杆、止损止盈距离和持仓秒数；update 可调整已有仓位止盈和止损距离、持仓秒数，止损只收紧。hold/close 其它参数填 0。止损止盈用小数（0.01=1%），hold_seconds 范围 60-3600。
程序会依据硬性资金、组合相关性、流动性、费用、风险预算和强平距离限制执行。requested leverage 是上限，可能被风控调低。confidence 是判断信心，不能当成盈利概率。
features 中 return_* 和 btc_return_60s 是对数收益；depth_imbalance/trade_imbalance/OFI 正数偏买方，负数偏卖方，不代表必涨跌；spread_bps 单位万分之一；ewma_volatility 是短期波动，volume_anomaly 是成交量相对基线；log_* 是 log1p；atr_proxy 是价格波动比例估计。
current 是即时数据，positions 是本模拟账本持仓，feedback 是本模拟盘扣费后反馈。综合短期趋势、订单流、市场环境和成本，不追求强行增加交易，不承诺收益。reason 不超过 80 字，summary 不超过 120 字。'''


def validate_decisions(value, symbols):
    if not isinstance(value, dict) or set(value) != {'summary','decisions'}:
        raise ValueError('Invalid portfolio response')
    if not isinstance(value['summary'], str) or not isinstance(value['decisions'], list):
        raise ValueError('Invalid portfolio fields')
    if len(value['decisions']) > len(symbols):
        raise ValueError('Too many decisions')
    seen = set()
    keys = {'symbol','action','confidence','reason','margin_usdt','leverage',
            'stop_loss_pct','take_profit_pct','hold_seconds'}
    for d in value['decisions']:
        if not isinstance(d, dict) or set(d) != keys:
            raise ValueError('Invalid decision fields')
        if not isinstance(d['symbol'],str) or d['symbol'] not in symbols or d['symbol'] in seen:
            raise ValueError('Unknown or duplicate symbol')
        seen.add(d['symbol'])
        if d['action'] not in ('hold','long','short','close','update'):
            raise ValueError('Invalid action')
        if not isinstance(d['reason'],str) or not d['reason'].strip():
            raise ValueError('Missing decision reason')
        for k in ('confidence','margin_usdt','leverage','stop_loss_pct','take_profit_pct','hold_seconds'):
            if type(d[k]) not in (int,float) or not math.isfinite(d[k]) or d[k] < 0:
                raise ValueError('Invalid numeric parameter')
        if d['confidence'] > 1:
            raise ValueError('Invalid confidence')
        if d['action'] in ('long','short','update'):
            if not .0008 <= d['stop_loss_pct'] <= .10 or not .0026 <= d['take_profit_pct'] <= .30:
                raise ValueError('Invalid protection distance')
            if int(d['hold_seconds']) != d['hold_seconds'] or not 60 <= d['hold_seconds'] <= 3600:
                raise ValueError('Invalid holding horizon')
        if d['action'] in ('long','short'):
            if d['margin_usdt'] <= 0 or int(d['leverage']) != d['leverage'] or not 1 <= d['leverage'] <= 100:
                raise ValueError('Invalid entry size')
        d['reason'] = d['reason'].strip()[:160]
    return {'summary':value['summary'][:240], 'decisions':value['decisions']}


class DeepSeekAutopilot:
    def __init__(self, cfg):
        if (cfg.mode != 'paper' or cfg.observation_only or cfg.live_enabled or cfg.api_key or cfg.api_secret):
            raise ValueError('DS autopilot is credential-free paper only')
        self.cfg = cfg

    def decide(self, payload):
        telemetry = {'source':'paper_autopilot','model':self.cfg.deepseek_model,'thinking':'disabled'}
        started = time.monotonic()
        def failure(code):
            telemetry.update(error_code=code, elapsed_ms=round((time.monotonic()-started)*1000))
            logging.warning('DeepSeek autopilot %s',json.dumps(telemetry))
            return {'status':'error','reason':code,'decisions':[], 'telemetry':telemetry}
        url = urlparse(self.cfg.deepseek_base_url)
        if not self.cfg.deepseek_api_key:
            return failure('unconfigured')
        if url.scheme != 'https' or not url.hostname or url.username or url.password or url.query or url.fragment:
            return failure('invalid_url')
        try:
            with requests.Session() as session:
                session.trust_env = False
                with session.post(self.cfg.deepseek_base_url.rstrip('/')+'/chat/completions',
                    headers={'Authorization':'Bearer '+self.cfg.deepseek_api_key},
                    json={'model':self.cfg.deepseek_model,'messages':[
                        {'role':'system','content':SYSTEM},{'role':'user','content':compact_json(payload)}],
                        'thinking':{'type':'disabled'},'response_format':{'type':'json_object'},
                        'temperature':0,'max_tokens':2048},
                    timeout=(4,30),allow_redirects=False,stream=True) as response:
                    if response.status_code != 200:
                        return failure('http_'+str(response.status_code))
                    raw = bytearray()
                    for chunk in response.iter_content(8192):
                        raw.extend(chunk)
                        if len(raw)>128000 or time.monotonic()-started>35:
                            return failure('response_limit')
                    result = json.loads(raw)
                    telemetry['usage'] = usage_telemetry(result)
                    choice = result['choices'][0]
                    if choice.get('finish_reason') != 'stop':
                        return failure('incomplete_response')
                    decision = validate_decisions(json.loads(choice['message']['content']),
                                                  {m['symbol'] for m in payload['current']['markets']})
            telemetry.update(elapsed_ms=round((time.monotonic()-started)*1000),
                             response_model=str(result.get('model',''))[:80])
            logging.info('DeepSeek autopilot %s',json.dumps(telemetry))
            return decision | {'status':'complete','telemetry':telemetry}
        except (requests.RequestException,ValueError,KeyError,IndexError,TypeError):
            return failure('request_or_schema_error')


def position_identity(p):
    return {k:p.get(k) for k in ('opened_at','amount','entry')} if p else None


class PaperAutopilot:
    def __init__(self, engine, lab):
        self.engine,self.lab = engine,lab
        self.client = DeepSeekAutopilot(engine.cfg)
        self.pool = ThreadPoolExecutor(max_workers=1,thread_name_prefix='paper-autopilot')
        self.job = None
        self.actions = {}

    def tick(self):
        e = self.engine
        now = time.time()
        if self.job:
            future,context = self.job
            if not future.done():
                return
            self.job = None
            try:
                result = future.result()
            except Exception:
                result = {'status':'error','reason':'decision_worker_error','decisions':[]}
            if now-context['started']>min(45,e.cfg.ai_review_max_age):
                result = result | {'status':'expired','reason':'响应已过期','decisions':[]}
            status = result | {'at':utcnow(),'request_id':context['id'],
                               'started_at':context['started_at'],'mode':'paper'}
            status['daily_budget']=e.store.get('autopilot:daily_budget',{})
            status['daily_request_limit']=288
            prior = e.store.get('autopilot',{})
            totals = dict(prior.get('usage_total',{}))
            for k,v in result.get('telemetry',{}).get('usage',{}).items():
                if type(v) is int:
                    totals[k] = totals.get(k,0)+v
            status['usage_total'] = totals
            status['requests'] = prior.get('requests',0)+1
            e.store.set('autopilot',status)
            for d in result.get('decisions',[]):
                self.actions[d['symbol']] = (d,context)
                e.store.set('ai:'+d['symbol'],d | {'status':'complete','at':utcnow(),'source':'paper_autopilot'})
            return
        if not e.cfg.enabled or not e.ready:
            return
        interval=max(300,e.cfg.paper_ai_autopilot_interval)
        if now-float(e.store.get('autopilot:last_request',0))<interval:
            return
        day=datetime.now(timezone.utc).strftime('%Y-%m-%d')
        daily=e.store.get('autopilot:daily_budget',{})
        if daily.get('day_utc')!=day:daily={'day_utc':day,'requests':0}
        if daily['requests']>=288:
            e.store.set('autopilot',e.store.get('autopilot',{})|{'status':'daily_budget_exhausted',
                'reason':'已达到每日 288 次 AI 决策上限','daily_budget':daily,'daily_request_limit':288})
            return
        # Batch fresh candidates, including managed positions; never queue per-symbol calls.
        positions = e.store.get('positions',{})
        symbols = sorted(e.cfg.symbols, key=lambda s:(s not in positions,s))
        markets,rows = [],{}
        community = e.store.get('community',{}) if e.cfg.community_enabled else {}
        radar=self.lab.bus.get('potential_candidates') if hasattr(self.lab.bus,'get') else None
        potentials={}
        if (isinstance(radar,dict) and radar.get('schema')=='potential-radar-v1'
                and 0<=time.time()*1000-radar.get('ts',0)<=180000):
            for item in radar.get('rows',[]):
                if (item.get('symbol') in symbols and 0<=time.time()*1000-item.get('ts',0)<=180000
                        and all(type(item.get(k)) in (int,float) and math.isfinite(item[k])
                                for k in ('long_score','short_score','return_5m','return_15m','return_1h','return_4h'))):
                    potentials[item['symbol']]=item
        for s in symbols[:40]:
            try:
                if not self.lab.gate(s):
                    continue
                r = self.lab.row(s)
                rows[s] = r
                markets.append({'symbol':s,'price':r['price'],'ts':r['ts'],
                    'features':dict(zip(FEATURES,r['vector'])),
                    'atr_proxy':r['meta']['atr_proxy'],
                    'potential':potentials.get(s),
                    'community':community_context(community,s)})
            except (ValueError,KeyError,TypeError):
                continue
        if not markets:
            e.store.set('autopilot',e.store.get('autopilot',{}) | {
                'status':'waiting_data','reason':'等待新鲜候选盘口与订单流特征','checked_at':utcnow()})
            return
        if not positions and not any(m.get('potential') for m in markets):
            e.store.set('autopilot',e.store.get('autopilot',{})|{'status':'waiting_data',
                'reason':'等待1–4小时多空候选与实时盘口重合，暂不花费AI请求','checked_at':utcnow()})
            return
        allowed = {m['symbol'] for m in markets}
        report = e.reflection.status().get('report',{})
        limits = {k:getattr(e.cfg,k) for k in ('paper_margin','paper_max_margin','max_notional',
            'max_positions','paper_min_leverage','paper_max_leverage','paper_major_max_leverage',
            'paper_risk_fraction','fee_rate','paper_slippage')}
        payload = {'background':{'schema':'paper.portfolio.v2','selection_horizon':'1-4h',
                       'entry_horizon':'5-15m with 5-60s microstructure check',
                       'limits':limits,'feature_order':FEATURES},
            'current':{'markets':markets,'positions':{s:p for s,p in positions.items() if s in allowed},
                       'account':e.store.get('account',{}),'paused_entries':e.store.get('paused',False),
                       'feedback':{k:report[k] for k in ('net_pnl','window_trades','win_rate','by_symbol') if k in report}}}
        context = {'id':uuid.uuid4().hex,'started':now,'started_at':utcnow(),'rows':rows,
                   'positions':{s:position_identity(positions.get(s)) for s in allowed},
                   'potentials':potentials}
        e.store.set('autopilot:last_request',now)
        daily['requests']+=1
        e.store.set('autopilot:daily_budget',daily)
        self.actions.clear()
        self.job = (self.pool.submit(self.client.decide,payload),context)
        e.store.set('autopilot',e.store.get('autopilot',{}) | {
            'status':'pending','reason':'DS 正在分析整批候选币与现有持仓',
            'request_id':context['id'],'started_at':context['started_at'],'symbols':sorted(allowed),
            'daily_budget':daily,'daily_request_limit':288})

    def signal(self,symbol):
        item = self.actions.pop(symbol,None)
        if not item:
            return 'hold'
        e = self.engine
        d,ctx = item
        row = self.lab.row(symbol)
        p = e.store.get('positions',{}).get(symbol)
        blocked = None
        if time.time()-ctx['started']>min(45,e.cfg.ai_review_max_age):
            blocked = '决策已过期'
        elif position_identity(p) != ctx['positions'].get(symbol):
            blocked = '请求后持仓已变化，等待下一轮'
        elif abs(row['price']/ctx['rows'][symbol]['price']-1) > .005:
            blocked = '请求后价格变化超过 0.5%，等待下一轮'
        elif d['confidence']<e.cfg.ai_min_confidence and d['action'] != 'hold':
            blocked = '决策置信度低于阈值'
        elif d['action'] in ('long','short') and (p or e.store.get('paused',False)):
            blocked = '已有仓位或已暂停新仓'
        elif d['action'] in ('close','update') and not p:
            blocked = '当前空仓'
        elif d['action'] in ('long','short'):
            potential=ctx['potentials'].get(symbol)
            side='long_score' if d['action']=='long' else 'short_score'
            other='short_score' if d['action']=='long' else 'long_score'
            if (not potential or time.time()*1000-potential['ts']>180000
                    or potential[side]<20 or potential[side]<=potential[other]):
                blocked='1-4小时方向筛选不支持该方向或数据已过期'
        action = 'hold' if blocked else d['action']
        e.store.set('signal:'+symbol,d | {'action':action,'reason':blocked or d['reason'],
            'at':utcnow(),'source':'deepseek_autopilot','request_id':ctx['id']})
        if action in ('long','short'):
            e.store.set('entry_context:'+symbol,{'model_version':'deepseek:'+e.cfg.deepseek_model,
                'model_approved':False,'exploration':True,'feature_ts':row['ts'],
                'confidence':d['confidence'],'regime':'microstructure','strategy':'ds_autopilot',
                'ai_parameters':d,'autopilot_request':ctx['id']})
        if action == 'update':
            direction = 1 if p['amount']>0 else -1
            candidate = p['mark']*(1-direction*d['stop_loss_pct'])
            old_stop = p.get('stop_price',p['entry']*(1-direction*p['stop_pct']))
            p['stop_price'] = max(old_stop,candidate) if direction>0 else min(old_stop,candidate)
            p['stop_pct'] = min(p['stop_pct'],d['stop_loss_pct'])
            p['exit_plan'] = p['exit_plan'] | {'stop_pct':p['stop_pct'],'take_pct':d['take_profit_pct']}
            p['ds_hold_seconds'] = d['hold_seconds']
            positions = e.store.get('positions',{})
            positions[symbol] = p
            e.store.set('positions',positions)
            action = 'hold'
        return action
