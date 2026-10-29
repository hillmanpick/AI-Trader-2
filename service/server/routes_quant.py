import hmac
import os
from datetime import datetime, timezone

from fastapi import Header, HTTPException, Request

from quant.settings import Settings
from quant.store import Store
from quant.community import age_seconds
from quant.ai_review import DeepSeekReviewer, DeepSeekAdvisor
from quant.official import OfficialReads
from quant.paper import paper_settings
from quant.reflection import StrategyReflector


def register_quant_routes(app):
    cfg = Settings()

    def authorized(token):
        if not cfg.operator_token or not token or not hmac.compare_digest(token,cfg.operator_token):
            raise HTTPException(status_code=401,detail='Quant operator authentication required')

    def journal():
        return Store(cfg.db_path)

    def write_authorized(token, action):
        authorized(token)
        # A custom header requires CORS preflight on a foreign origin.
        if action != 'operator':
            raise HTTPException(status_code=403,detail='Operator action header required')

    @app.get('/api/quant/lab/status')
    def lab_status(x_quant_token: str = Header(None)):
        authorized(x_quant_token)
        from quant.lab_config import LabConfig
        from quant.lab_store import LabStore, RealtimeBus
        from quant.lab_model import model_status
        lab = LabConfig()
        model,_ = model_status(lab)
        try:
            bus = RealtimeBus(lab.redis_url)
            state = bus.get('state') or {}
            redis_ok = bool(bus.client.ping())
            ranking = bus.get('ranking') or {}
        except Exception:
            state,redis_ok,ranking = {},False,{}
        try:
            history = LabStore(lab.directory).stats()
        except Exception:
            history = {'samples':0,'days':0,'error':'Historical store unavailable'}
        now = int(datetime.now(timezone.utc).timestamp()*1000)
        try:
            from quant.event_lake import EventLake
            lake = EventLake(lab.directory, int(lab.event_max_gb * 1024**3), lab.retention_days * 24)
            event_history = lake.stats()
            event_history['clickhouse'] = lake.export_status(
                os.getenv('QUANT_CLICKHOUSE_URL',''),
                os.getenv('QUANT_CLICKHOUSE_DATABASE','quant'),
                os.getenv('QUANT_CLICKHOUSE_TABLE','events'))
        except Exception:
            event_history = {'error':'Event lake unavailable'}
        try:
            import json
            from pathlib import Path
            event_replay = json.loads((Path(lab.directory)/'verified-book-replay.json').read_text())
            event_replay.pop('fills',None)
        except (OSError,ValueError):
            event_replay = None
        return {'enabled':lab.enabled,'redis_connected':redis_ok,'collector':state,
                'event_history':event_history,'ranking':ranking,'event_replay':event_replay,
                'stale':now-state.get('ts',0)>20000,'model':model,'history':history,
                'requirements':{'min_samples':lab.min_samples,'min_days':lab.min_days,
                                'horizon_seconds':lab.horizon_seconds,'candidate_limit':lab.candidates,
                                'event_validation_seconds':lab.event_validation_seconds,
                                'sample_seconds':lab.sample_seconds,'retention_days':lab.retention_days},
                'real_execution_enabled':False}

    @app.get('/api/quant/paper/status')
    def paper_status(x_quant_token: str = Header(None)):
        authorized(x_quant_token)
        paper = paper_settings(cfg)
        db = Store(paper.db_path)
        worker = db.get('worker', {'status':'not_started'})
        symbols = db.get('config',paper.public()).get('symbols',paper.symbols)
        return {'source':'independent_paper', 'real_execution_enabled':False,
                'config':db.get('config',paper.public()), 'worker':worker, 'stale':age_seconds(worker.get('heartbeat')) > 180,
                'paused':db.get('paused',False), 'daily_loss_blocked':db.get('daily_loss_blocked',False),
                'account':db.get('account',{}), 'positions':db.get('positions',{}),
                'signals':{s:db.get(f'signal:{s}') for s in symbols},
                'ai':{s:db.get(f'ai:{s}') for s in symbols},
                'entry_quality':{s:db.get(f'entry_quality:{s}') for s in symbols},
                'reflection':StrategyReflector(db, paper).status(),
                'lab_gate':db.get('lab_gate',{}),
                'execution_statistics':db.get('execution_statistics',{}),
                'execution_closed_fills':db.get('execution_closed_fills',[])[-30:],
                'sizing':{s:db.get(f'sizing:{s}') for s in db.get('config',paper.public()).get('symbols',[])},
                'events':db.events(limit=30), 'orders':db.orders()[:30], 'mail':db.get('mail_status',{})}

    @app.get('/api/quant/official/{path:path}')
    def official_read(path: str, request: Request, x_quant_token: str = Header(None)):
        authorized(x_quant_token)
        return OfficialReads(cfg, journal()).read(path, request.query_params.multi_items())

    @app.get('/api/quant/status')
    def status(x_quant_token: str = Header(None)):
        authorized(x_quant_token)
        db = journal()
        worker = db.get('worker',{'status':'not_started'})
        heartbeat = worker.get('heartbeat')
        stale = not heartbeat or (datetime.now(timezone.utc)-datetime.fromisoformat(heartbeat)).total_seconds()>180
        return {'config':cfg.public(),'worker':worker,'stale':stale,'paused':db.get('paused',False),'daily_loss_blocked':db.get('daily_loss_blocked',False),'account':db.get('account',{}),'positions':db.get('positions',{}),'mail':db.get('mail_status',{}),'events':db.events(limit=100),'signals':{s:db.get(f'signal:{s}') for s in cfg.symbols},'ai':{s:db.get(f'ai:{s}') for s in cfg.symbols},'ai_advice':db.get('ai_advice',{}),'ai_advice_job':db.get('ai_advice_job',{})}

    @app.get('/api/quant/live-account')
    def live_account(x_quant_token: str = Header(None)):
        authorized(x_quant_token)
        db = journal()
        snapshot = db.get('live_account', {})
        worker = db.get('worker', {})
        return snapshot | {'read_only':True, 'execution_enabled':False, 'stale':age_seconds(snapshot.get('updated_at')) > 90,
                           'account_stream':worker.get('account_stream', {'status':'not_started'}),
                           'credentials_configured':bool(cfg.api_key and cfg.api_secret),
                           'observer_enabled':cfg.observation_only}

    @app.get('/api/quant/community')
    def community(x_quant_token: str = Header(None)):
        authorized(x_quant_token)
        snapshot = journal().get('community',{})
        stamps = snapshot.get('feed_updated_at',{})
        stale = {kind:not 0 <= age_seconds(stamps.get(kind)) <= 180 for kind in ('operation','strategy','discussion')}
        return snapshot | {'enabled':cfg.community_enabled,'stale':stale,'account_token_configured':bool(cfg.community_token),'account_agent_id':cfg.community_agent_id}

    @app.post('/api/quant/ai-test')
    def ai_test(x_quant_token: str = Header(None), x_quant_action: str = Header(None)):
        write_authorized(x_quant_token,x_quant_action)
        if not cfg.deepseek_api_key:
            raise HTTPException(status_code=400,detail='请先在 .env 填写 DEEPSEEK_API_KEY 并重新加载服务')
        if cfg.observation_only:
            result = DeepSeekAdvisor(cfg).analyze({'symbol':'BTCUSDT','purpose':'连接测试，无行情数据，请观望，不执行交易','closed_prices':[]},{})
        else:
            result = DeepSeekReviewer(cfg).review({'symbol':'BTCUSDT','action':'long','purpose':'连接测试，不执行交易','closed_prices':[]},{})
        return {'ok':result['status']=='complete','reason':result['reason']}

    @app.get('/api/quant/orders')
    def orders(x_quant_token: str = Header(None)):
        authorized(x_quant_token)
        return {'orders':journal().orders()}

    @app.post('/api/quant/pause')
    def pause(x_quant_token: str = Header(None), x_quant_action: str = Header(None)):
        write_authorized(x_quant_token,x_quant_action)
        db = journal()
        db.set('paused',True)
        db.event('操作记录','操作员暂停开新仓；已有仓位止盈止损继续运行')
        return {'paused':True}

    @app.post('/api/quant/resume')
    def resume(x_quant_token: str = Header(None), x_quant_action: str = Header(None)):
        write_authorized(x_quant_token,x_quant_action)
        if cfg.observation_only:
            raise HTTPException(status_code=409,detail='当前为只读观察模式，不能通过此按钮开启交易')
        db = journal()
        if db.orders(pending=True):
            raise HTTPException(status_code=409,detail='Unresolved orders require exchange reconciliation')
        db.set('paused',False)
        db.event('操作记录','操作员恢复策略；仍受模式、每日亏损及交易所连通性限制')
        return {'paused':False}

    @app.post('/api/quant/mail-test')
    def mail_test(x_quant_token: str = Header(None), x_quant_action: str = Header(None)):
        write_authorized(x_quant_token,x_quant_action)
        journal().event('邮件测试','QQ 邮件配置测试；00:00–08:00 同样延迟发送。')
        return {'queued':True,'configured':cfg.mail_ready}
