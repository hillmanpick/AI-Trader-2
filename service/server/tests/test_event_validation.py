import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from quant.event_labels import EventLabeler, LABEL_SCHEMA
from quant.event_lake import EventLake, envelope
from quant.event_validation import ModelEventReplay, replay_model, validation_passes, validate_candidate, REPLAY_SCHEMA
from quant.execution_policy import fingerprint, model_version
from quant.execution_policy import leverage_bounds
from quant.execution_state import OrderJournal
from quant.lab_config import LabConfig
from quant.lab_features import FEATURES, SCHEMA
from quant.lab_model import model_status
from quant.lab_store import LabStore


def ev(kind, ms, symbol='BTCUSDT', source='websocket', **fields):
    if kind=='feature':
        fields['symbol'] = symbol
    return envelope({'e':kind,'s':symbol,'E':ms,**fields},source,received_ns=ms*1000000)


def sample(ms, price=100):
    return {'symbol':'BTCUSDT','ts':ms,'price':price,'vector':[0.]*len(FEATURES),
            'meta':{'quality':True,'schema':SCHEMA,'atr_proxy':.0012,
                    'filters':[{'filterType':'LOT_SIZE','stepSize':'.001','minQty':'.001','maxQty':'10000'},
                               {'filterType':'MIN_NOTIONAL','notional':'5'}]}}


def trade(ms, identity, price=100, qty='1', source='websocket'):
    return ev('trade',ms,source=source,t=identity,T=ms,p=str(price),q=qty,m=False)


def test_individual_path_catches_intrabar_reversal_and_requires_full_horizon():
    labeler = EventLabeler(horizon_ms=10000,stride_ms=1000)
    labeler.consume(trade(1000,1))
    labeler.consume(ev('feature',1001,**sample(1001)))
    labeler.consume(trade(2000,2,100.5))
    assert not labeler.drain()  # even an early barrier hit needs full coverage
    labeler.consume(trade(7000,3,99.5))
    labeler.consume(trade(11002,4,100))
    result, = labeler.drain()
    assert result['valid'] and result['label']==1 and result['hit_ns']==2000000000
    assert result['label_end']==11002  # availability, not just nominal expiry


def test_expiry_cannot_use_later_price_and_gap_after_hit_invalidates():
    labeler = EventLabeler(horizon_ms=10000)
    labeler.consume(trade(1000,1))
    labeler.consume(ev('feature',1001,**sample(1001)))
    labeler.consume(trade(9000,2))
    labeler.consume(trade(11002,3,120))
    result, = labeler.drain()
    assert result['valid'] and result['label']==0
    labeler = EventLabeler(horizon_ms=10000)
    for event in (trade(1000,1),ev('feature',1001,**sample(1001)),
                  trade(2000,2,101),trade(9000,4),trade(11002,5)):
        labeler.consume(event)
    result, = labeler.drain()
    assert not result['valid'] and result['invalid']=='individual_trade_id_gap'


def test_expiry_waits_for_confirming_id_and_rejects_disconnected_or_idle_path():
    labeler = EventLabeler(horizon_ms=10000)
    for event in (trade(1000,1),ev('feature',1001,**sample(1001)),trade(10000,2),
                  ev('heartbeat',12000)):
        labeler.consume(event)
    assert not labeler.drain()
    labeler.consume(trade(12001,4))
    assert not labeler.drain()[0]['valid']
    labeler = EventLabeler(horizon_ms=10000)
    for event in (trade(1000,1),ev('feature',1001,**sample(1001)),
                  ev('feedStatus',2000,symbol='',symbols=['BTCUSDT']),trade(11002,2)):
        labeler.consume(event)
    assert labeler.drain()[0]['invalid']=='trade_feed_reconnected'
    labeler = EventLabeler(horizon_ms=10000)
    for event in (trade(1000,1),ev('feature',1001,**sample(1001)),ev('heartbeat',27000)):
        labeler.consume(event)
    assert not labeler.drain()[0]['valid']


def test_zero_frames_keep_id_continuity_and_backfill_cannot_rewrite_label(tmp_path):
    labeler = EventLabeler(horizon_ms=10000)
    for event in (trade(1000,1),ev('feature',1001,**sample(1001)),trade(2000,2,0,'0'),
                  trade(3000,20,120,source='rest_backfill'),trade(9000,3),trade(11002,4)):
        labeler.consume(event)
    result, = labeler.drain()
    assert result['label']==0 and result['valid']
    store = LabStore(tmp_path)
    store.append([sample(1001),sample(2001)])
    store.append_labels([result])
    store.append_labels([result])
    assert store.stats()['event_labels']['valid']==1
    assert store.labeled_rows()[0]['label_schema']==LABEL_SCHEMA
    assert len(store.labeled_rows())==1  # unlabelled older quotes cannot leak in


def test_interleaved_symbols_keep_independent_receive_order_and_label_quality():
    labeler = EventLabeler(horizon_ms=10000)
    labeler.consume(trade(1000,1))
    labeler.consume(ev('trade',900,symbol='ETHUSDT',t=10,T=900,p='200',q='1',m=False))
    labeler.consume(ev('feature',1001,**sample(1001)))
    labeler.consume(trade(11002,2))
    assert labeler.drain()[0]['valid']


def model():
    result = {'schema':SCHEMA,'features':list(FEATURES),'classes':[-1,0,1],
        'mean':[0.]*len(FEATURES),'scale':[1.]*len(FEATURES),
        'coef':[[0.]*len(FEATURES) for _ in range(3)],'intercept':[-10,-10,10],
        'threshold':.6,'training_end_ms':0,'label_schema':LABEL_SCHEMA}
    return result | {'version':model_version(result)}


def policy():
    return {'fee_rate':.0005,'paper_slippage':.0003,'paper_margin':2000,
        'paper_max_margin':6000,'paper_min_leverage':2,'paper_max_leverage':20,
        'paper_risk_fraction':.1,'paper_risk_buffer':.001,'paper_maintenance_rate':.005,
        'paper_depth_participation':.05,'paper_depth_range_bps':10,
        'paper_max_spread_bps':10,'max_positions':3,'max_notional':120000,'horizon_seconds':30}


def market_events():
    result = []
    previous = 100
    for second in range(161):
        ms = second*1000
        price = 100 if second<70 else 100.5
        if second%60==0:
            result.append(ev('depthSnapshot',ms,source='rest_snapshot',lastUpdateId=previous,
                bids=[[str(price-.01),'100000']],asks=[[str(price+.01),'100000']]))
        result.append(ev('depthUpdate',ms,U=previous,u=previous+1,pu=previous,
            b=[[str((100.5 if second>70 else 100)-.01),'0'],[str(price-.01),'100000']],
            a=[[str((100.5 if second>70 else 100)+.01),'0'],[str(price+.01),'100000']]))
        previous += 1
        result.append(trade(ms,second+1,price))
        if second>=65 and second%5==0:
            result.append(ev('feature',ms,source='derived',**sample(ms,price)))
    return result


def test_portfolio_event_replay_uses_next_event_fills_costs_and_shared_dynamic_sizing(tmp_path):
    journal = OrderJournal(tmp_path/'orders.db')
    replay = ModelEventReplay(journal,model(),policy(),65000*1000000,160000*1000000)
    report = replay.report(market_events())
    assert report['valid'],report
    assert report['trades']>=1 and report['fees']>0 and report['funding']>0
    assert report['net_pnl']==pytest.approx(sum(t['net'] for t in replay.closed))
    assert not report['open_positions'] and replay.closed[0]['net']>0
    assert report['net_pnl']<0  # subsequent flat-price round trips lose real costs
    entry = next(o for o in journal.orders() if not o['reduce_only'])
    assert entry['first_fill_ns']>entry['submitted_ns']
    assert 2<=entry['plan']['leverage']<=20
    assert entry['plan']['planned_risk_usdt']<=200
    assert report['model_version']==model()['version']
    assert not validation_passes(report)  # one profitable trade is insufficient


def test_event_replay_fails_missing_depth_gaps_and_unclosed_positions(tmp_path):
    events = market_events()
    # Remove a single trade from the untouched test path.
    events = [e for e in events if not(e['kind']=='trade' and e['payload']['t']==69)]
    replay = ModelEventReplay(OrderJournal(tmp_path/'gap.db'),model(),policy(),65000*1000000,160000*1000000)
    report = replay.report(events)
    assert not report['valid'] and 'missing_depth_or_sequence_gap' in report['invalid_reasons']
    short = ModelEventReplay(OrderJournal(tmp_path/'short.db'),model(),policy(),65000*1000000,160000*1000000)
    report = short.report([e for e in market_events() if e['received_ns']<69000*1000000])
    assert not report['valid'] and report['open_positions']==1
    assert 'unclosed_position_or_pending_order_at_end' in report['invalid_reasons']


@pytest.mark.parametrize('fault', ['stale_depth', 'missing_requested_depth'])
def test_portfolio_proof_preserves_underlying_book_failure(tmp_path, fault):
    events = market_events()
    replay = ModelEventReplay(OrderJournal(tmp_path/'book-fault.db'),model(),policy(),
                              65000*1000000,160000*1000000)
    if fault == 'stale_depth':
        # Trades continue to the window end, but no new book arrives for 5s.
        events = [e for e in events if not (e['kind'] in ('depthSnapshot','depthUpdate')
                  and e['received_ns']>155000*1000000)]
        expected = 'stale_depth_at_end:BTCUSDT'
    else:
        replay.expected_symbols = {'BTCUSDT','ETHUSDT'}
        expected = 'missing_requested_depth:ETHUSDT'
    report = replay.report(events)
    assert not report['valid']
    assert expected in report['invalid_reasons']
    assert not validation_passes(report)


def test_model_holdout_rejects_refit_and_changed_artifact_or_missing_lake(tmp_path):
    lake = EventLake(tmp_path)
    with pytest.raises(ValueError,match='boundary'):
        replay_model(lake,model() | {'training_end_ms':70000},policy(),65000,160000,{'BTCUSDT'})
    with pytest.raises(ValueError,match='coverage'):
        replay_model(lake,model(),policy(),65000,160000,{'BTCUSDT'})


def test_archived_parquet_holdout_replay_is_repeatable_and_net_of_realistic_costs(tmp_path):
    lake = EventLake(tmp_path)
    lake.append(market_events())
    first = replay_model(lake,model(),policy(),65000,160000,{'BTCUSDT'})
    second = replay_model(lake,model(),policy(),65000,160000,{'BTCUSDT'})
    assert first['valid'] and first['schema']==REPLAY_SCHEMA
    assert first['manifest_hash']==second['manifest_hash']
    assert first['net_pnl']==pytest.approx(second['net_pnl'])
    assert first['fees']>0 and first['funding']>0 and first['trades']==3


def test_threshold_selection_never_uses_test_results():
    calls = []
    def fake(lake,m,p,start,end,symbols,stress=False):
        calls.append((m['version'],m['threshold'],start,end,stress))
        return {'valid':True,'trades':30,'net_pnl':m['threshold'] if start==100 else -1,
                'profit_factor':2.,'max_drawdown':1,'model_version':m['version'],'manifest_hash':'history'}
    with patch('quant.event_validation.replay_model',side_effect=fake):
        candidate,proof = validate_candidate(None,model(),policy(),100,200,300,{'BTCUSDT'})
    assert candidate['threshold']==.7
    assert not proof['passed']  # loss on unseen test cannot choose a new threshold
    assert len(calls)==5 and all(c[1]==.7 for c in calls[-2:])
    assert proof['model_version']==model_version(candidate)


def test_model_gate_binds_weights_policy_and_complete_event_proof(tmp_path):
    candidate = model() | {'approved':True,'trained_ms':1000}
    valid = {'valid':True,'trades':30,'net_pnl':10,'profit_factor':2,'max_drawdown':1,
             'model_version':candidate['version'],'manifest_hash':'history'}
    candidate['execution_validation'] = {'passed':True,'schema':REPLAY_SCHEMA,
        'model_version':candidate['version'],'policy_hash':fingerprint(policy()),
        'validation_start_ms':100,'test_start_ms':200,'test_end_ms':300,
        'validation':valid,'test':valid,'stress':valid}
    candidate['test'] = valid
    path = tmp_path/'model.json'
    path.write_text(json.dumps(candidate))
    with patch('quant.execution_policy.current_policy',return_value=policy()):
        assert model_status(LabConfig(directory=str(tmp_path)),now=2000)[0]['ready']
        candidate['coef'][0][0] = 1
        path.write_text(json.dumps(candidate))
        assert not model_status(LabConfig(directory=str(tmp_path)),now=2000)[0]['ready']
    candidate['coef'][0][0] = 0
    path.write_text(json.dumps(candidate))
    with patch('quant.execution_policy.current_policy',return_value=policy() | {'paper_max_leverage':50}):
        assert not model_status(LabConfig(directory=str(tmp_path)),now=2000)[0]['ready']


def test_configured_major_cap_is_bound_to_validation_policy():
    from dataclasses import replace
    from quant.execution_policy import policy_for_settings
    from quant.settings import Settings
    settings = Settings(paper_major_max_leverage=40)
    cfg = policy_for_settings(settings)
    assert settings.public()['paper_major_max_leverage']==40
    assert leverage_bounds(cfg,'BTCUSDT')==(2,40)
    assert leverage_bounds(cfg,'SOLUSDT')==(2,20)
    assert fingerprint(cfg)!=fingerprint(policy_for_settings(replace(settings,paper_major_max_leverage=50)))


def test_dynamic_leverage_caps_btc_eth_at_100_but_keeps_altcoin_cap():
    cfg = policy() | {'major_symbols':['BTCUSDT','ETHUSDT'],'major_symbol_max_leverage':100}
    assert leverage_bounds(cfg,'BTCUSDT')==(2,100)
    assert leverage_bounds(cfg,'ETHUSDT')==(2,100)
    assert leverage_bounds(cfg,'SOLUSDT')==(2,20)


def test_restart_never_turns_stale_ioc_intent_into_new_entry(tmp_path):
    from quant.execution_state import SimulatedBroker
    from quant.order_book import DepthBook
    path = tmp_path/'restart.db'
    before = SimulatedBroker(OrderJournal(path))
    before.submit('old','BTCUSDT',1,'1',0)
    after = SimulatedBroker(OrderJournal(path))
    book = DepthBook()
    book.seed({'lastUpdateId':1,'bids':[['99','100']],'asks':[['101','100']]},5000)
    book.ready = True
    after.on_event(5000000000,{'BTCUSDT':book})
    assert after.journal.get('old')['state']=='EXPIRED'
    assert not after.journal.fills()
