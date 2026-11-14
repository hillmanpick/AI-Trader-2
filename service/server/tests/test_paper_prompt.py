import copy,json,sys
from pathlib import Path

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from quant.lab_features import FEATURES
from quant.paper_prompt import build_messages,recent_usage,STAT_COLUMNS,SYMBOL_STAT_COLUMNS,SCHEMA
from quant.prompt_context import compact_json


def payload():
    stats=dict(zip(STAT_COLUMNS,(40,15,-90.,65.,-155.,6.,-9.)))
    return {'background':{'limits':{'fee_rate':.0005},'feature_order':FEATURES},
        'current':{'markets':[{'symbol':'BTCUSDT','price':98765.123456789,'ts':1794646800123,
            'features':dict(zip(FEATURES,(1e-12,-2e-6,.0001,.0002,1.,.3,-5.,.2,10.,2.,-.001,9.,8.,15.))),
            'execution_cost':{'round_trip_fee_pct':.001,'round_trip_slippage_pct':.0006,
                              'funding_and_jump_risk_included':False,'spread_bps':1.,'break_even_move_pct':.0017},
            'potential':{'ts':1794646800000,'long_score':30,'short_score':10},
            'community':[{'excerpt':'不能保证收益','truncated':True}]}],
        'positions':{'BTCUSDT':{'entry':98000.,'amount':.001,'stop_price':97500.,
                               'opened_at':'2026-11-14T08:00:00Z','mark':98765.123456789,'pnl':.765,
                               'mae_pct':-.001,'mfe_pct':.005}},
        'realized_feedback':{'total':stats,'by_side':{'long':stats,'short':stats},'scope':'completed only'},
        'pending_exits':{'BTCUSDT':{'reason':'ds_stop_loss','requested_ns':1794646800123456789}},
        'reinvestment_budget':{'base_capital':6000.,'version':'v1','source':'settled',
                               'profitability_proven':False,'usable_capital':5845.,'per_trade_risk_usdt':5.1},
        'account':{'updated_at':'2026-11-14T09:00:00.123456Z','equity':979000.},
        'paused_entries':False,'feedback':{'net_pnl':-155.},'future_field':{'value':7}}}


def decode(messages):return json.loads(messages[1]['content'])


def test_packing_preserves_values_relative_to_original_wire_and_does_not_mutate_source():
    p=payload();before=copy.deepcopy(p)
    messages,stats=build_messages(p,'system')
    assert p==before
    encoded=decode(messages);old=json.loads(compact_json(p))
    market=encoded['current']['markets'][0];original=old['current']['markets'][0]
    assert dict(zip(FEATURES,market['features']))==original['features']
    assert market['price']==original['price'] and market['ts']==original['ts']
    restored_cost=encoded['background']['execution_cost']|market['execution_cost']
    assert restored_cost==original['execution_cost']
    assert encoded['current']['market_context']['BTCUSDT']['potential']==original['potential']
    assert encoded['current']['market_context']['BTCUSDT']['community']==original['community']
    restored_position=encoded['current']['management']['BTCUSDT']|encoded['current']['position_marks']['BTCUSDT']
    assert restored_position==old['current']['positions']['BTCUSDT']
    restored_budget=encoded['background']['reinvestment']|encoded['current']['reinvestment_budget']
    assert restored_budget==old['current']['reinvestment_budget']
    assert encoded['current']['pending_exits']==old['current']['pending_exits']
    assert encoded['current']['account']==old['current']['account']
    assert encoded['current']['future_field']==old['current']['future_field']
    assert dict(zip(STAT_COLUMNS,encoded['current']['realized_feedback']['total']))==old['current']['realized_feedback']['total']
    assert stats['prompt_schema']==SCHEMA


def test_quote_changes_keep_entire_slow_state_prefix_but_never_reuse_old_prices():
    first=payload();second=copy.deepcopy(first)
    second['current']['markets'][0].update(price=100000.,ts=1794646800999)
    second['current']['markets'][0]['features']['return_5s']=-.002
    a,ta=build_messages(first,'system');b,tb=build_messages(second,'system')
    assert a[0]==b[0] and ta['fixed_prefix_sha256']==tb['fixed_prefix_sha256']
    assert a[1]['content'].split('"markets":',1)[0]==b[1]['content'].split('"markets":',1)[0]
    assert decode(a)['current']['markets'][0]['price']!=decode(b)['current']['markets'][0]['price']
    assert list(decode(b)['current'])[-1]=='markets'


def test_nonmarket_state_changes_are_sent_immediately_without_ttl_or_quantization():
    p=payload();a,_=build_messages(p,'system')
    p['current']['positions']['BTCUSDT']['stop_price']=99000.
    p['current']['realized_feedback']['total']['net']=-160.
    p['current']['pending_exits']['BTCUSDT']['attempts']=2
    p['current']['reinvestment_budget']['per_trade_risk_usdt']=4.9
    b,_=build_messages(p,'system');current=decode(b)['current']
    assert current['management']['BTCUSDT']['stop_price']==99000.
    assert current['realized_feedback']['total'][STAT_COLUMNS.index('net')]==-160.
    assert current['pending_exits']['BTCUSDT']['attempts']==2
    assert current['reinvestment_budget']['per_trade_risk_usdt']==4.9
    assert a!=b


def test_mapping_insertion_order_does_not_break_stable_prefix():
    p=payload();a,_=build_messages(p,'system')
    p['background']['limits']={'fee_rate':.0005}
    p['background']=dict(reversed(list(p['background'].items())))
    p['current']['positions']['BTCUSDT']=dict(reversed(list(p['current']['positions']['BTCUSDT'].items())))
    b,_=build_messages(p,'system')
    assert a==b


def test_null_features_and_price_stay_null_for_management_only_positions():
    p=payload();p['current']['markets'][0].update(price=None,ts=None,features=None,management_only=True)
    messages,_=build_messages(p,'system');row=decode(messages)['current']['markets'][0]
    assert row['features'] is None and row['price'] is None and row['ts'] is None
    assert row['management_only']


def test_per_symbol_feedback_keeps_all_coins_and_statistics_with_single_column_definition():
    p=payload();rows={f'COIN{i}USDT':{'trades':i,'net_pnl':-i*.25,'win_rate':.4} for i in range(40)}
    p['current']['feedback']['by_symbol']=rows
    messages,_=build_messages(p,'system');encoded=decode(messages)
    packed=encoded['current']['feedback']['by_symbol']
    assert set(packed)==set(rows)
    assert {s:dict(zip(SYMBOL_STAT_COLUMNS,v)) for s,v in packed.items()}==rows
    assert encoded['background']['symbol_stat_columns']==list(SYMBOL_STAT_COLUMNS)


@pytest.mark.parametrize('bad',[float('nan'),float('inf')])
def test_nonfinite_market_data_is_rejected(bad):
    p=payload();p['current']['markets'][0]['price']=bad
    with pytest.raises(ValueError):build_messages(p,'system')


def test_round_trip_costs_are_not_silently_shared_across_different_fee_rates():
    p=payload();other=copy.deepcopy(p['current']['markets'][0]);other['symbol']='ETHUSDT'
    other['execution_cost']['round_trip_fee_pct']=.002
    p['current']['markets'].append(other)
    with pytest.raises(ValueError):build_messages(p,'system')


def test_current_schema_window_is_bounded_and_uses_token_weighted_hit_rate():
    rows=[{'prompt_schema':'old','usage':{'prompt_tokens':9000,'prompt_cache_miss_tokens':9000}}]
    rows += [{'prompt_schema':SCHEMA,'usage':{'prompt_tokens':1000,'prompt_cache_hit_tokens':600,
                                            'prompt_cache_miss_tokens':400}} for _ in range(30)]
    rows[-1]['usage']={'prompt_tokens':2000,'prompt_cache_hit_tokens':0,'prompt_cache_miss_tokens':2000}
    result=recent_usage(rows,SCHEMA)
    assert result['calls']==24 and result['prompt_tokens']==25000
    assert result['prompt_cache_hit_rate']==pytest.approx(13800/25000)
    assert result['average_miss_tokens']==pytest.approx(11200/24)
    assert recent_usage([],SCHEMA)['prompt_cache_hit_rate'] is None
