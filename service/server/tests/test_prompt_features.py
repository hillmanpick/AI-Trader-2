import json
import sys
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from quant.ai_review import DeepSeekAdvisor, DeepSeekReviewer
from quant.features import indicators, entry_quality
from quant.prompt_context import compact_json, community_context, model_context
from quant.settings import Settings
from quant.strategies import volatility_plan


@pytest.fixture
def cfg():
    return Settings(strategy='adaptive',fast=3,slow=10,leverage=100,
                    paper_dynamic_exits=True,paper_slippage=.0003,deepseek_api_key='private-key')


def candles(closes):
    return [[i*60000,p,p*1.001,p*.999,p,1,(i+1)*60000-1] for i,p in enumerate(closes)]


def test_indicators_match_full_history_and_keep_only_six_prices(cfg):
    closes = list(range(100,140))
    result = indicators(closes,cfg,candles(closes))
    assert result['sma']['fast'] == 138
    assert result['sma']['previous_fast'] == 137
    assert result['sma']['slow'] == 134.5
    assert result['trend']['efficiency'] == 1
    assert result['trend']['regime'] == 'trend'
    assert result['trend']['returns_bps']['20'] == pytest.approx((139/119-1)*10000)
    assert result['volatility']['atr14'] > 0
    assert result['recent_closes'] == closes[-6:]


def test_nonfinite_and_bad_candles_do_not_become_model_evidence(cfg):
    with pytest.raises(ValueError): indicators([100]*30+[float('nan')],cfg)
    bad = candles([100]*40); bad[-1][1] = 105
    with pytest.raises(ValueError): indicators([100]*40,cfg,bad)
    assert indicators([100],cfg)['status'] == 'insufficient'


def test_community_is_fresh_deduplicated_bounded_and_keeps_caveats():
    now=datetime.now(timezone.utc).isoformat()
    rows=[{'symbol':'BTC','kind':'strategy','created_at':now,'content':
           ('[Copied from alice] ' if i == 1 else '')+'a'*800+'注意突破止损'} for i in range(2)]
    rows += [{'symbol':'BTC','kind':'strategy','created_at':now,'content':str(i)*1000} for i in range(7)]
    snapshot={'feed_updated_at':{'strategy':now},'feeds':{'strategy':rows}}
    result=community_context(snapshot,'BTCUSDT')
    assert len(result) == 3
    assert result[0]['truncated'] and result[0]['excerpt'].endswith('注意突破止损')
    assert len(compact_json(result)) < 1000
    snapshot['feed_updated_at']['strategy']='2020-01-01T00:00:00Z'
    assert community_context(snapshot,'BTCUSDT') == []


def test_prompt_background_is_identical_when_only_live_values_change(cfg):
    first={'symbol':'BTCUSDT','action':'long','price':100,'closed_prices':[100]*40,'bar':1}
    second=first|{'symbol':'ETHUSDT','action':'short','price':200,'closed_prices':[200]*40,'bar':2}
    a,b = (compact_json(model_context(x,{},cfg)) for x in (first,second))
    assert a.split(',"current":')[0] == b.split(',"current":')[0]
    assert a != b
    assert 'closed_prices' not in a


@pytest.mark.parametrize('action,price',[('long',101),('short',99)])
def test_range_entry_after_mean_reversion_has_no_net_target(cfg,action,price):
    f=indicators([100,100.1,100,99.9]*10,cfg)
    q=entry_quality(f,price,action,cfg,{'stop_pct':.0025,'take_pct':.005})
    assert not q['passes'] and q['status']=='target_below_cost'
    assert q['round_trip_cost_bps'] == pytest.approx(16)


def test_reward_risk_and_chasing_checks_are_mirrored(cfg):
    f=indicators(list(range(100,140)),cfg)
    q=entry_quality(f,139,'long',cfg,{'stop_pct':.0025,'take_pct':.01})
    assert q['passes'] and q['net_reward_risk'] == pytest.approx(.0084/.0041)
    q=entry_quality(f,140,'long',cfg,{'stop_pct':.0025,'take_pct':.01})
    assert q['status']=='entry_chased'
    q=entry_quality(f,139,'long',cfg,{'stop_pct':.0025,'take_pct':.0026})
    assert q['status']=='weak_net_reward_risk'


def response(decision,finish='stop'):
    session=MagicMock();reply=session.__enter__.return_value.post.return_value.__enter__.return_value
    reply.status_code=200
    reply.iter_content.return_value=[json.dumps({'model':'deepseek-flash','usage':{
        'prompt_tokens':600,'completion_tokens':90,'prompt_cache_hit_tokens':128,'prompt_cache_miss_tokens':472},
        'choices':[{'finish_reason':finish,'message':{'content':json.dumps(decision)}}]}).encode()]
    return session


def test_advice_has_bounded_output_usage_and_redacted_logs(cfg,caplog):
    session=response({'bias':'wait','confidence':.9,'reason':'risk','risk':'breakout'})
    with patch('quant.ai_review.requests.Session',return_value=session):
        value=DeepSeekAdvisor(cfg).analyze({'symbol':'BTCUSDT','closed_prices':[100]*40},{})
    assert value['status']=='complete'
    assert value['telemetry']['source']=='read_only_advice'
    assert value['telemetry']['usage']['prompt_cache_hit_tokens']==128
    assert value['telemetry']['usage']['prompt_cache_hit_rate'] == pytest.approx(128/600, abs=0.0001)
    body=session.__enter__.return_value.post.call_args.kwargs['json']
    assert body['thinking']=={'type':'disabled'} and body['max_tokens']==512
    assert 'private-key' not in json.dumps(value)+caplog.text+json.dumps(body)


def test_truncated_advice_is_not_shown_as_valid_analysis(cfg):
    session=response({'bias':'bullish','confidence':.9,'reason':'ok','risk':'none'},'length')
    with patch('quant.ai_review.requests.Session',return_value=session):
        value=DeepSeekAdvisor(cfg).analyze({'symbol':'BTCUSDT'},{})
    assert value['status']=='error' and value['bias']=='wait'
    assert value['telemetry']['usage']['completion_tokens']==90


def test_each_entry_uses_its_own_latest_market_instead_of_cached_decision(cfg):
    session=response({'decision':'allow','confidence':.9,'reason':'ok'})
    with patch('quant.ai_review.requests.Session',return_value=session):
        reviewer=DeepSeekReviewer(cfg)
        for price in [100,101]:reviewer.review({'symbol':'BTCUSDT','price':price,'closed_prices':[100]*40},{})
    calls=session.__enter__.return_value.post.call_args_list
    assert len(calls)==2
    assert [json.loads(c.kwargs['json']['messages'][1]['content'])['current']['price'] for c in calls]==[100,101]
