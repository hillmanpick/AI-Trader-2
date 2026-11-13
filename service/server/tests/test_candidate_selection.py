import sys
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from quant.candidate_selection import select_research_candidates


def rows(count=20):
    return [{'symbol':f'C{i}USDT','ready':True,'eligible':True,'score':count-i} for i in range(count)]


def test_selection_reserves_pool_for_both_fast_and_multi_hour_extremes():
    ranking=rows()
    radar={'schema':'potential-radar-v1','ts':1000,'rows':[
        {'symbol':'C18USDT','long_score':2,'short_score':80},
        {'symbol':'C19USDT','long_score':70,'short_score':3},
        {'symbol':'OUTSIDEUSDT','long_score':100,'short_score':100}]}
    chosen=select_research_candidates(ranking,radar,8,1100)
    assert 'BTCUSDT' not in chosen
    assert {'C18USDT','C19USDT'}<=set(chosen)
    assert len(chosen)==8


def test_bitcoin_is_context_and_expired_radar_cannot_displace_fast_leaders():
    ranking=[{'symbol':'BTCUSDT','ready':True,'eligible':True,'score':0}]+rows()
    radar={'schema':'potential-radar-v1','ts':1000,'rows':[{'symbol':'C19USDT','long_score':2,'short_score':99}]}
    chosen=select_research_candidates(ranking,radar,6,181001)
    assert chosen==('BTCUSDT','C0USDT','C1USDT','C2USDT','C3USDT','C4USDT')


def test_not_ready_or_ineligible_radar_symbols_are_never_subscribed():
    ranking=rows(3);ranking[2]['eligible']=False
    radar={'schema':'potential-radar-v1','ts':1000,'rows':[{'symbol':'C2USDT','long_score':10,'short_score':80}]}
    assert select_research_candidates(ranking,radar,3,1000)==('C0USDT','C1USDT')
