"""Mix cross-sectional fast leaders with slower directional radar extremes."""


def select_research_candidates(ranking, radar, count, now_ms, max_age_ms=180000):
    ready=[r for r in ranking if r.get('ready') and r.get('eligible')]
    allowed={r['symbol'] for r in ready}
    if not ready or count<=0:return ()
    selected=['BTCUSDT'] if 'BTCUSDT' in allowed else []
    room=max(0,count-len(selected))
    if isinstance(radar,dict) and radar.get('schema')=='potential-radar-v1' and 0<=now_ms-radar.get('ts',0)<=max_age_ms:
        directional=sorted((r for r in radar.get('rows',[]) if r.get('symbol') in allowed
                            and r.get('symbol') not in selected),
            key=lambda r:-max(r.get('long_score',0),r.get('short_score',0)))
        quota=min(room,max(1,room//2)) if room else 0
        selected.extend(r['symbol'] for r in directional[:quota])
    for row in ready:
        if len(selected)>=count:break
        if row['symbol'] not in selected:selected.append(row['symbol'])
    return tuple(selected[:count])
