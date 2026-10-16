"""Bounded prompt context. This module never caches trading decisions."""
import json
import math
import re

from .community import recent_context
from .features import indicators


def compact_json(value):
    def clean(item):
        if isinstance(item,float):
            if not math.isfinite(item):
                raise ValueError('Non-finite AI input')
            return float(format(item,'.10g'))
        if isinstance(item,dict):
            return {k:clean(v) for k,v in item.items()}
        if isinstance(item,(list,tuple)):
            return [clean(v) for v in item]
        return item
    return json.dumps(clean(value),ensure_ascii=False,separators=(',',':'),allow_nan=False)


def community_context(snapshot, symbol):
    # Preserve freshness checks in recent_context; omit identities and IDs, not risks.
    rows = recent_context(snapshot,symbol)
    selected, seen = [], set()
    for row in rows:
        content = re.sub(r'^(?:\[Copied from [^\]]{1,120}\]\s*)+','',row.get('content') or '',flags=re.I)
        content = re.sub(r'\s+',' ',content).strip()
        title = re.sub(r'\s+',' ',row.get('title') or '').strip()
        identity = (row.get('kind'),row.get('side'),content.casefold(),title.casefold())
        if identity in seen:
            continue
        seen.add(identity)
        # Head + tail retains end-of-post caveats that head-only slicing discards.
        excerpt = content if len(content) <= 220 else content[:140]+' … '+content[-77:]
        selected.append({'kind':row.get('kind'), 'side':row.get('side'),
                         'title':title[:60], 'excerpt':excerpt,
                         'truncated':len(content)>220})
        if len(selected) == 3:
            break
    return selected


def model_context(market, snapshot, cfg):
    features = market.get('indicators')
    if features is None:
        features = indicators(market.get('closed_prices',[]),cfg)
    # Exact prices remain available; statistics are computed locally on full history.
    stable = {'schema':'quant.context.v2',
              'strategy':{'name':market.get('strategy',cfg.strategy),
                          'interval':market.get('interval',cfg.interval),
                          'sma_periods':[cfg.fast,cfg.slow],
                          'boll_period':cfg.boll_period,'boll_deviation':cfg.boll_deviation}}
    current = {k:market[k] for k in ('symbol','action','price','bar','valid_until',
                                   'candle_closed_at','purpose','rule_candidate','position_context','entry_quality')
               if k in market}
    current['indicators'] = features
    current['community'] = community_context(snapshot,market['symbol'])
    return {'background':stable, 'current':current}
