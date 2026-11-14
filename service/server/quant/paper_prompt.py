"""Lossless paper input packing with a reusable prefix; never caches decisions."""
import hashlib

from .lab_features import FEATURES
from .prompt_context import compact_json

SCHEMA = 'paper.cache.v1'
STAT_COLUMNS = ('trades','wins','gross','fees','net','average_win','average_loss')
SYMBOL_STAT_COLUMNS = ('trades','net_pnl','win_rate')

GUIDE = '''
输入编码 paper.cache.v1：background 定义固定策略、字段和单位；current 是本轮数据。features 数组按 background.feature_order 对应，null 仍表示缺失；数组不能当作概率。
realized_feedback 的统计数组按 background.stat_columns 对应，by_side/by_exit 的键分别为方向和退出原因；feedback.by_symbol 数组按 background.symbol_stat_columns 对应，都是已完成交易的描述统计。management 是已有仓位不随行情变化的参数，position_marks 是本轮 mark/pnl/mae_pct/mfe_pct；两者按合约名合并评估，不能把旧入场价当成现价。
background.execution_cost 固定给出往返手续费、滑点以及未包含的风险；每币 execution_cost 给出点差和保本价格变动比例。相同字段只发送一次，缺失值不能补造。background.reinvestment 说明复投资金与盈亏来源；current.reinvestment_budget 为本轮实际可用预算。
current 的统计、management、保护参数是本轮重新读取的真实状态；market_context 给出候选的慢周期 potential/社区上下文；markets 在最后，包含本轮价格、时间和订单流。pending_exits 已触发退出且不能撤销。不要把请求字段顺序解释为交易优先级；仍须独立决定多空或观望。'''


def _ordered(value):
    if isinstance(value,dict):
        return {k:_ordered(value[k]) for k in sorted(value)}
    if isinstance(value,(list,tuple)):
        return [_ordered(v) for v in value]
    return value


def _stats(value):
    if isinstance(value,dict) and set(value)==set(STAT_COLUMNS):
        return [value[k] for k in STAT_COLUMNS]
    if isinstance(value,dict) and set(value)==set(SYMBOL_STAT_COLUMNS):
        return [value[k] for k in SYMBOL_STAT_COLUMNS]
    if isinstance(value,dict):
        return {k:_stats(value[k]) for k in sorted(value)}
    return value


def _exact_json(value):
    # Match the existing 10-significant-digit wire precision, including tiny
    # prices and returns; do not introduce coarser rounding for cache hits.
    return compact_json(value)


def build_messages(payload,system):
    background=dict(payload.get('background',{}))
    background.update(prompt_schema=SCHEMA,feature_order=list(FEATURES),stat_columns=list(STAT_COLUMNS),
                      symbol_stat_columns=list(SYMBOL_STAT_COLUMNS))
    source=payload['current']
    markets,context=[],{}
    fixed_cost=None
    for item in source['markets']:
        row=dict(item)
        features=row.get('features')
        if isinstance(features,dict) and set(features)==set(FEATURES):
            row['features']=[features[k] for k in FEATURES]
        cost=dict(row.get('execution_cost',{}))
        common={k:cost[k] for k in ('round_trip_fee_pct','round_trip_slippage_pct',
                                   'funding_and_jump_risk_included') if k in cost}
        if common:
            if fixed_cost is None:fixed_cost=common
            if common!=fixed_cost:raise ValueError('Inconsistent fixed execution costs')
            row['execution_cost']={k:v for k,v in cost.items() if k not in common}
        # Exact per-symbol contexts are separate from rapidly changing quotes.
        if any(k in row for k in ('potential','community')):
            context[row['symbol']]={k:row.pop(k) for k in ('potential','community') if k in row}
        markets.append(row)
    if fixed_cost is not None:background['execution_cost']=fixed_cost
    budget=dict(source.get('reinvestment_budget',{}))
    constants={k:budget.pop(k) for k in ('version','base_capital','source','profitability_proven') if k in budget}
    if constants:background['reinvestment']=constants
    management,marks={},{}
    for symbol,position in source.get('positions',{}).items():
        management[symbol]={k:v for k,v in position.items() if k not in ('mark','pnl','mae_pct','mfe_pct')}
        marks[symbol]={k:position[k] for k in ('mark','pnl','mae_pct','mfe_pct') if k in position}
    # Explicit order is intentional: do not globally sort this object.
    current={}
    for key in ('realized_feedback','feedback'):
        if key in source:current[key]=_stats(source[key])
    if 'paused_entries' in source:current['paused_entries']=source['paused_entries']
    current['management']=_ordered(management)
    current['pending_exits']=_ordered(source.get('pending_exits',{}))
    current['market_context']=_ordered(context)
    if 'reinvestment_budget' in source:current['reinvestment_budget']=_ordered(budget)
    # Keep arbitrary future data, rather than silently dropping new fields.
    known={'markets','positions','account','reinvestment_budget',*current}
    for key in sorted(set(source)-known):current[key]=_ordered(source[key])
    current['account']=_ordered(source.get('account',{}))
    current['position_marks']=_ordered(marks)
    current['markets']=markets
    prefix='{"background":'+_exact_json(_ordered(background))+',"current":'
    content=prefix+_exact_json(current)+'}'
    messages=[{'role':'system','content':system+GUIDE},{'role':'user','content':content}]
    stats={
        'prompt_schema':SCHEMA,'input_chars':len(system+GUIDE)+len(content),
        'legacy_input_chars':len(system)+len(compact_json(payload)),
        'fixed_prefix_chars':len(system+GUIDE)+len(prefix),
        'fixed_prefix_sha256':hashlib.sha256((system+GUIDE+prefix).encode()).hexdigest()[:24],
        'market_count':len(markets),
    }
    return messages,stats


def recent_usage(rows,schema):
    rows=[r for r in rows if r.get('prompt_schema')==schema][-24:]
    def total(key):
        return sum(r.get('usage',{}).get(key,0) for r in rows)
    hit,miss=total('prompt_cache_hit_tokens'),total('prompt_cache_miss_tokens')
    return {'prompt_schema':schema,'calls':len(rows),'window_limit':24,
            'prompt_tokens':total('prompt_tokens'),'completion_tokens':total('completion_tokens'),
            'prompt_cache_hit_tokens':hit,'prompt_cache_miss_tokens':miss,
            'prompt_cache_hit_rate':hit/(hit+miss) if hit+miss else None,
            'average_prompt_tokens':total('prompt_tokens')/len(rows) if rows else None,
            'average_miss_tokens':miss/len(rows) if rows else None}
