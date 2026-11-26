"""Explain paper entry blockers without granting permissions or placing orders."""
import json
from pathlib import Path


def latest_report(directory):
    try:
        return json.loads((Path(directory)/'latest-report.json').read_text(encoding='utf-8'))
    except (OSError,ValueError):
        return {}


def paper_readiness(paper, lab, model, report, collector, redis_ok, paused, now_ms):
    blockers=[]
    trial=bool(paper.paper_exploration_mode and not paper.paper_ai_autopilot)
    quant_trial=bool(paper.paper_quant_system and paper.paper_quant_exploration)
    def block(code,reason):
        blockers.append({'code':code,'reason':reason})
    if not paper.enabled:
        block('paper_disabled','模拟开仓总开关关闭')
    if paused:
        block('operator_paused','模拟账本处于暂停状态')
    if not lab.enabled or not paper.paper_lab_required or paper.strategy!='lab_model':
        block('pipeline_disabled','数值模型模拟执行链路未启用')
    if paper.paper_ai_autopilot:
        block('ai_autopilot','当前使用 AI 操盘，不属于数值模型自动开仓')
    if not redis_ok:
        block('redis_unavailable','Redis 实时数据不可用')
    if not 0 <= now_ms-collector.get('ts',0) <= 15000:
        block('collector_stale','采集器心跳过期或时钟异常')
    elif not collector.get('ready_symbols'):
        block('features_unavailable','当前没有就绪的候选币特征')
    if not model.get('ready') and not quant_trial:
        block('model_unvalidated',model.get('reason','模型验证未通过'))
    elif model.get('exploration') and not trial:
        block('trial_not_authorized','试验模型未获模拟前向试验开关授权')
    validation_warnings=[]
    if model.get('exploration'):
        validation_warnings.append({'code':'model_unvalidated',
            'reason':'原模拟盘前向试验：模型尚未通过样本外验证，真实盘口虚拟成交不等于策略已盈利'})
    if quant_trial:
        validation_warnings.append({'code':'systematic_trial_unvalidated',
            'reason':'系统化策略正在受限模拟前向试验，尚未通过样本外验证；这不代表策略已盈利'})
    tests=[f.get('test',{}) for f in report.get('folds',[])]
    return {'armed':bool(paper.enabled and not paused),
        'entry_ready':not blockers,'blockers':blockers,
        'mode':'systematic_forward_trial' if quant_trial else 'systematic_paper' if paper.paper_quant_system else 'forward_trial' if trial else 'validated_model',
        'validated_entry_ready':not blockers and not model.get('exploration') and not paper.paper_quant_system,
        'validation_warnings':validation_warnings,
        'next_step':'等待实时方向信号及仓位检查' if not blockers else blockers[0]['reason'],
        'model_version':model.get('version'),'training_status':report.get('status'),
        'sampled_test_pnl':[t.get('net_pnl') for t in tests],
        'sampled_tests_with_gaps':sum(not t.get('valid',True) for t in tests),
        'event_validation_reason':report.get('execution_validation',{}).get('reason'),
        'automatic_after_validation':bool(paper.enabled and not paper.paper_exploration_mode
            and not paper.paper_quant_system
            and paper.paper_lab_required and paper.strategy=='lab_model' and not paper.paper_ai_autopilot),
        'scope':'existing_paper_only','real_execution_enabled':False,
        'note':'就绪只代表允许检查开仓信号，不代表已经成交或保证盈利'}
