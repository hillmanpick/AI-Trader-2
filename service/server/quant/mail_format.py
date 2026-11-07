"""Presentation only: journal timestamps stay in UTC for sorting and auditing."""
import json
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from email.message import EmailMessage
from email.utils import format_datetime
from html import escape

SHANGHAI = timezone(timedelta(hours=8))
MODES = {'paper': '模拟交易', 'testnet': '测试网交易', 'live': '实盘交易'}
STATUSES = {'observing': '只读观察（自动交易关闭）', 'observation_error': '实盘只读同步异常', 'running': '运行中', 'blocked': '运行受阻', 'paused_entries': '已暂停开新仓', 'not_started': '尚未启动'}
REASONS = {'stop_loss': '触发止损', 'take_profit': '触发止盈', 'sma_cross': '均线交叉信号', 'bollinger': '布林带信号', 'protection_failure': '保护单异常'}
REASONS.update({'dynamic_stop':'动态止损 / 移动止损','dynamic_take_profit':'动态止盈',
                'model_horizon':'计划持仓时间到期','paper_liquidation':'模拟强平','lab_model':'数值模型方向信号',
                'deepseek_autopilot':'DeepSeek 自主决策'})
ISO_TIME = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?')
NUM = r'[-+\d.eE]+'


def local_time(now=None):
    value = now or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(SHANGHAI)


def display_time(value=None):
    try:
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return local_time(value).strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError, OverflowError):
        return '时间未知'


def number(value, places=8, signed=False):
    try:
        value = Decimal(str(value))
        if not value.is_finite():
            return '—'
        rendered = format(value, f',.{places}f').rstrip('0').rstrip('.')
        return ('+' if signed and value > 0 else '') + rendered
    except (InvalidOperation, ValueError, TypeError):
        return '—'


def position_lines(symbol, p):
    amount = float(p['amount'])
    return [f'交易对：{symbol}', f'方向：{"做多" if amount > 0 else "做空"}',
            f'持仓数量：{number(abs(amount))}', f'开仓均价：{number(p["entry"])} USDT',
            f'标记价格：{number(p.get("mark"))} USDT',
            f'浮动盈亏：{number(p.get("pnl"), 4, signed=True)} USDT（未扣平仓费用）'] + (
                [f'杠杆：{number(p["leverage"], 2)} 倍'] if 'leverage' in p else []) + (
                [f'占用保证金：{number(p["initial_margin"], 6)} USDT'] if 'initial_margin' in p else [])


def trade_notice_body(n):
    closing = n['closing']
    qty, quote = n['quantity'], n['quote']
    lines = [f'交易对：{n["symbol"]}',
             f'交易方向：{("平多" if n["direction"] == "long" else "平空") if closing else ("做多" if n["direction"] == "long" else "做空")}',
             f'交易模式：{"探索模拟（模型未通过样本外验证）" if n["exploration"] else "独立模拟"}',
             f'杠杆：{number(n["leverage"],2)} 倍',
             f'{"本次释放保证金" if closing else "本次投入保证金"}：{number(n["margin"],6)} USDT',
             f'成交数量：{number(qty)} / 委托 {number(n["requested_quantity"])}',
             f'成交明细：{n["fills"]} 档成交合并，金额 {number(quote,6)} USDT',
             f'开仓均价：{number(n["entry_price"])} USDT']
    if closing:
        total_fee = n['entry_fee']+n['fee']
        net = n['gross_pnl']-total_fee
        held = max(0,(datetime.fromisoformat(n['filled_at'])-datetime.fromisoformat(n['opened_at'])).total_seconds())
        lines += [f'平仓均价：{number(quote/qty)} USDT',
                  f'已实现毛盈亏：{number(n["gross_pnl"],6,signed=True)} USDT',
                  f'分摊开仓手续费：{number(n["entry_fee"],6)} USDT',
                  f'平仓手续费：{number(n["fee"],6)} USDT',
                  f'双边手续费合计：{number(total_fee,6)} USDT',
                  f'已实现净盈亏：{number(net,6,signed=True)} USDT',
                  f'保证金净收益率：{number(net/n["margin"]*100,4,signed=True)}%' if n['margin'] else '保证金净收益率：—',
                  f'持仓时长：{number(held,1)} 秒',f'剩余持仓数量：{number(n["remaining_position"])}']
    else:
        lines += [f'开仓手续费：{number(n["fee"],6)} USDT','已实现交易盈亏：尚未平仓',
                  f'移动止损价：{number(n.get("stop_price"))} USDT',f'动态止盈价：{number(n.get("take_price"))} USDT']
    lines += [f'触发原因：{REASONS.get(n["reason"],n["reason"])}',
              f'虚拟钱包余额：{number(n["wallet"],6)} USDT',
              '计算口径：按实际模拟成交计算，成交价已含滑点；净盈亏扣双边手续费，不含资金费。']
    return '\n'.join(lines)


def report_body(cfg, positions, status, account, now=None, performance=None):
    lines = [f'运行模式：{MODES.get(cfg.mode, cfg.mode)}',
             f'运行状态：{STATUSES.get(status, status)}', f'报告时间：{display_time(now)}']
    for key, label in [('equity', '账户权益'), ('available', '可用余额')]:
        if key in account:
            lines.append(f'{label}：{number(account[key], 2)} USDT')
    lines.append(f'持仓数量：{len(positions)} 个交易对')
    if performance:
        lines += [f'统计范围：{performance["scope"]}',
                  f'累计盈利状态：{"盈利" if performance["net"]>0 else "亏损" if performance["net"]<0 else "持平"}',
                  f'累计已实现净盈亏：{number(performance["net"],6,signed=True)} USDT',
                  f'今日已实现净盈亏：{number(performance["today_net"],6,signed=True)} USDT',
                  f'近两小时已实现净盈亏：{number(performance["recent_net"],6,signed=True)} USDT',
                  f'累计已支付手续费（含未平仓）：{number(performance["fees"],6)} USDT',
                  '盈亏口径：按盘口模拟成交计算并扣除双边手续费，未计资金费。']
        if 'completed_trades' in performance:
            lines += [f'完整平仓交易：{performance["completed_trades"]} 笔（盈利 {performance["wins"]} 笔）',
                      f'扣费后胜率：{number(performance["win_rate"]*100,2)}%',
                      f'累计已实现毛盈亏（含滑点）：{number(performance["gross"],6,signed=True)} USDT']
    lines.append(f'当前总浮动盈亏：{number(sum(float(p.get("pnl",0)) for p in positions.values()),6,signed=True)} USDT')
    if not positions:
        lines.append('当前持仓：空仓')
    for symbol, p in positions.items():
        lines.extend([''] + position_lines(symbol, p))
    return '\n'.join(lines)


def readable_body(event):
    """Support historical notification bodies without rewriting trade records."""
    body = str(event['body'])
    paper = re.fullmatch(rf'(\w+) 数量=({NUM}) 价格=({NUM}) 原因=(\S+) 已实现盈亏=({NUM}) 手续费=({NUM}) USDT（模拟，不含资金费）', body)
    if paper:
        symbol, qty, price, reason, pnl, fee = paper.groups()
        amount = Decimal(qty)
        closing = event['kind'] == '清仓'
        direction = ('平多' if amount < 0 else '平空') if closing else ('做多' if amount > 0 else '做空')
        return '\n'.join([f'交易对：{symbol}', f'交易方向：{direction}', f'成交数量：{number(abs(amount))}',
                          f'成交价格：{number(price)} USDT', f'触发原因：{REASONS.get(reason, reason)}',
                          f'已实现盈亏：{number(pnl, 4, signed=True)} USDT', f'手续费：{number(fee, 4)} USDT',
                          '说明：模拟成交，盈亏未扣除手续费，不含资金费'])
    live = re.fullmatch(rf'(\w+): ({NUM}) → ({NUM}); 当前=(.*)', body)
    if live:
        symbol, before, after, raw = live.groups()
        try:
            p = json.loads(raw)
            lines = [f'交易对：{symbol}', f'变更前数量（正为多 / 负为空）：{number(before)}',
                     f'变更后数量（正为多 / 负为空）：{number(after)}']
            if p:
                lines.extend(position_lines(symbol, p)[1:])
            else:
                lines.append('当前持仓：已清仓')
            return '\n'.join(lines)
        except (ValueError, KeyError, TypeError):
            pass
    body = ISO_TIME.sub(lambda m: display_time(m.group()), body)
    for old, new in [('模式：', '运行模式：'), ('状态：', '运行状态：'), ('北京时间：', '报告时间：')]:
        body = re.sub(rf'(?m)^{old}', new, body)
    for raw, label in MODES.items():
        body = body.replace(f'运行模式：{raw}', f'运行模式：{label}')
    for raw, label in STATUSES.items():
        body = body.replace(f'运行状态：{raw}', f'运行状态：{label}')
    return body


def body_html(body):
    rows = []
    for line in body.splitlines():
        if not line:
            rows.append('<tr><td colspan="2" style="height:12px"></td></tr>')
        elif '：' in line:
            label, value = line.split('：', 1)
            emphasis = '净盈亏' in label
            color = ('#b42318' if value.strip().startswith('-') else '#087f5b') if emphasis else '#172033'
            weight = '700' if emphasis else '400'
            rows.append(f'<tr><td style="padding:7px 12px 7px 0;color:#64748b;vertical-align:top;width:38%">{escape(label)}</td>'
                        f'<td style="padding:7px 0;color:{color};font-weight:{weight};word-break:break-word">{escape(value)}</td></tr>')
        else:
            rows.append(f'<tr><td colspan="2" style="padding:7px 0;color:#334155;word-break:break-word">{escape(line)}</td></tr>')
    return '<table role="presentation" width="100%" cellspacing="0" cellpadding="0">' + ''.join(rows) + '</table>'


def build_message(cfg, events, now=None):
    sent = local_time(now)
    mode = MODES.get(cfg.mode, cfg.mode)
    title = events[0]['kind'] if len(events) == 1 else f'通知汇总 · {len(events)} 条'
    message = EmailMessage()
    message['From'] = cfg.smtp_user
    message['To'] = cfg.mail_to
    message['Date'] = format_datetime(sent)
    message['Subject'] = f'[AI-Trader · {mode}] {title} | {sent:%m-%d %H:%M}'
    text = [f'AI-Trader · {mode}', title, f'发送时间：{display_time(sent)}（北京时间 UTC+8）', '']
    cards = []
    for event in events:
        body = readable_body(event)
        happened = display_time(event['created_at'])
        text.extend([f'【{event["kind"]}】', f'发生时间：{happened}', body, '', '─' * 24, ''])
        color = '#087f8c' if event['kind'] in ('开仓','开仓成交') else '#b45309' if event['kind'] in ('清仓','清仓成交') else '#334155'
        cards.append(f'<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="margin:0 0 16px;background:#fff;border:1px solid #e2e8f0;border-radius:12px"><tr><td style="padding:20px">'
                     f'<div style="font-size:18px;font-weight:700;color:{color}">{escape(event["kind"])}</div>'
                     f'<div style="margin:6px 0 14px;color:#64748b;font-size:12px">发生时间 · {happened} · 北京时间</div>'
                     f'{body_html(body)}</td></tr></table>')
    footer = ('所有时间均为北京时间（UTC+8）。模拟盘仅在 08:00、10:00、12:00、14:00、16:00、18:00、20:00、22:00 发送盈亏与持仓汇总；00:00–08:00 静默，不发送逐笔交易邮件。'
              if cfg.mode == 'paper' and cfg.paper_summary_only else
              '所有时间均为北京时间（UTC+8）。开仓 / 清仓通知，每两小时持仓报告；00:00–08:00 静默，夜间事件早上补发。')
    text.append(footer)
    message.set_content('\n'.join(text))
    html = f'''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"></head>
<body style="margin:0;background:#f1f5f9;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Microsoft YaHei',sans-serif;font-size:14px;line-height:1.6">
<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:#f1f5f9"><tr><td align="center" style="padding:28px 12px">
<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="max-width:640px"><tr><td style="background:#142337;border-radius:14px 14px 0 0;padding:28px;color:#fff">
<div style="font-size:12px;letter-spacing:3px;color:#b9d4dc">AI-TRADER / 交易通知</div>
<div style="margin:10px 0;font-size:26px;font-weight:700">{escape(title)}</div>
<div style="color:#d3e0ec">{escape(mode)} &nbsp;·&nbsp; {len(events)} 条通知</div>
<div style="margin-top:14px;color:#b9c7d6;font-size:12px">发送时间 · {display_time(sent)} · 北京时间 UTC+8</div>
</td></tr><tr><td style="padding:20px 16px 4px;background:#f8fafc">{''.join(cards)}</td></tr>
<tr><td style="padding:16px 24px 24px;background:#f8fafc;border-radius:0 0 14px 14px;color:#64748b;font-size:12px">{footer}</td></tr></table>
</td></tr></table></body></html>'''
    message.add_alternative(html, subtype='html')
    return message
