import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from quant.mail_format import build_message, display_time, report_body
from quant.settings import Settings


def test_timestamp_offsets_and_midnight_are_consistent():
    expected = '2026-10-14 00:05:06'
    for raw in ['2026-10-13T16:05:06.123Z', '2026-10-13T16:05:06+00:00',
                '2026-10-14T00:05:06+08:00', '2026-10-13T16:05:06']:
        assert display_time(raw) == expected
    assert display_time('invalid date') == '时间未知'


def test_multipart_mail_handles_legacy_records_and_untrusted_text():
    cfg = Settings(smtp_user='sender@qq.com', mail_to='receiver@qq.com')
    events = [
        {'kind': '清仓', 'created_at': '2026-10-14T02:30:00+00:00',
         'body': 'BTCUSDT 数量=-0.001 价格=61000 原因=take_profit 已实现盈亏=1 手续费=0.0305 USDT（模拟，不含资金费）'},
        {'kind': '持仓报告', 'created_at': '2026-10-14T02:30:00Z',
         'body': '模式：paper\n状态：running\n北京时间：2026-10-14T10:30:00+08:00\n空仓'},
        {'kind': '告警', 'created_at': 'invalid', 'body': '提示：<script>alert(1)</script>&'},
    ]
    message = build_message(cfg, events, datetime(2026, 10, 14, 2, 31, tzinfo=timezone.utc))
    plain = message.get_body(preferencelist=('plain',)).get_content()
    html = message.get_body(preferencelist=('html',)).get_content()
    assert '2026-10-14 10:30:00' in plain and '2026-10-14T' not in plain
    assert '2026-10-14T' not in html
    assert '交易方向：平多' in plain and '触发原因：触发止盈' in plain
    assert '成交价格：61,000 USDT' in plain and '已实现盈亏：+1 USDT' in plain
    assert '运行模式：模拟交易' in plain and '运行状态：运行中' in plain
    assert '<script>' not in html and '&lt;script&gt;' in html
    assert message['Date'].endswith('+0800')


def test_report_formats_short_position_without_losing_small_quantity():
    body = report_body(Settings(), {'ETHUSDT': {'amount': -0.00001, 'entry': 3000, 'mark': 3010, 'pnl': -0.0001}},
                       'running', {'equity': 10000, 'available': 9500}, datetime(2026, 10, 14, 0, tzinfo=timezone.utc))
    assert '报告时间：2026-10-14 08:00:00' in body
    assert '持仓数量：0.00001' in body and '方向：做空' in body
    assert '账户权益：10,000 USDT' in body and '浮动盈亏：-0.0001 USDT' in body


def test_report_includes_net_returns_and_margin():
    body = report_body(Settings(),{'BTCUSDT':{'amount':1,'entry':100,'mark':102,'pnl':2,
                        'leverage':5,'initial_margin':20}},'running',{'equity':1000},
                       performance={'scope':'盘口模拟','net':-1.234567,'today_net':2,'recent_net':.000123,'fees':.5})
    assert '累计已实现净盈亏：-1.234567 USDT' in body
    assert '近两小时已实现净盈亏：+0.000123 USDT' in body
    assert '杠杆：5 倍' in body and '占用保证金：20 USDT' in body


def test_short_close_notice_profits_after_both_fees():
    from quant.mail_format import trade_notice_body
    n = {'symbol':'ETHUSDT','closing':True,'direction':'short','exploration':True,
         'quantity':2,'quote':180,'requested_quantity':2,'fills':3,'leverage':10,'margin':20,
         'entry_price':100,'entry_fee':.1,'fee':.09,'gross_pnl':20,'remaining_position':0,
         'opened_at':'2026-11-05T04:00:00+00:00','filled_at':'2026-11-05T04:01:00+00:00',
         'reason':'dynamic_take_profit','wallet':1019.81}
    body = trade_notice_body(n)
    assert '平空' in body and '已实现净盈亏：+19.81 USDT' in body
    assert '保证金净收益率：+99.05%' in body and '持仓时长：60 秒' in body
    assert '动态止盈' in body
