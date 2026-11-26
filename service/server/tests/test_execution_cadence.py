"""Realtime clocks and maintenance work cannot starve IOC execution."""
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from quant.engine import Engine
from quant.settings import Settings
from quant.store import Store
from quant.paper_execution import PaperExecution
from quant.lab_execution import LabExecution
from quant.lab_features import FEATURES, SCHEMA


def test_pending_ack_skips_slow_statistics_and_uses_current_match_clock(tmp_path):
    store=Store(str(tmp_path/'paper.db'))
    engine=Engine(Settings(db_path=store.path,reflection_enabled=False),store,MagicMock())
    execution=PaperExecution(engine,MagicMock())
    execution.journal=MagicMock()
    execution.journal.orders.return_value=[{'id':'new','symbol':'BTCUSDT'}]
    execution.journal.get.return_value={'id':'new','state':'SUBMITTED'}
    execution.broker=MagicMock()
    execution.retry_exits=MagicMock()
    execution.bus.get.return_value=None
    with patch('quant.paper_execution.time.time_ns',side_effect=[1_000_000_000,1_010_000_000,1_020_000_000,1_030_000_000,1_030_000_000]):
        execution.tick()
    execution.broker.on_event.assert_called_once_with(1_030_000_000,{})
    execution.journal.statistics.assert_not_called()
    engine.reflection=MagicMock()
    execution.sync_reflection()
    engine.reflection.sync_completed.assert_not_called()


def test_five_second_strategy_does_not_require_subsecond_fields():
    lab=object.__new__(LabExecution)
    lab.quant=MagicMock()
    lab.quant.policy={'grid_ms':5000}
    lab.bus=MagicMock()
    lab.bus.get.return_value={'symbol':'BTCUSDT','ts':100_000,'price':100,
              'vector':[0.]*len(FEATURES),'meta':{'schema':SCHEMA,'quality':True,'entry_allowed':True}}
    with patch('quant.lab_execution.time.time',return_value=103):
        assert lab.row('BTCUSDT')['symbol']=='BTCUSDT'
    lab.quant.policy={'grid_ms':100}
    import pytest
    with patch('quant.lab_execution.time.time',return_value=103),pytest.raises(ValueError):
        lab.row('BTCUSDT')
