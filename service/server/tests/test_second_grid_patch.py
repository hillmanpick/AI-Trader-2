from pathlib import Path
import sys

import pytest


ROOT=Path(__file__).resolve().parents[3]
PATCH=ROOT/'deploy'/'patch_second_grid.py'


def test_patch_function_updates_exact_deployed_constants():
    sys.path.insert(0,str(PATCH.parent))
    from patch_second_grid import patch_source
    source="""VERSION = 'systematic-flow-multi-v1'
HORIZON_MS = 600000
COOLDOWN_MS = 120000
GRID_MS = 5000
"""
    assert patch_source(source)=="""VERSION = 'systematic-flow-second-v1'
HORIZON_MS = 600000
COOLDOWN_MS = 120000
GRID_MS = 1000
"""
    with pytest.raises(RuntimeError,match='changed'):
        patch_source(source.replace('GRID_MS = 5000','GRID_MS = 1000'))


def test_status_patch_exposes_the_actual_policy_cadence():
    sys.path.insert(0,str(PATCH.parent))
    from patch_second_grid import patch_status_source
    source="'enabled': True, 'at': utcnow(), 'version': VERSION,"
    result=patch_status_source(source)
    assert "'grid_ms': self.policy['grid_ms']" in result
    assert "'horizon_ms': self.policy['horizon_ms']" in result
    assert "'cooldown_ms': self.policy['cooldown_ms']" in result
    with pytest.raises(RuntimeError,match='changed'):
        patch_status_source(source.replace("'enabled': True","'enabled': False"))
