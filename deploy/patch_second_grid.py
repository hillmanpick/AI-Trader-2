"""Set only the deployed paper strategy's decision cadence to one second."""
from pathlib import Path
import sys


def patch_source(source):
    old_version = "VERSION = 'systematic-flow-multi-v1'"
    new_version = "VERSION = 'systematic-flow-second-v1'"
    old_grid = 'GRID_MS = 5000'
    new_grid = 'GRID_MS = 1000'
    if source.count(old_version) != 1 or source.count(old_grid) != 1:
        raise RuntimeError('Deployed strategy source changed, refusing cadence patch')
    return source.replace(old_version, new_version, 1).replace(old_grid, new_grid, 1)


def patch_status_source(source):
    needle="'enabled': True, 'at': utcnow(), 'version': VERSION,"
    replacement=needle+"""
                'grid_ms': self.policy['grid_ms'],
                'horizon_ms': self.policy['horizon_ms'],
                'cooldown_ms': self.policy['cooldown_ms'],"""
    if source.count(needle)!=1:
        raise RuntimeError('Deployed quant status source changed, refusing status patch')
    return source.replace(needle,replacement,1)


if __name__ == '__main__':
    target = Path(sys.argv[1]) if len(sys.argv) == 2 else Path('/app/service/server/quant/quant_strategy.py')
    target.write_text(patch_source(target.read_text(encoding='utf-8')), encoding='utf-8')
    runtime = Path('/app/service/server/quant/quant_runtime.py')
    runtime.write_text(patch_status_source(runtime.read_text(encoding='utf-8')), encoding='utf-8')
