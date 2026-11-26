"""Precise overlay on existing deployed images; retain deployed strategy/grid."""
from pathlib import Path
import sys

root=Path('/app/service/server')


def replace_once(path,old,new):
    text=path.read_text(encoding='utf-8')
    if text.count(old)!=1:
        raise RuntimeError(f'Unexpected source drift: {path.name}')
    path.write_text(text.replace(old,new,1),encoding='utf-8')


if sys.argv[1]=='paper':
    worker=root/'quant/paper_worker.py'
    if 'from .compute_bridge import install' not in worker.read_text():
        if 'import os\n' not in worker.read_text():
            replace_once(worker,'import logging\n','import logging\nimport os\n')
        needle='        engine.paper_execution = PaperExecution(engine,lab.bus)'
        replace_once(worker,needle,needle+"""
        if lab.quant and os.getenv('QUANT_COMPUTE_ENABLED','false').lower()=='true':
            from .compute_bridge import install
            install(lab.quant,enabled=True)""")
    controller=root/'quant/quant_runtime.py'
    if 'compute_reader' not in controller.read_text():
        replace_once(controller,'        observations = {n: signal(n, row, self.policy) for n in STRATEGIES}',"""
        reader = getattr(self, 'compute_reader', None)
        observations = (reader.observations(row, self.policy, STRATEGIES, signal) if reader
                        else {n: signal(n, row, self.policy) for n in STRATEGIES})
        if reader and time.monotonic() >= reader._next_status:
            e.store.set('hybrid_compute', reader.status())
            reader._next_status = time.monotonic()+2""".strip('\n'))
    runtime=root/'quant/execution_runtime.py'
    if "'hybrid_compute'" not in runtime.read_text():
        replace_once(runtime,"'execution_statistics','execution_runtime'}",
                     "'execution_statistics','execution_runtime','hybrid_compute'}")
elif sys.argv[1]=='api':
    route=root/'routes_quant.py'
    if "'hybrid_compute':db.get" not in route.read_text():
        needle="'execution_runtime':db.get('execution_runtime',{}),"
        replace_once(route,needle,needle+"\n                'hybrid_compute':db.get('hybrid_compute',{}),")
else:
    raise ValueError('Choose paper or api')
