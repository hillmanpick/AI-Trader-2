"""Narrow image overlay; refuse drift instead of replacing unrelated modules."""
from pathlib import Path
import sys


def replace_once(path, old, new):
    source=path.read_text(encoding='utf-8')
    if source.count(old)!=1:
        raise RuntimeError(f'Image source drift at {path.name}; expected one target')
    path.write_text(source.replace(old,new,1),encoding='utf-8')


root=Path('/app/service/server')
if sys.argv[1]=='observer':
    path=root/'quant/settings.py'
    if 'private_journal_path:' not in path.read_text(encoding='utf-8'):
        replace_once(path,'    symbols: tuple =',
            "    private_journal_path: str = field(default_factory=lambda: os.getenv('QUANT_PRIVATE_JOURNAL_PATH', '/data/private-production-observer.db'))\n    symbols: tuple =")
elif sys.argv[1]=='api':
    path=root/'routes_quant.py'
    source=path.read_text(encoding='utf-8')
    if "'order_observation':" not in source:
        needle="'account_stream':worker.get('account_stream', {'status':'not_started'}),"
        replace_once(path,needle,needle+"\n                           'order_observation':worker.get('private_execution', {'reconciliation':{'ready':False,'reason':'not_reported'}}),\n                           'gateway_probe':db.get('private_gateway_probe',{}),")
    source=path.read_text(encoding='utf-8')
    if "'execution_runtime':db.get('execution_runtime'" not in source:
        needle="'execution_statistics':db.get('execution_statistics',{}),"
        replace_once(path,needle,needle+"\n                'execution_runtime':db.get('execution_runtime',{}),")
else:
    raise ValueError('Choose observer or api')
