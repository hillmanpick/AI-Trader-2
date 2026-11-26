"""Local public-feature compute worker; no Binance key, exchange or order API."""
import argparse
import asyncio
from contextlib import contextmanager
import hashlib
import importlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
import time
import uuid

import aiohttp

SOURCES = {'quant_strategy.py','execution_policy.py','lab_features.py'}


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def load_evaluator(bundle, workspace):
    """Only our own server's three hashed public research modules are loaded."""
    sources=bundle.get('sources')
    if not isinstance(sources,dict) or set(sources)!=SOURCES or digest(sources)!=bundle.get('code_id'):
        raise ValueError('Strategy bundle identity mismatch')
    if any(not isinstance(s,str) or len(s.encode())>256*1024 for s in sources.values()):
        raise ValueError('Strategy bundle outside bounds')
    package='compute_'+bundle['code_id']
    root=Path(workspace)/'code'
    folder=root/package
    folder.mkdir(parents=True,exist_ok=True)
    (folder/'__init__.py').write_text('',encoding='utf-8')
    for name,source in sources.items():
        # Fixed names only, verified through SSH + dedicated token. Never .env.
        (folder/name).write_text(source,encoding='utf-8')
    if str(root) not in sys.path:
        sys.path.insert(0,str(root))
    module=importlib.import_module(package+'.quant_strategy')
    return module


def evaluate_batch(module, batch, code_id):
    if (batch.get('type')!='jobs' or batch.get('code_id')!=code_id
            or batch.get('strategies')!=list(module.STRATEGIES)
            or not isinstance(batch.get('jobs'),list) or not 1<=len(batch['jobs'])<=40):
        raise ValueError('Invalid strategy batch')
    results={}
    for job in batch['jobs']:
        if digest({'row':job['row'],'policy':batch['policy'],'code_id':code_id})!=job['job_id']:
            raise ValueError('Public feature input hash mismatch')
        results[job['job_id']]={name:module.signal(name,job['row'],batch['policy'])
                               for name in module.STRATEGIES}
    return {'type':'results','generation':batch['generation'],'session':batch['session'],
            'code_id':code_id,'batch_id':batch['batch_id'],'results':results}


def hidden_options():
    return {'creationflags':subprocess.CREATE_NO_WINDOW} if os.name=='nt' else {}


@contextmanager
def ssh_tunnel(config):
    port=int(config.get('local_port',18766))
    remote_port=int(config.get('remote_port',18765))
    if not 1024<=port<=65535 or not 1024<=remote_port<=65535:
        raise ValueError('Invalid SSH tunnel port')
    # An already occupied local port is a configuration error, never assume it
    # is a trustworthy tunnel and send the service token to somebody else.
    check=socket.socket()
    try:
        check.bind(('127.0.0.1',port))
    finally:
        check.close()
    command=['ssh','-N','-i',config['key'],'-o','BatchMode=yes',
        '-o','ExitOnForwardFailure=yes','-o','ConnectTimeout=10',
        '-o','ServerAliveInterval=10','-o','ServerAliveCountMax=2',
        '-L',f'127.0.0.1:{port}:127.0.0.1:{remote_port}',config['host']]
    process=subprocess.Popen(command,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,**hidden_options())
    try:
        deadline=time.monotonic()+15
        while time.monotonic()<deadline:
            if process.poll() is not None:
                raise RuntimeError('SSH forwarding unavailable; server computation remains active')
            try:
                with socket.create_connection(('127.0.0.1',port),timeout=.2):
                    break
            except OSError:
                time.sleep(.1)
        else:
            raise RuntimeError('SSH tunnel did not become ready')
        yield f'http://127.0.0.1:{port}'
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


class Worker:
    def __init__(self,config,stop=None):
        self.config=config
        self.root=Path(config['workspace'])
        self.root.mkdir(parents=True,exist_ok=True)
        self.stop=stop or threading.Event()
        self.status={'state':'starting','batches':0,'jobs':0,'executor':'server_only',
                     'real_execution_enabled':False}
        self.node=config.get('node','windows-'+uuid.uuid4().hex[:8])

    def save(self,**values):
        self.status.update(values,at=time.time())
        target=self.root/'status.json'
        tmp=target.with_suffix('.tmp')
        tmp.write_text(json.dumps(self.status,ensure_ascii=False,allow_nan=False,indent=2),encoding='utf-8')
        os.replace(tmp,target)

    async def session(self,url,seconds=None):
        headers={'X-Compute-Token':self.config['token'],'X-Compute-Node':self.node}
        timeout=aiohttp.ClientTimeout(total=None,sock_connect=5)
        started=time.monotonic()
        async with aiohttp.ClientSession(headers=headers,timeout=timeout,trust_env=False) as client:
            async with client.get(url+'/bundle') as response:
                if response.status!=200:
                    raise RuntimeError('Compute service authentication/strategy bundle unavailable')
                bundle=await response.json()
            module=load_evaluator(bundle,self.root)
            async with client.ws_connect(url+'/compute',heartbeat=10,max_msg_size=512*1024) as ws:
                self.save(state='connected',code_id=bundle['code_id'])
                while not self.stop.is_set() and (seconds is None or time.monotonic()-started<seconds):
                    try:
                        message=await ws.receive(timeout=.5)
                    except asyncio.TimeoutError:
                        continue
                    if message.type!=aiohttp.WSMsgType.TEXT:
                        break
                    batch=message.json()
                    if batch.get('type')=='hello':
                        if batch.get('code_id')!=bundle['code_id']:
                            raise ValueError('Server strategy changed; reconnect required')
                        self.save(generation=batch['generation'])
                        continue
                    before=time.perf_counter_ns()
                    result=evaluate_batch(module,batch,bundle['code_id'])
                    await ws.send_json(result)
                    self.save(batches=self.status['batches']+1,
                        jobs=self.status['jobs']+len(result['results']),
                        last_compute_ms=(time.perf_counter_ns()-before)/1e6)
                await ws.close()

    def run(self,seconds=None,tunnel=True):
        # One local client per workspace; duplicate GUI/tasks fail before
        # opening the tunnel. Kernel lock is released even after a crash.
        with (self.root/'compute.lock').open('a+b') as lock:
            if os.name=='nt':
                import msvcrt
                lock.write(b'0');lock.flush();lock.seek(0)
                msvcrt.locking(lock.fileno(),msvcrt.LK_NBLCK,1)
            else:
                import fcntl
                fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            while not self.stop.is_set():
                try:
                    if tunnel:
                        with ssh_tunnel(self.config) as url:
                            asyncio.run(self.session(url,seconds))
                    else:
                        asyncio.run(self.session(self.config['url'],seconds))
                except Exception as exc:
                    self.save(state='disconnected_server_fallback',error_type=type(exc).__name__)
                if seconds is not None:
                    break
                if self.stop.wait(2):
                    break
            self.save(state='stopped_server_fallback')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',required=True)
    parser.add_argument('--seconds',type=float)
    args=parser.parse_args()
    config=json.loads(Path(args.config).read_text(encoding='utf-8-sig'))
    Worker(config).run(args.seconds)


if __name__=='__main__':
    main()
