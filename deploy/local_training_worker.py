"""Windows local-first worker. Uses SSH only; never starts a trading worker."""
import argparse
import hashlib
import gzip
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import threading
import time

from hybrid_training import clean_environment, atomic_json


def hidden_process_options():
    return {'creationflags': subprocess.CREATE_NO_WINDOW} if os.name == 'nt' else {}


def training_command(config, code, output):
    if getattr(sys, 'frozen', False):
        return [sys.executable, '--train', '--code', str(code), '--output', str(output)]
    return [config['python'], '-m', 'quant.lab_train', '--output-directory', str(output)]


class Worker:
    def __init__(self, config):
        self.config = config
        self.root = Path(config['workspace'])
        self.root.mkdir(parents=True, exist_ok=True)
        self.token = None
        self.lost = threading.Event()
        self.pause_until = 0
        self.lock = threading.Lock()

    def rpc(self, action, timeout=45, **values):
        cfg = self.config
        command = ['ssh', '-i', cfg['key'], '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
                   '-o', 'ServerAliveInterval=15', '-o', 'ServerAliveCountMax=2', cfg['host'],
                   cfg['remote_command']]
        r = subprocess.run(command, input=json.dumps({'action': action, **values}), text=True,
                           encoding='utf-8', capture_output=True, timeout=timeout, **hidden_process_options())
        if r.returncode:
            raise RuntimeError((r.stderr or r.stdout)[-800:])
        return json.loads(r.stdout)

    def heartbeat(self):
        while True:
            try:
                if time.time() >= self.pause_until:
                    result = self.rpc('presence')
                    atomic_json(self.root/'connection.json', {'at':time.time(),'connected':True,
                                'state':result['state']})
                with self.lock:
                    token = self.token
                if token:
                    try:
                        self.rpc('renew', token=token)
                        self.last_renewal = time.monotonic()
                    except Exception:
                        if time.monotonic()-self.last_renewal > 120:
                            self.lost.set()
            except Exception as exc:
                with self.lock:
                    if self.token and time.monotonic()-self.last_renewal > 120:
                        self.lost.set()
                print('heartbeat:', type(exc).__name__, str(exc)[:200], flush=True)
                atomic_json(self.root/'connection.json', {'at':time.time(),'connected':False,
                            'error':str(exc)[:400]})
            time.sleep(30)

    def train_once(self, token):
        def phase(name, **details):
            atomic_json(self.root/'status.json', {'phase':name,'at':time.time(),'token':token,**details})
        phase('snapshot')
        info = self.rpc('snapshot', timeout=10800, token=token)
        phase('download', bytes=info['bytes'])
        archive = self.root/'snapshot.tar'
        cfg = self.config
        subprocess.run(['scp', '-q', '-i', cfg['key'], '-o', 'BatchMode=yes',
                        '-o', 'ConnectTimeout=10', '-o', 'ServerAliveInterval=15',
                        cfg['host']+':'+info['path'], str(archive)], check=True, timeout=7200,
                        **hidden_process_options())
        with archive.open('rb') as f:
            if hashlib.file_digest(f, 'sha256').hexdigest() != info['sha256']:
                raise ValueError('Snapshot checksum mismatch')
        self.rpc('downloaded', token=token)
        phase('extract')
        # One retained snapshot only; immutable input, candidate output separate.
        import shutil
        source = self.root/'input'
        shutil.rmtree(source, ignore_errors=True)
        source.mkdir()
        with tarfile.open(archive, 'r:*') as tf:
            tf.extractall(source, filter='data')
        archive.unlink()
        with gzip.open(source/'lab/features.db.gz', 'rb') as src, (source/'lab/features.db').open('wb') as dst:
            shutil.copyfileobj(src, dst, length=1024*1024)
        (source/'lab/features.db.gz').unlink()
        (source/'code/config.py').write_text('"""Isolated research configuration: no .env loading."""\n')
        environment = clean_environment(json.loads((source/'environment.json').read_text()), source/'lab')
        environment.update(OMP_NUM_THREADS='4', OPENBLAS_NUM_THREADS='4', PYTHONUNBUFFERED='1')
        output = self.root/'results'/token
        output.mkdir(parents=True)
        with (output/'training.log').open('w', encoding='utf-8') as log:
            phase('training')
            process = subprocess.Popen(training_command(cfg,source/'code',output),
                        cwd=source/'code', env={**os.environ, **environment}, stdout=log, stderr=subprocess.STDOUT,
                        **hidden_process_options())
            try:
                started = time.monotonic()
                while process.poll() is None:
                    if self.lost.is_set() or time.monotonic()-started > 10800:
                        raise ValueError('Lease lost or training time limit exceeded')
                    time.sleep(5)
                if process.returncode:
                    raise ValueError('Training failed; see training.log')
            finally:
                if process.poll() is None:
                    process.terminate()
                    process.wait(timeout=30)
        report = json.loads((output/'latest-report.json').read_text(encoding='utf-8'))
        report['hybrid_training'] = {'worker':'local','snapshot_sha256':info['sha256'],'token':token}
        model = json.loads((output/'model.json').read_text(encoding='utf-8')) if (output/'model.json').exists() else None
        result = self.rpc('publish', token=token, report=report, model=model)
        phase('completed', result=result)
        atomic_json(self.root/'last-result.json', {**result, 'token': token, 'at': time.time()})
        print(json.dumps(result, ensure_ascii=False), flush=True)

    def run(self, once=False):
        # Kernel file lock prevents task-scheduler overlap on Windows.
        import msvcrt
        lockfile = (self.root/'worker.lock').open('a+b')
        lockfile.seek(0)
        lockfile.write(b'0')
        lockfile.flush()
        lockfile.seek(0)
        msvcrt.locking(lockfile.fileno(), msvcrt.LK_NBLCK, 1)
        atomic_json(self.root/'worker-pid.json', {'pid':os.getpid(),'started':time.time(),
                    'executable':sys.executable})
        threading.Thread(target=self.heartbeat, daemon=True).start()
        while True:
            if time.time() < self.pause_until:
                if once:
                    break
                time.sleep(60)
                continue
            try:
                claim = self.rpc('claim')
                atomic_json(self.root/'connection.json', {'at':time.time(),'connected':True,
                            'state':claim['state']})
                if claim['claimed']:
                    with self.lock:
                        self.token = claim['state']['token']
                        self.last_renewal = time.monotonic()
                    self.lost.clear()
                    try:
                        self.train_once(self.token)
                    except Exception:
                        # A working Windows host with a broken trainer must not
                        # keep suppressing fallback through presence heartbeats.
                        self.pause_until = time.time()+1200
                        try:
                            self.rpc('finish', token=self.token, success=False)
                        except Exception:
                            pass
                        raise
                    finally:
                        with self.lock:
                            self.token = None
                else:
                    atomic_json(self.root/'status.json', {'phase':'waiting','at':time.time(),
                                'next_due':claim['state'].get('next_due')})
            except Exception as exc:
                atomic_json(self.root/'last-error.json', {'at': time.time(), 'error': str(exc)})
                print(type(exc).__name__, str(exc), flush=True)
            if once:
                break
            time.sleep(60)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args()
    Worker(json.loads(Path(args.config).read_text(encoding='utf-8-sig'))).run(args.once)
