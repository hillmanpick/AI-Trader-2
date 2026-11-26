"""Standalone Windows training assistant and bundled research runtime."""
import argparse
import ctypes
import hashlib
from datetime import datetime, timedelta, timezone
import json
import multiprocessing
import os
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
import threading
import time

from hybrid_training import atomic_json
from local_training_worker import Worker, hidden_process_options

APP_NAME = 'AI-Trader 本地训练助手'
APP_VERSION = '1.0.1'
APP_DIR = Path(os.getenv('LOCALAPPDATA', str(Path.home())))/'AI-Trader-LocalTraining'
LEGACY = Path('D:/aispea/jioayi1/AI-Trader/work/hybrid-training/config.json')
UTC8 = timezone(timedelta(hours=8))


def research_dependencies():
    # Explicit imports let PyInstaller include native numeric/Arrow modules.
    # Server-supplied quant source is loaded only for an isolated research job.
    import numpy
    import sklearn.cluster
    import sklearn.linear_model
    import sklearn.preprocessing
    import sklearn.ensemble
    import pyarrow.parquet
    import pyarrow.compute
    import pyarrow.dataset
    import redis
    import requests
    import aiohttp
    import dotenv
    return {'numpy':numpy.__version__, 'sklearn':__import__('sklearn').__version__,
            'pyarrow':__import__('pyarrow').__version__}


def default_config():
    if LEGACY.exists():
        data = json.loads(LEGACY.read_text(encoding='utf-8-sig'))
    else:
        data = {'host':'ubuntu@124.156.199.59',
                'key':str(Path.home()/'.ssh/trade.pem'),
                'workspace':str(APP_DIR/'data'),
                'remote_command':'sudo -n python3 /home/ai-trader-quant/deploy/hybrid_training.py rpc '
                     '--lab /home/ai-trader-quant/deploy/data/lab '
                     '--code /home/ai-trader-quant/deploy/hybrid-code '
                     '--environment /home/ai-trader-quant/deploy/hybrid-environment.json'}
    data['python'] = sys.executable
    return data


def read_json(path, default=None):
    try:
        return json.loads(Path(path).read_text(encoding='utf-8-sig'))
    except (OSError,ValueError):
        return {} if default is None else default


def format_time(value):
    if not value:
        return '—'
    return datetime.fromtimestamp(value, UTC8).strftime('%Y年%m月%d日 %H:%M:%S')


def read_log_tail(path):
    """Read legacy PowerShell UTF-16 and new UTF-8 logs without letter spacing."""
    try:
        with Path(path).open('rb') as f:
            head=f.read(200)
            utf16=head.startswith((b'\xff\xfe',b'\xfe\xff')) or b'\x00' in head
            f.seek(0,2)
            start=max(0,f.tell()-4000)
            if utf16:
                start-=start%2
            f.seek(start)
            raw=f.read()
        encoding=('utf-16-be' if head.startswith(b'\xfe\xff') else 'utf-16-le') if utf16 else 'utf-8'
        lines=raw.decode(encoding,errors='replace').lstrip('\ufeff').splitlines()
        rendered=[]
        for line in lines[-12:]:
            try:
                item=json.loads(line)
                if 'published' in item:
                    status={'rejected':'未通过验证','approved':'通过验证',
                            'insufficient_data':'数据不足'}.get(item.get('status'),item.get('status','未知'))
                    rendered.append('训练报告已回传：'+status+'；'+
                                    ('已更新模型' if item.get('promoted') else '未替换当前模型'))
                else:
                    rendered.append(line)
            except (ValueError,TypeError):
                rendered.append(line)
        return '\n'.join(rendered)
    except OSError:
        return ''


def runtime_status(config):
    workspace=Path(config['workspace'])
    connection=read_json(workspace/'connection.json')
    return {'version':APP_VERSION,'worker_running':worker_running(workspace),
            'connection':connection,'status':read_json(workspace/'status.json'),
            'last_result':read_json(workspace/'last-result.json'),
            'paused':read_json(workspace/'paused.json').get('paused',False)}


def worker_running(workspace):
    info = read_json(Path(workspace)/'worker-pid.json')
    if not info.get('pid') or os.name != 'nt':
        return False
    api = ctypes.WinDLL('kernel32', use_last_error=True)
    api.OpenProcess.restype = ctypes.c_void_p
    api.OpenProcess.argtypes = [ctypes.c_ulong,ctypes.c_int,ctypes.c_ulong]
    api.CloseHandle.argtypes = [ctypes.c_void_p]
    api.QueryFullProcessImageNameW.argtypes = [ctypes.c_void_p,ctypes.c_ulong,
                                             ctypes.c_wchar_p,ctypes.POINTER(ctypes.c_ulong)]
    process = api.OpenProcess(0x1000,False,int(info['pid']))
    if not process:
        return False
    try:
        path = ctypes.create_unicode_buffer(32768)
        length = ctypes.c_ulong(len(path))
        return bool(api.QueryFullProcessImageNameW(process,0,path,ctypes.byref(length))
                    and os.path.normcase(path.value) == os.path.normcase(info.get('executable','')))
    finally:
        api.CloseHandle(process)


def run_training(code, output):
    research_dependencies()
    code = Path(code).resolve()
    if not (code/'quant/lab_train.py').is_file():
        raise ValueError('Missing isolated training source')
    sys.path.insert(0,str(code))
    sys.argv = ['quant.lab_train','--output-directory',str(Path(output).resolve())]
    runpy.run_module('quant.lab_train',run_name='__main__')


def self_test(destination):
    """Exercise frozen native libraries, label fitting and sampled replay."""
    versions = research_dependencies()
    import numpy as np
    from sklearn.linear_model import LogisticRegression
    from sklearn.cluster import MiniBatchKMeans
    from sklearn.preprocessing import StandardScaler
    import pyarrow as pa
    import pyarrow.parquet as pq
    x=np.array([[i,j,i*j] for i in range(12) for j in range(3)],dtype=float)
    y=np.array([i%3 for i in range(len(x))])
    z=StandardScaler().fit_transform(x)
    model=LogisticRegression(max_iter=300).fit(z,y)
    centers=MiniBatchKMeans(n_clusters=3,random_state=7,n_init=3).fit(z)
    assert model.predict_proba(z).shape==(36,3) and centers.cluster_centers_.shape==(3,3)
    with tempfile.TemporaryDirectory() as directory:
        p=Path(directory)/'events.parquet'
        pq.write_table(pa.table({'price':[1.,2.,3.]}),p)
        assert pq.read_table(p).num_rows==3
    atomic_json(destination,{'ok':True,'frozen':getattr(sys,'frozen',False),
                'versions':versions,'checks':['logistic_regression','kmeans','parquet_round_trip']})


class Desktop:
    def __init__(self, config_path, auto_worker=True):
        import tkinter as tk
        from tkinter import ttk
        self.tk,self.ttk=tk,ttk
        self.path=Path(config_path)
        if not self.path.exists():
            atomic_json(self.path,default_config())
        self.config=read_json(self.path)
        self.root=tk.Tk()
        self.root.title(APP_NAME+'  v'+APP_VERSION)
        self.root.geometry('820x650')
        self.root.minsize(760,600)
        self.root.configure(bg='#f1f5f9')
        self.root.report_callback_exception=self.callback_error
        style=ttk.Style(self.root)
        style.theme_use('clam')
        style.configure('TFrame',background='#f1f5f9')
        style.configure('TLabel',background='#f1f5f9',foreground='#334155',font=('Microsoft YaHei UI',10))
        style.configure('Title.TLabel',font=('Microsoft YaHei UI',22,'bold'),foreground='#0f172a')
        style.configure('Heading.TLabel',font=('Microsoft YaHei UI',11,'bold'))
        style.configure('TButton',font=('Microsoft YaHei UI',10),padding=(12,7))
        style.configure('TEntry',padding=6)
        body=ttk.Frame(self.root,padding=24)
        body.pack(fill='both',expand=True)
        ttk.Label(body,text='AI-Trader 本地训练助手',style='Title.TLabel').pack(anchor='w')
        ttk.Label(body,text='本地训练与回放  ·  服务器采集与模拟交易  ·  离线自动兜底  ·  v'+APP_VERSION).pack(anchor='w',pady=(5,18))
        self.labels={}
        status=ttk.Frame(body)
        status.pack(fill='x')
        for label,key in [('本地任务','worker'),('服务器连接','connection'),('当前阶段','phase'),
                          ('下次训练（北京时间）','due'),('最近训练结果','result')]:
            row=ttk.Frame(status)
            row.pack(fill='x',pady=4)
            ttk.Label(row,text=label,width=24,style='Heading.TLabel').pack(side='left')
            value=tk.StringVar(value='正在读取…')
            ttk.Label(row,textvariable=value,wraplength=500).pack(side='left',fill='x',expand=True)
            self.labels[key]=value
        controls=ttk.Frame(body)
        controls.pack(fill='x',pady=(17,16))
        for title,handler in [('启动本地训练',self.start),('停止本地训练',self.stop),
                              ('检查服务器连接',self.check),('打开日志目录',self.open_logs)]:
            ttk.Button(controls,text=title,command=handler).pack(side='left',padx=(0,8))
        ttk.Separator(body).pack(fill='x',pady=(0,14))
        self.fields={}
        self.start_pending_until=0
        for title,key in [('服务器','host'),('SSH 密钥文件','key'),('本地数据目录','workspace')]:
            row=ttk.Frame(body)
            row.pack(fill='x',pady=4)
            ttk.Label(row,text=title,width=17).pack(side='left')
            field=tk.StringVar(value=self.config.get(key,''))
            ttk.Entry(row,textvariable=field).pack(side='left',fill='x',expand=True)
            if key in ('key','workspace'):
                ttk.Button(row,text='选择',command=lambda k=key:self.browse(k)).pack(side='left',padx=(6,0))
            self.fields[key]=field
        ttk.Button(body,text='保存连接配置',command=self.save).pack(anchor='e',pady=8)
        self.message=tk.StringVar(value='关闭窗口后，后台继续运行。停止本地训练后，服务器按兜底规则接手。')
        ttk.Label(body,textvariable=self.message,wraplength=760).pack(anchor='w',pady=(6,7))
        self.log=tk.Text(body,height=5,font=('Microsoft YaHei UI',9),bg='#ffffff',fg='#475569',
                         relief='flat',padx=10,pady=8)
        self.log.pack(fill='both',expand=True)
        self.root.protocol('WM_DELETE_WINDOW',self.root.destroy)
        self.refresh()
        if auto_worker:
            self.root.after(500,self.start)

    def callback_error(self,kind,error,trace):
        from tkinter import messagebox
        messagebox.showerror(APP_NAME,str(error),parent=self.root)

    def browse(self,key):
        from tkinter import filedialog
        path=filedialog.askopenfilename(title='选择 SSH 密钥',parent=self.root) if key=='key' else filedialog.askdirectory(parent=self.root)
        if path:
            self.fields[key].set(path)

    def save(self):
        from tkinter import messagebox
        if worker_running(self.config['workspace']):
            messagebox.showinfo(APP_NAME,'请先停止本地训练，再修改连接配置。',parent=self.root)
            return
        for key,value in self.fields.items():
            self.config[key]=value.get().strip()
        if not Path(self.config['key']).is_file() or not self.config['host'] or not self.config['workspace']:
            raise ValueError('请填写服务器、有效密钥文件和数据目录。')
        self.config['python']=sys.executable
        atomic_json(self.path,self.config)
        self.message.set('配置已保存；点击“启动本地训练”即可运行。')
        self.refresh()

    def start(self):
        if worker_running(self.config['workspace']):
            self.message.set('本地训练助手已在后台运行，不会重复启动。')
            return
        if time.monotonic()<self.start_pending_until:
            self.message.set('程序正在启动，请稍候…')
            return
        if not Path(self.config['key']).is_file():
            self.message.set('未找到 SSH 密钥，请选择密钥文件并保存配置。')
            return
        atomic_json(Path(self.config['workspace'])/'paused.json',{'paused':False})
        command=([sys.executable,'--worker'] if getattr(sys,'frozen',False) else
                 [sys.executable,str(Path(__file__).resolve()),'--worker'])
        env={**os.environ,'PYINSTALLER_RESET_ENVIRONMENT':'1'}
        subprocess.Popen(command+['--config',str(self.path)],env=env,**hidden_process_options())
        self.start_pending_until=time.monotonic()+30
        self.message.set('正在启动；关闭此窗口后后台继续运行。')

    def stop(self):
        from tkinter import messagebox
        if not worker_running(self.config['workspace']):
            self.message.set('本地训练已停止；服务器兜底机制继续运行。')
            return
        if not messagebox.askyesno(APP_NAME,'停止本地训练及其正在运行的训练任务？服务器会在旧租约失效、本地离线超过 10 分钟后接手到期任务。',parent=self.root):
            return
        info=read_json(Path(self.config['workspace'])/'worker-pid.json')
        atomic_json(Path(self.config['workspace'])/'paused.json',{'paused':True})
        subprocess.run(['taskkill','/PID',str(info['pid']),'/T','/F'],capture_output=True,**hidden_process_options())
        self.message.set('本地训练已停止。服务器采集与模拟交易继续运行。')
        self.refresh()

    def check(self):
        self.message.set('正在检查服务器连接…')
        def run():
            try:
                value=Worker(self.config).rpc('status')
                atomic_json(Path(self.config['workspace'])/'connection.json',
                            {'at':time.time(),'connected':True,'state':value['state']})
                text='服务器连接正常。'
            except Exception as exc:
                atomic_json(Path(self.config['workspace'])/'connection.json',
                            {'at':time.time(),'connected':False,'error':str(exc)[:400]})
                text='连接失败：'+str(exc)[:250]
            try:
                self.root.after(0,lambda:self.message.set(text))
            except RuntimeError:
                pass
        threading.Thread(target=run,daemon=True).start()

    def open_logs(self):
        Path(self.config['workspace']).mkdir(parents=True,exist_ok=True)
        os.startfile(self.config['workspace'])

    def refresh(self):
        workspace=Path(self.config['workspace'])
        running=worker_running(workspace)
        starting=not running and time.monotonic()<self.start_pending_until
        self.labels['worker'].set('后台运行中' if running else ('正在启动…' if starting else '已停止'))
        connection=read_json(workspace/'connection.json')
        if time.time()-connection.get('at',0)>120:
            connection_text='尚未检查服务器连接'
        else:
            connection_text='已连接' if connection.get('connected') else '连接中断，等待重试'
        self.labels['connection'].set(connection_text)
        status=read_json(workspace/'status.json')
        phase=status.get('phase')
        names={'snapshot':'服务器生成一致性快照','download':'正在下载训练数据',
               'extract':'校验与解包数据','training':'正在本地训练和回放',
               'completed':'本轮训练已完成','waiting':'等待下一轮训练'}
        text=names.get(phase,'准备就绪') if running else ('正在启动后台进程…' if starting else '本地训练已停止')
        archive=workspace/'snapshot.tar'
        if phase=='download' and running and archive.exists():
            done,total=archive.stat().st_size,status.get('bytes',0)
            text+=f'  {done/1024**3:.2f} / {total/1024**3:.2f} GB' if total else f'  {done/1024**3:.2f} GB'
        self.labels['phase'].set(text)
        self.labels['due'].set(format_time(connection.get('state',{}).get('next_due')))
        result=read_json(workspace/'last-result.json')
        result_name={'approved':'通过验证','rejected':'未通过验证，未替换当前模型',
                     'insufficient_data':'训练数据不足','insufficient_clean_labels':'有效标签不足'}
        self.labels['result'].set((format_time(result.get('at'))+'  '+result_name.get(result.get('status'),result.get('status','—'))) if result else '尚无训练报告')
        logs=read_log_tail(workspace/'assistant.log')
        if not logs:
            logs='本地助手已启动，等待下一轮任务。' if running else '本地助手尚未启动。'
            if result:
                logs+='\n最近训练：'+format_time(result.get('at'))+'，'+result_name.get(result.get('status'),result.get('status','未知'))+'。'
        self.log.configure(state='normal')
        self.log.delete('1.0','end')
        self.log.insert('1.0',logs)
        self.log.configure(state='disabled')
        self.root.after(2000,self.refresh)


def main():
    multiprocessing.freeze_support()
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--worker',action='store_true')
    parser.add_argument('--once',action='store_true')
    parser.add_argument('--train',action='store_true')
    parser.add_argument('--code')
    parser.add_argument('--output')
    parser.add_argument('--self-test')
    parser.add_argument('--diagnose',help='Write local runtime status as JSON, without changing jobs')
    parser.add_argument('--no-worker',action='store_true')
    parser.add_argument('--config',default=str(APP_DIR/'config.json'))
    args=parser.parse_args()
    if args.self_test:
        self_test(args.self_test)
    elif args.diagnose:
        atomic_json(args.diagnose,runtime_status(read_json(args.config)))
    elif args.train:
        output=Path(args.output)
        output.mkdir(parents=True,exist_ok=True)
        with (output/'training.log').open('a',encoding='utf-8',buffering=1) as log:
            sys.stdout=sys.stderr=log
            try:
                run_training(args.code,args.output)
            except Exception:
                import traceback
                traceback.print_exc()
                raise
    elif args.worker:
        config=read_json(args.config)
        if read_json(Path(config['workspace'])/'paused.json').get('paused'):
            return
        # Reserve the worker before opening its log. The scheduled task and a
        # double-click can launch together while onefile extraction is ongoing.
        api=ctypes.WinDLL('kernel32',use_last_error=True)
        api.CreateMutexW.argtypes=[ctypes.c_void_p,ctypes.c_int,ctypes.c_wchar_p]
        api.CreateMutexW.restype=ctypes.c_void_p
        mutex_name='Local\\AITraderTraining-'+hashlib.sha256(
            str(Path(config['workspace']).resolve()).casefold().encode()).hexdigest()[:24]
        mutex=api.CreateMutexW(None,False,mutex_name)
        if not mutex:
            raise ctypes.WinError(ctypes.get_last_error())
        if ctypes.get_last_error()==183:
            return
        log_path=Path(config['workspace'])/'assistant.log'
        log_path.parent.mkdir(parents=True,exist_ok=True)
        with log_path.open('a',encoding='utf-8',buffering=1) as log:
            sys.stdout=sys.stderr=log
            print(format_time(time.time())+' 本地训练助手 v'+APP_VERSION+' 后台启动',flush=True)
            try:
                Worker(config).run(args.once)
            except OSError as exc:
                print('后台任务已在运行或无法启动：',str(exc),flush=True)
    else:
        app=Desktop(args.config,not args.no_worker)
        app.root.mainloop()


if __name__=='__main__':
    try:
        main()
    except Exception:
        import traceback
        APP_DIR.mkdir(parents=True,exist_ok=True)
        (APP_DIR/'crash.log').write_text(traceback.format_exc(),encoding='utf-8')
        raise
