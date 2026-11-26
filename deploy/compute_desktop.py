"""Small local compute launcher; credentials are never displayed in the UI."""
import argparse
import json
from pathlib import Path
import sys
import threading
import tkinter as tk
from tkinter import ttk

from local_compute_worker import Worker


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--config')
    parser.add_argument('--seconds',type=float)
    args=parser.parse_args()
    base=Path(sys.executable).parent if getattr(sys,'frozen',False) else Path(__file__).resolve().parent
    path=Path(args.config) if args.config else base/'compute-config.json'
    if args.seconds is not None:
        Worker(json.loads(path.read_text(encoding='utf-8-sig'))).run(args.seconds)
        return
    root=tk.Tk()
    root.title('AI-Trader 本地计算')
    root.geometry('640x320')
    frame=ttk.Frame(root,padding=20)
    frame.pack(fill='both',expand=True)
    title=ttk.Label(frame,text='本地计算 · 服务器执行与兜底',font=('Microsoft YaHei UI',14))
    title.pack(anchor='w')
    state=tk.StringVar(value='尚未启动')
    detail=tk.StringVar(value='本地关闭或断网后，服务器继续自行计算。实盘交易关闭。')
    ttk.Label(frame,textvariable=state,font=('Microsoft YaHei UI',11)).pack(anchor='w',pady=16)
    ttk.Label(frame,textvariable=detail,wraplength=580).pack(anchor='w')
    stop=threading.Event()
    task=None
    config=None
    labels={'connected':'已连接：本地正在处理公开特征','starting':'正在连接',
            'disconnected_server_fallback':'连接中断：服务器兜底','stopped_server_fallback':'已停止：服务器兜底'}
    def start():
        nonlocal task,config
        if task and task.is_alive():
            return
        try:
            config=json.loads(path.read_text(encoding='utf-8-sig'))
        except Exception:
            state.set('配置文件不可读取，请检查同目录 compute-config.json')
            return
        stop.clear()
        def work():
            try:
                Worker(config,stop).run()
            except Exception:
                pass
        task=threading.Thread(target=work,daemon=True)
        task.start()
    def refresh():
        if config:
            try:
                status=json.loads((Path(config['workspace'])/'status.json').read_text(encoding='utf-8'))
                state.set(labels.get(status['state'],status['state']))
                detail.set(f"已计算 {status.get('jobs',0)} 个输入，{status.get('batches',0)} 批；最近计算 {status.get('last_compute_ms',0):.2f} ms。\n本地发送的是候选信号；服务器仍检查数据、风险和成交条件，迟到即自行计算。")
            except Exception:
                pass
        root.after(1000,refresh)
    buttons=ttk.Frame(frame)
    buttons.pack(anchor='w',pady=20)
    ttk.Button(buttons,text='启动本地计算',command=start).pack(side='left',padx=(0,10))
    ttk.Button(buttons,text='停止，交给服务器',command=stop.set).pack(side='left')
    def close():
        stop.set()
        if task:
            task.join(timeout=3)
        root.destroy()
    root.protocol('WM_DELETE_WINDOW',close)
    refresh()
    start()
    root.mainloop()


if __name__=='__main__':
    main()
