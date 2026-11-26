"""The packaged worker must invoke its own runtime, never external Python."""
import importlib.util
from pathlib import Path
import sys

DEPLOY = Path(__file__).resolve().parents[3]/'deploy'
sys.path.insert(0,str(DEPLOY))
from local_training_worker import training_command
from training_desktop import read_log_tail, worker_running


def test_frozen_training_uses_self_executable(monkeypatch,tmp_path):
    monkeypatch.setattr(sys,'frozen',True,raising=False)
    monkeypatch.setattr(sys,'executable',str(tmp_path/'Trainer.exe'))
    command=training_command({'python':'missing-python.exe'},tmp_path/'code',tmp_path/'output')
    assert command == [str(tmp_path/'Trainer.exe'),'--train','--code',str(tmp_path/'code'),
                       '--output',str(tmp_path/'output')]
    assert 'missing-python.exe' not in command


def test_source_worker_keeps_python_entry(tmp_path):
    assert training_command({'python':'python'},tmp_path/'code',tmp_path/'output') == [
        'python','-m','quant.lab_train','--output-directory',str(tmp_path/'output')]


def test_utf16_log_shows_readable_report(tmp_path):
    path=tmp_path/'legacy.log'
    path.write_text('{"published":true,"promoted":false,"status":"rejected"}\n',encoding='utf-16')
    text=read_log_tail(path)
    assert text=='训练报告已回传：未通过验证；未替换当前模型'
    assert '\x00' not in text and '\ufffd' not in text


def test_utf8_log_keeps_chinese(tmp_path):
    path=tmp_path/'assistant.log'
    path.write_text('本地助手已启动\n',encoding='utf-8')
    assert read_log_tail(path)=='本地助手已启动'


def test_worker_detection_checks_real_current_process(tmp_path):
    import json,os
    (tmp_path/'worker-pid.json').write_text(json.dumps({'pid':os.getpid(),'executable':sys.executable}))
    assert worker_running(tmp_path)
    (tmp_path/'worker-pid.json').write_text(json.dumps({'pid':os.getpid(),'executable':'C:/wrong.exe'}))
    assert not worker_running(tmp_path)
