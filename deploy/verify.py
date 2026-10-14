"""Run on server to verify HTTPS + auth + API without displaying secrets."""
import base64
import json
import ssl
import socket
import urllib.error
import urllib.request
from pathlib import Path

root = Path(__file__).resolve().parent
values = dict(line.split(': ',1) for line in (root/'access.txt').read_text().splitlines())
token = base64.b64encode((values['Username']+':'+values['Password']).encode()).decode()
context = ssl.create_default_context(cafile=str(root/'tls/server.crt'))
context.check_hostname = False  # localhost probe, certificate is issued for public IP
settings = dict(line.split('=',1) for line in (root/'.env').read_text().splitlines() if '=' in line and not line.startswith('#'))
base = 'https://127.0.0.1:'+settings.get('WEB_HTTPS_PORT','9443')
headers = {'Authorization':'Basic '+token}

try:
    urllib.request.urlopen(base+'/api/quant/status',context=context,timeout=10)
    raise RuntimeError('Anonymous request must be denied')
except urllib.error.HTTPError as exc:
    assert exc.code==401, exc.code
print('Anonymous access: 401 (expected)')
for path in ['/quant','/api/quant/status','/api/quant/orders']:
    request = urllib.request.Request(base+path,headers=headers)
    with urllib.request.urlopen(request,context=context,timeout=15) as response:
        body = response.read()
        assert response.status==200
        if path.endswith('/status'):
            data=json.loads(body)
            print(json.dumps({k:data[k] for k in ['config','worker','stale','paused','account','mail']},ensure_ascii=False))
    print(path+': 200')
# Exercise harmless control writes; preserve existing pause state.
paused = data['paused']
for action in ['pause','pause' if paused else 'resume']:
    request=urllib.request.Request(base+'/api/quant/'+action,data=b'',headers=headers|{'X-Quant-Action':'operator'},method='POST')
    with urllib.request.urlopen(request,context=context,timeout=15) as response:
        assert response.status==200
print('Pause/resume API verified; previous pause state restored')
try:
    with socket.create_connection(('smtp.qq.com',465),timeout=10) as connection:
        with ssl.create_default_context().wrap_socket(connection,server_hostname='smtp.qq.com') as smtp:
            print('QQ SMTP SSL reachable: '+str(smtp.recv(128).startswith(b'220')))
except OSError:
    print('QQ SMTP SSL connection failed; delivery needs network review')
