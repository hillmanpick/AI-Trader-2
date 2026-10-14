"""Server-side initialization; existing credentials/config are never replaced."""
import os
import secrets
import subprocess
from pathlib import Path

root = Path(__file__).resolve().parent
env = root/'.env'
if not env.exists():
    text = (root/'.env.example').read_text()
    text = text.replace('POSTGRES_PASSWORD=\n','POSTGRES_PASSWORD='+secrets.token_hex(24)+'\n')
    text = text.replace('QUANT_API_TOKEN=\n','QUANT_API_TOKEN='+secrets.token_hex(32)+'\n')
    env.write_text(text)
    env.chmod(0o600)
settings = dict(line.split('=',1) for line in env.read_text().splitlines() if '=' in line and not line.startswith('#'))
public_host = settings.get('PUBLIC_HOST','124.156.199.59')
https_port = settings.get('WEB_HTTPS_PORT','8443')
if not (root/'htpasswd').exists():
    password = secrets.token_urlsafe(24)
    encrypted = subprocess.run(['openssl','passwd','-6','-stdin'],input=password+'\n',text=True,capture_output=True,check=True).stdout.strip()
    # nginx's crypt() supports SHA-512 password hashes on Alpine.
    (root/'htpasswd').write_text('admin:'+encrypted+'\n')
    (root/'htpasswd').chmod(0o644)
    (root/'access.txt').write_text(f'URL: https://{public_host}:{https_port}/quant\nUsername: admin\nPassword: '+password+'\n')
    (root/'access.txt').chmod(0o600)
(root/'tls').mkdir(exist_ok=True)
if not (root/'tls/server.key').exists():
    import ipaddress
    try:
        ipaddress.ip_address(public_host)
        san = 'IP:'+public_host
    except ValueError:
        san = 'DNS:'+public_host
    subprocess.run(['openssl','req','-x509','-nodes','-newkey','rsa:2048','-days','365','-keyout',str(root/'tls/server.key'),'-out',str(root/'tls/server.crt'),'-subj','/CN='+public_host,'-addext','subjectAltName='+san],check=True,capture_output=True)
    (root/'tls/server.key').chmod(0o600)
(root/'data').mkdir(exist_ok=True)
os.chown(root/'data',10001,10001)
print('Config, access credentials, TLS and persistent journal initialized; secrets omitted.')
