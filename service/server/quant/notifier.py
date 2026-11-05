import smtplib
import ssl
import time
from .mail_format import build_message, local_time, report_body


def quiet(now=None):
    return local_time(now).hour < 8


def report_slot(now=None):
    t = local_time(now)
    # Each two-hour slot is delivered once; after a restart catch up within slot.
    return None if t.hour < 8 else f'{t:%Y-%m-%d}-{t.hour//2*2:02d}'


class Notifier:
    def __init__(self,cfg,store):
        self.cfg,self.store = cfg,store
        self.next_attempt = 0

    def tick(self,positions,status,now=None,account_snapshot=None,slot_namespace=''):
        if quiet(now):
            return
        slot = report_slot(now)
        if slot_namespace:
            slot = slot_namespace + ':' + slot
        if self.store.get('report_slot') != slot:
            account = self.store.get('account',{}) if account_snapshot is None else account_snapshot
            performance = None
            if self.cfg.mode == 'paper':
                from .paper_performance import paper_performance
                performance = paper_performance(self.store,now)
            self.store.event('持仓报告',report_body(self.cfg,positions,status,account,now,performance))
            self.store.set('report_slot',slot)
        if not self.cfg.mail_ready or time.monotonic() < self.next_attempt:
            return
        pending = self.store.events(pending=True,limit=100)
        if not pending:
            return
        message = build_message(self.cfg,pending,now)
        # SSL + certificate validation; QQ needs an SMTP authorization code.
        self.next_attempt = time.monotonic()+60
        try:
            with smtplib.SMTP_SSL(self.cfg.smtp_host,self.cfg.smtp_port,context=ssl.create_default_context(),timeout=15) as smtp:
                smtp.login(self.cfg.smtp_user,self.cfg.smtp_password)
                smtp.send_message(message)
            self.store.mark_mailed([e['id'] for e in pending])
            self.store.set('mail_status',{'ok':True,'at':local_time(now).isoformat()})
        except (OSError,smtplib.SMTPException):
            self.store.set('mail_status',{'ok':False,'error':'QQ SMTP failed; check address/authorization code/connectivity'})
