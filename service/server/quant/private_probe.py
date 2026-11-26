"""Read-only Binance gateway RTT and forced user-stream recovery evidence.

This CLI has no trading option. Existing production credentials are used ONLY
for signed reads. Testnet has a different environment variable namespace.
"""
import argparse
import asyncio
from dataclasses import replace
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time

from .account_stream import AccountStream
from .binance import Binance
from .observe import ReadOnlyBinance
from .private_gateway import GatewayConfig, PrivateGateway
from .private_journal import PrivateJournal
from .private_reconcile import PrivateReconciler
from .settings import Settings


class DemoReadOnlyBinance(ReadOnlyBinance):
    def __init__(self, cfg):
        # Explicit testnet client; never redirect the existing production reader.
        Binance.__init__(self,replace(cfg,mode='testnet',enabled=False,live_enabled=False,observation_only=True))
        self._symbol_configs,self._symbol_config_next = [],0.


def configuration(environment):
    prefix = 'BINANCE_TESTNET_' if environment=='testnet' else 'BINANCE_'
    return GatewayConfig(environment=environment,
        api_key=os.getenv(prefix+'API_KEY',''),api_secret=os.getenv(prefix+'API_SECRET',''))


async def user_stream_recovery(config, journal_path, symbols):
    cfg=replace(Settings(),api_key=config.api_key,api_secret=config.api_secret,
                enabled=False,live_enabled=False,observation_only=True)
    api=DemoReadOnlyBinance(cfg) if config.environment=='testnet' else ReadOnlyBinance(cfg)
    journal=PrivateJournal(journal_path,config.environment,config.api_key)
    stream=AccountStream(api)
    recovery=PrivateReconciler(journal,api,symbols,stream=stream)
    stream.on_order,stream.on_gap,stream.on_connected=recovery.enqueue,recovery.gap,recovery.connected
    result={'scope':'this probe user stream only; read-only recovery, no orders sent',
            'before':None,'after':None,'forced_disconnect_recovered':False}
    try:
        await asyncio.to_thread(api.sync_time)
        stream.start()
        deadline=time.monotonic()+30
        while time.monotonic()<deadline and stream.snapshot_status()['status']!='connected':
            await asyncio.sleep(.1)
        result['before']=await asyncio.to_thread(recovery.run)
        if not result['before'].get('ready'):
            return result
        generation=stream.snapshot_status().get('connection_generation',0)
        started=time.perf_counter_ns()
        stream.request_reconnect()
        deadline=time.monotonic()+30
        while time.monotonic()<deadline:
            status=stream.snapshot_status()
            if status.get('connection_generation',0)>generation and status['status']=='connected':
                break
            await asyncio.sleep(.1)
        else:
            result['error']='User stream reconnect deadline exceeded'
            return result
        result['after']=await asyncio.to_thread(recovery.run)
        result['forced_disconnect_recovered']=bool(result['after'].get('ready'))
        result['blackout_and_reconciliation_ms']=(time.perf_counter_ns()-started)/1e6
        result['stream']=stream.snapshot_status()
        return result
    finally:
        await asyncio.to_thread(stream.stop)
        api.session.close()


async def probe(config, samples=30, interval=.5, public_only=False,
                recovery_journal=None, symbols=('BTCUSDT','ETHUSDT')):
    gateway=PrivateGateway(config)
    result={'at':datetime.now(timezone.utc).isoformat(),'environment':config.environment,
        'read_only':True,'orders_sent':0,'exchange_order_ack_measured':False,
        'private_credentials_present':bool(config.api_key and config.api_secret),
        'requested_samples_per_method':samples,'errors':[],
        'limitations':['Read-RPC RTT includes client/network/gateway processing, not matching-engine latency',
            'No live/testnet order is sent by this probe; no order-fill latency or profitability claim']}
    try:
        if config.api_key and config.api_secret and not public_only:
            cfg=replace(Settings(),api_key=config.api_key,api_secret=config.api_secret)
            api=DemoReadOnlyBinance(cfg) if config.environment=='testnet' else ReadOnlyBinance(cfg)
            try:
                await asyncio.to_thread(api.sync_time)
                gateway.offset_ms=api.offset
            finally:
                api.session.close()
        await gateway.connect()
        failures=0
        for i in range(samples):
            if i==samples//2 and samples>1:
                started=time.perf_counter_ns()
                await gateway.close()
                await gateway.connect()
                result['rpc_reconnect_ms']=(time.perf_counter_ns()-started)/1e6
            methods=['depth']
            if config.api_key and config.api_secret and not public_only:
                methods.append('v2/account.status')
            for method in methods:
                try:
                    await gateway.request(method,{'symbol':symbols[0],'limit':5} if method=='depth' else {})
                except Exception as exc:
                    failures+=1
                    result['errors'].append({'method':method,'type':type(exc).__name__,
                        'status':getattr(exc,'status',None),'code':getattr(exc,'code',None)})
            if failures>=3:
                result['aborted']='Three request errors; no retry storm'
                break
            await asyncio.sleep(interval)
    except Exception as exc:
        result['errors'].append({'phase':'connect_or_clock_sync','type':type(exc).__name__})
    finally:
        result['gateway']=gateway.status()
        await gateway.close()
    if recovery_journal:
        if not config.api_key or not config.api_secret:
            result['user_stream_recovery']={'forced_disconnect_recovered':False,'error':'Private credentials missing'}
        else:
            try:
                result['user_stream_recovery']=await user_stream_recovery(config,recovery_journal,symbols)
            except Exception as exc:
                result['user_stream_recovery']={'forced_disconnect_recovered':False,'error':type(exc).__name__}
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--environment',choices=['production','testnet'],default='production')
    parser.add_argument('--samples',type=int,default=30)
    parser.add_argument('--interval',type=float,default=.5)
    parser.add_argument('--public-only',action='store_true')
    parser.add_argument('--recovery-journal',help='Separate audit SQLite path; also forces THIS probe stream to reconnect')
    parser.add_argument('--output')
    parser.add_argument('--status-db',help='Existing account status SQLite; publish sanitized evidence only')
    args=parser.parse_args()
    if not 2<=args.samples<=200 or not .1<=args.interval<=60:
        parser.error('Use 2..200 samples and 0.1..60 second interval')
    result=asyncio.run(probe(configuration(args.environment),args.samples,args.interval,
        args.public_only,args.recovery_journal))
    if args.status_db:
        from .store import Store
        Store(args.status_db).set('private_gateway_probe',result)
    output=json.dumps(result,ensure_ascii=False,allow_nan=False,indent=2)
    if args.output:
        Path(args.output).write_text(output,encoding='utf-8')
    print(output)


if __name__=='__main__':
    main()
