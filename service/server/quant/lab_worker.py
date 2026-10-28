"""Credential-free all-USDT discovery and bounded candidate microstructure feed."""
import asyncio
import json
import logging
import math
import time
from collections import deque

import aiohttp
import config  # noqa: F401: dotenv before configuration

from .lab_config import LabConfig
from .lab_features import FeatureBuilder, FEATURES, SCHEMA
from .lab_store import LabStore, RawArchive, RealtimeBus
from .order_book import DepthBook
from .event_lake import EventLake, envelope
from .market_ranker import MarketRanker
from .event_labels import EventLabeler, backfill_labels


def universe(info):
    return {s['symbol']:s for s in info['symbols'] if s.get('status') == 'TRADING'
            and s.get('contractType') == 'PERPETUAL' and s.get('quoteAsset') == 'USDT'
            and s.get('marginAsset') == 'USDT'}


def select_candidates(rules, tickers, cfg):
    liquid = [s for s in rules if float(tickers.get(s,{}).get('q',0)) >= cfg.min_volume]
    ranked = sorted(liquid, key=lambda s: float(tickers[s]['q']), reverse=True)
    # BTC is mandatory context, not an implicit trade authorization.
    return tuple(dict.fromkeys((['BTCUSDT'] if 'BTCUSDT' in liquid else [])+ranked))[:cfg.candidates]


class Collector:
    def __init__(self, cfg):
        self.cfg = cfg.validate()
        self.bus, self.store = RealtimeBus(cfg.redis_url), LabStore(cfg.directory)
        self.archive = RawArchive(cfg.directory,cfg.raw_mb_day)
        self.builder = FeatureBuilder()
        self.rules, self.tickers, self.books, self.event_ms = {}, {}, {}, {}
        self.buffers, self.snapshot_tasks = {}, {}
        self.selected = ()
        self.raw = deque(maxlen=30000)
        self.raw_dropped = 0
        self.errors = 0
        self.reconnects = 0
        self.received_ms = 0
        self.started = time.time()
        self.last_train = 0
        self.lake = EventLake(cfg.directory,int(cfg.event_max_gb*1024**3),cfg.retention_days*24)
        self.ranker = MarketRanker()
        self.event_queue = asyncio.Queue(maxsize=20000)
        self.trade_checkpoints = self.lake.checkpoints()
        self.trade_shards = {}
        self.trade_tasks = []
        self.trade_symbols = ()
        self.repair_gate = asyncio.Semaphore(2)
        self.repair_lock = asyncio.Lock()
        self.last_repair_call = 0.
        self.event_written = 0
        self.repairs = set()
        self.trade_notices = 0

    async def read(self, path, params=None):
        headers = {'X-MBX-APIKEY':self.cfg.market_data_key} if path=='/fapi/v1/historicalTrades' and self.cfg.market_data_key else {}
        async with self.session.get('https://fapi.binance.com'+path,params=params,headers=headers) as r:
            r.raise_for_status()
            return await r.json()

    def record(self, data):
        if not getattr(self,'legacy_raw_enabled',False):
            return
        if len(self.raw) == self.raw.maxlen:
            self.raw_dropped += 1
        self.raw.append(data)

    def prepare_selected(self, selected, now_ms):
        """Rebase feature state when a symbol enters the rotating depth pool.

        The all-market trade stream remains continuous while feature/depth
        processing is intentionally limited to candidates. A symbol therefore
        has an expected observation interval while it is out of the pool; that
        interval must not be recorded as a feed sequence gap when it returns.
        Start a fresh causal warmup instead of reusing stale feature history.
        """
        previous = set(self.selected)
        for symbol in set(selected)-previous:
            checkpoint = self.trade_checkpoints.get(symbol)
            if checkpoint is None:
                self.builder.last_trade.pop(symbol, None)
            else:
                self.builder.last_trade[symbol] = checkpoint
            self.builder.trades[symbol].clear()
            self.builder.history[symbol].clear()
            self.builder.bad_until[symbol] = int(now_ms)+60000

    async def record_event(self,data,source='websocket',received_ns=None):
        # bookTicker is a redundant best-quote projection of the depth feed.
        # Depth snapshots/diffs remain the durable reconstruction source; do
        # not spend event-lake capacity storing an otherwise unused duplicate.
        if data.get('e') == 'bookTicker':
            return
        await self.event_queue.put(envelope(data,source,received_ns))

    async def event_writer(self):
        await asyncio.to_thread(self.lake.recover)
        labeler = EventLabeler(self.cfg.horizon_seconds*1000)
        # Recover the tail that may have been archived before a crash but whose
        # in-memory labeler had not yet reached expiry. This runs beside live
        # ingestion; SQLite's idempotent primary key prevents duplicate labels.
        recovery_end = int(time.time()*1000)
        recovery_start = max(0,recovery_end-(self.cfg.horizon_seconds+30)*1000)
        asyncio.create_task(asyncio.to_thread(
            backfill_labels,self.cfg.directory,recovery_start,recovery_end,
            self.cfg.horizon_seconds*1000),name='label-tail-recovery')
        while True:
            first = await self.event_queue.get()
            batch = [first]
            deadline = time.monotonic()+2
            while len(batch)<20000 and time.monotonic()<deadline:
                try:
                    batch.append(await asyncio.wait_for(self.event_queue.get(),timeout=max(.001,deadline-time.monotonic())))
                except asyncio.TimeoutError:
                    break
            # A failed write exits the supervisor. No queue records are silently
            # dropped; restart resumes from the durable trade checkpoint.
            await asyncio.to_thread(self.lake.append,batch)
            for event in batch:
                payload = event['payload']
                if event['kind']=='depthStatus' and payload.get('status')=='sequence_gap':
                    await asyncio.to_thread(self.lake.gap,event['symbol'],'depth',
                        payload['previous_u'],payload['pu'],'unresolved',
                        json.dumps({k:payload[k] for k in ('U','u','pu','previous_u')}))
            # Labels only use durably archived events; never sampled quotes or
            # a REST repair arriving after a trading decision.
            for event in sorted(batch,key=lambda e:e['received_ns']):
                labeler.consume(event)
            labels = labeler.drain()
            if labels:
                await asyncio.to_thread(self.store.append_labels,labels)
            self.event_written += len(batch)
            for _ in batch:
                self.event_queue.task_done()

    async def seed(self, symbol):
        book = self.books[symbol]
        task = asyncio.current_task()
        try:
            # Buffer live diffs while the REST snapshot is in flight.
            snapshot = await self.read('/fapi/v1/depth', {'symbol':symbol,'limit':1000})
            now = int(time.time()*1000)
            # A late REST response belongs only to the feed generation that
            # requested it. Never seed a replacement book after reconnect.
            if self.books.get(symbol) is not book:
                return
            book.seed(snapshot,now)
            self.record({'e':'depthSnapshot','s':symbol,**snapshot})
            await self.record_event({'e':'depthSnapshot','s':symbol,**snapshot},'rest_snapshot')
            for event, received in list(self.buffers[symbol]):
                if int(event['u']) >= book.last:
                    book.apply(event,received)
                    if book.last is None:
                        break
            self.buffers[symbol].clear()
        except Exception as exc:
            logging.warning('Depth snapshot %s: %s',symbol,type(exc).__name__)
            self.errors += 1
        finally:
            if self.snapshot_tasks.get(symbol) is task:
                self.snapshot_tasks.pop(symbol,None)

    async def reset_depth_feed(self, symbols, status):
        for symbol in symbols:
            if symbol in self.books:
                self.books[symbol].invalidate()
        tasks = {s:self.snapshot_tasks[s] for s in symbols if s in self.snapshot_tasks}
        for task in tasks.values():
            task.cancel()
        await asyncio.gather(*tasks.values(),return_exceptions=True)
        for symbol,task in tasks.items():
            if self.snapshot_tasks.get(symbol) is task:
                self.snapshot_tasks.pop(symbol,None)
        for symbol in symbols:
            if symbol in self.books:
                self.books[symbol].invalidate()
            if status == 'connected':
                self.books[symbol] = DepthBook()
            self.buffers[symbol] = deque(maxlen=10000)
        await self.record_event({'e':'depthStatus','s':'','symbols':list(symbols),
                                 'status':status},'control')
        if symbols:
            try:
                await asyncio.to_thread(self.bus.client.delete,
                    *(f'quant:lab:{kind}:{s}' for s in symbols for kind in ('book','liquidity')))
            except Exception:
                # Redis failure must not kill source ingestion or replace the
                # reconnect path; cached execution depth also expires in 3s.
                self.errors += 1

    async def accept_depth(self, event, received_ns):
        symbol, now = event['s'],received_ns//1000000
        await self.record_event(event,received_ns=received_ns)
        book = self.books[symbol]
        self.event_ms[symbol] = int(event.get('E',now))
        if symbol in self.snapshot_tasks:
            self.buffers[symbol].append((event,now))
        elif book.last is None:
            self.seed_needed(symbol)
            self.buffers[symbol].append((event,now))
        else:
            previous, gaps = book.last, book.gaps
            book.apply(event,now)
            if book.gaps > gaps:
                await self.record_event({'e':'depthStatus','s':symbol,
                    'status':'sequence_gap','previous_u':previous,
                    **{k:event[k] for k in ('U','u','pu')}},'control')
            if book.last is None or now-book.snapshot_ms > 60000:
                self.seed_needed(symbol)

    def seed_needed(self,symbol):
        if symbol not in self.snapshot_tasks:
            self.buffers[symbol] = deque(maxlen=10000)
            self.snapshot_tasks[symbol] = asyncio.create_task(self.seed(symbol))

    async def market(self):
        delay = 1
        while True:
            try:
                async with self.session.ws_connect('wss://fstream.binance.com/market/stream?streams=!ticker@arr',heartbeat=20) as ws:
                    async for frame in ws:
                        if frame.type != aiohttp.WSMsgType.TEXT:
                            continue
                        payload = json.loads(frame.data)
                        rows = payload.get('data',payload)
                        if not isinstance(rows,list):
                            continue
                        for row in rows:
                            if row.get('s') in self.rules:
                                self.tickers[row['s']] = row
                        self.received_ms = int(time.time()*1000)
                        self.record(rows)
                        delay = 1
            except Exception as exc:
                logging.warning('Universe stream: %s',type(exc).__name__)
                self.reconnects += 1
            await asyncio.sleep(delay)
            delay = min(30,delay*2)

    async def repair_trade_gap(self,symbol,start,end):
        if start>end:
            return True
        await asyncio.to_thread(self.lake.gap,symbol,'trade',start,end,'detected')
        async with self.repair_gate:
            cursor = start
            pages = 0
            try:
                while cursor<=end and pages<100:
                    # A global rate budget bounds REST weight across all shards.
                    async with self.repair_lock:
                        await asyncio.sleep(max(0,1-(time.monotonic()-self.last_repair_call)))
                        self.last_repair_call = time.monotonic()
                    if self.cfg.market_data_key:
                        rows = await self.read('/fapi/v1/historicalTrades',{'symbol':symbol,'fromId':cursor,'limit':1000})
                    else:
                        rows = await self.read('/fapi/v1/trades',{'symbol':symbol,'limit':1000})
                        rows = [r for r in rows if int(r['id'])>=cursor]
                    if not rows or int(rows[0]['id']) != cursor:
                        raise ValueError('Backfill did not bridge the exact trade ID')
                    for row in rows:
                        identity = int(row['id'])
                        if identity>end:
                            break
                        if identity != cursor:
                            raise ValueError('Backfill contains a trade ID gap')
                        await self.record_event({'e':'trade','s':symbol,'E':int(row['time']),'T':int(row['time']),
                            't':identity,'p':row['price'],'q':row['qty'],'m':row['isBuyerMaker']},'rest_backfill')
                        cursor += 1
                    pages += 1
                if cursor<=end:
                    raise ValueError('Backfill exceeded bounded recovery budget')
                await asyncio.to_thread(self.lake.gap,symbol,'trade',start,end,'repaired')
                return True
            except Exception as exc:
                await asyncio.to_thread(self.lake.gap,symbol,'trade',cursor,end,'unresolved',type(exc).__name__)
                return False

    async def accept_trade(self,event,received_ns):
        symbol,identity = event['s'],int(event['t'])
        previous = self.trade_checkpoints.get(symbol)
        if previous is not None and identity<=previous:
            return
        if previous is not None and identity>previous+1:
            self.ranker.invalidate(symbol,received_ns//1000000)
            self.builder.bad_until[symbol] = int(time.time()*1000)+60000
            # Backfill must not block ingestion for every other symbol in the
            # shard. The exact range remains recorded even if recovery fails.
            task = asyncio.create_task(self.repair_trade_gap(symbol,previous+1,identity-1))
            self.repairs.add(task)
            task.add_done_callback(self.repairs.discard)
        await self.record_event(event,'websocket',received_ns)
        self.trade_checkpoints[symbol] = identity
        price,quantity = float(event['p']),float(event['q'])
        if not math.isfinite(price) or not math.isfinite(quantity) or price<=0 or quantity<=0:
            # Binance emits ID-bearing X=NA frames with p=q=0. Preserve these
            # exact source records and ID continuity, but never feed a zero
            # price into returns or reconnect a whole shard because of one.
            self.trade_notices += 1
            if symbol in self.selected:
                self.builder.last_trade[symbol] = identity
            return
        self.ranker.trade(event)
        if symbol in self.selected:
            self.builder.trade(event | {'a':identity},received_ns//1000000)

    async def trade_stream(self, symbols, shard_id=0):
        delay = 1
        while True:
            try:
                streams = '/'.join(f'{s.lower()}@trade' for s in symbols)
                async with self.session.ws_connect('wss://fstream.binance.com/public/stream?streams='+streams,heartbeat=20) as ws:
                    await self.record_event({'e':'feedStatus','s':'','symbols':list(symbols),'status':'connected'},'control')
                    self.builder.reconnect(symbols,int(time.time()*1000))
                    self.trade_shards[shard_id] = {'status':'connected','subscribed':len(symbols),'events':0}
                    while True:
                        frame = await asyncio.wait_for(ws.receive(),timeout=15)
                        if frame.type != aiohttp.WSMsgType.TEXT:
                            raise ConnectionError('Individual trade stream closed')
                        payload = json.loads(frame.data)
                        event = payload.get('data',payload)
                        if event.get('e') == 'trade' and event.get('s') in symbols:
                            await self.accept_trade(event,time.time_ns())
                            self.trade_shards[shard_id]['events'] += 1
                            self.trade_shards[shard_id]['last_received_ms'] = int(time.time()*1000)
                        delay = 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logging.warning('Individual trade stream: %s',type(exc).__name__)
                await self.record_event({'e':'feedStatus','s':'','symbols':list(symbols),'status':'disconnected'},'control')
                self.reconnects += 1
                self.trade_shards[shard_id] = {'status':'reconnecting','subscribed':len(symbols)}
                self.builder.reconnect(symbols,int(time.time()*1000))
            await asyncio.sleep(delay)
            delay = min(30,delay*2)

    async def micro(self,symbols):
        tasks = [asyncio.create_task(self.public_stream(symbols))]
        if not self.cfg.all_market_trades:
            tasks.append(asyncio.create_task(self.trade_stream(symbols)))
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks,return_exceptions=True)

    async def public_stream(self, symbols):
        delay = 1
        while True:
            try:
                streams = '/'.join(f'{s.lower()}@{kind}' for s in symbols for kind in ('bookTicker','depth@100ms'))
                async with self.session.ws_connect('wss://fstream.binance.com/public/stream?streams='+streams,heartbeat=20) as ws:
                    await self.reset_depth_feed(symbols,'connected')
                    while True:
                        frame = await asyncio.wait_for(ws.receive(),timeout=15)
                        if frame.type != aiohttp.WSMsgType.TEXT:
                            raise ConnectionError('Microstructure connection closed')
                        payload = json.loads(frame.data)
                        event = payload.get('data',payload)
                        received_ns = time.time_ns()
                        now = received_ns//1000000
                        s = event.get('s')
                        if s not in symbols:
                            continue
                        self.record(event)
                        kind = event.get('e')
                        if kind == 'depthUpdate':
                            await self.accept_depth(event,received_ns)
                        else:
                            await self.record_event(event,received_ns=received_ns)
                        if kind == 'aggTrade' and not self.cfg.all_market_trades:
                            self.builder.trade(event,now)
                        elif kind == 'bookTicker':
                            # Archive best-quote events independently from reconstructed depth.
                            self.quote_count = getattr(self,'quote_count',0)+1
                        delay = 1
            except asyncio.CancelledError:
                await self.reset_depth_feed(symbols,'disconnected')
                raise
            except Exception as exc:
                logging.warning('Microstructure stream: %s',type(exc).__name__)
                self.reconnects += 1
                await self.reset_depth_feed(symbols,'disconnected')
            await asyncio.sleep(delay)
            delay = min(30,delay*2)

    def flush(self, samples, raw):
        for row in raw:
            self.archive.append(row)
        self.archive.flush()
        self.store.append(samples)

    async def fast_books(self):
        """Execution depth has its own cadence; feature sampling cannot delay exits."""
        revisions = {}
        while True:
            now = int(time.time()*1000)
            updates = []
            for symbol in self.selected:
                book = self.books.get(symbol)
                if not book or not book.ready:
                    if symbol in revisions:
                        try:
                            await asyncio.to_thread(self.bus.client.delete,
                                'quant:lab:book:'+symbol,'quant:lab:liquidity:'+symbol)
                            revisions.pop(symbol,None)
                        except Exception:
                            self.errors += 1
                    continue
                if revisions.get(symbol)==book.last:
                    continue
                try:
                    liquidity = book.metrics(now,self.cfg.depth_bps)
                except ValueError:
                    continue
                raw = {'lastUpdateId':book.last,'updated_ms':book.updated_ms,
                       'bids':sorted(book.bids.items(),reverse=True)[:100],
                       'asks':sorted(book.asks.items())[:100]}
                updates.extend([('book:'+symbol,raw),('liquidity:'+symbol,liquidity)])
                revisions[symbol] = book.last
            if updates:
                try:
                    def write():
                        pipe = self.bus.client.pipeline(transaction=False)
                        for key,value in updates:
                            pipe.set('quant:lab:'+key,json.dumps(value,allow_nan=False),ex=3)
                        pipe.execute()
                        self.bus.event('depth')
                    await asyncio.to_thread(write)
                except Exception:
                    self.errors += 1
                    revisions.clear()
            await asyncio.sleep(.1)

    async def publish(self):
        while True:
            now = int(time.time()*1000)
            rows, blocked = [], {}
            for s in self.selected:
                try:
                    book = self.books[s].metrics(now,self.cfg.depth_bps)
                    if book['spread_bps'] > self.cfg.max_spread_bps:
                        raise ValueError('Spread above limit')
                    row = self.builder.build(s,book,now,self.event_ms.get(s,0))
                    row['meta']['filters'] = self.rules[s].get('filters',[])
                    await self.record_event({'e':'feature','s':s,'E':row['ts'],**row},'derived')
                    rows.append(row)
                    await asyncio.to_thread(self.bus.put,'feature:'+s,row,15)
                except (KeyError,ValueError) as exc:
                    blocked[s] = str(exc)
                except Exception:
                    blocked[s] = 'Redis unavailable'
                    self.errors += 1
            raw = list(self.raw)
            self.raw.clear()
            await asyncio.to_thread(self.flush,rows,raw)
            rank_time = int(time.time()*1000)
            ranking = self.ranker.rank(self.rules,self.tickers,rank_time,self.cfg.min_volume)
            try:
                await asyncio.to_thread(self.bus.put,'ranking',{'ts':rank_time,'rows':ranking},20)
                await asyncio.to_thread(self.bus.put,'portfolio_returns',self.ranker.return_snapshot(rank_time),20)
                await self.record_event({'e':'ranking','s':'','E':rank_time,'rows':ranking},'derived')
            except Exception:
                self.errors += 1
            depth_gap_symbols = sorted(s for s,b in self.books.items()
                                       if getattr(b,'sequence_gap_active',False))
            state = {'status':'collecting','ts':now,'universe_count':len(self.rules),
                     'candidates':list(self.selected),'ready_symbols':[r['symbol'] for r in rows],
                     'blocked':blocked,'ticker_age_ms':max(0,int(time.time()*1000)-self.received_ms) if self.received_ms else None,
                     # Keep cumulative gaps for auditability, but expose live
                     # recovery state separately for health gates.
                     'depth_gaps':sum(b.gaps for b in self.books.values()),
                     'depth_gap_active':len(depth_gap_symbols),
                     'depth_gap_symbols':depth_gap_symbols,
                     'trade_gaps':sum(self.builder.trade_gaps.values()),'reconnects':self.reconnects,
                     'raw_dropped':self.raw_dropped+self.archive.dropped,
                     'raw_archive_complete':not (self.raw_dropped or self.archive.dropped),'errors':self.errors,
                     'all_market_trades':self.cfg.all_market_trades,'trade_shards':self.trade_shards,
                     'trade_symbols_seen':len(self.trade_checkpoints),'event_queue':self.event_queue.qsize(),
                     'events_written':self.event_written,'ranked_ready':sum(r['ready'] for r in ranking),
                     'non_execution_trade_frames':self.trade_notices,
                     'schema':SCHEMA,'features':list(FEATURES),'uptime_seconds':int(time.time()-self.started)}
            try:
                await asyncio.to_thread(self.bus.put,'state',state,20)
                await asyncio.to_thread(self.bus.event,'sample')
            except Exception:
                self.errors += 1
            await asyncio.sleep(self.cfg.sample_seconds)

    async def maintenance(self):
        while True:
            try:
                info = await self.read('/fapi/v1/exchangeInfo')
                self.rules = universe(info)
                await self.record_event({'e':'exchangeInfo','s':'','symbols':list(self.rules.values())},'rest_rules')
                all_symbols = tuple(sorted(self.rules))
                if self.cfg.all_market_trades and all_symbols != self.trade_symbols:
                    for task in self.trade_tasks:
                        task.cancel()
                    await asyncio.gather(*self.trade_tasks,return_exceptions=True)
                    self.trade_symbols = all_symbols
                    size = self.cfg.trade_shard_size
                    self.trade_shards = {}
                    self.trade_tasks = [asyncio.create_task(self.trade_stream(all_symbols[i:i+size],i//size))
                                        for i in range(0,len(all_symbols),size)]
                for row in await self.read('/fapi/v1/ticker/24hr'):
                    if row['symbol'] in self.rules:
                        self.tickers[row['symbol']] = {'s':row['symbol'],'q':row['quoteVolume']}
                await asyncio.to_thread(self.bus.put,'universe',list(self.rules.values()),7200)
                selected = select_candidates(self.rules,self.tickers,self.cfg)
                ranking = self.ranker.rank(self.rules,self.tickers,int(time.time()*1000),self.cfg.min_volume)
                eligible = [r['symbol'] for r in ranking if r['ready'] and r.get('eligible')]
                if len(eligible)>=self.cfg.candidates:
                    selected = tuple(dict.fromkeys(['BTCUSDT']+eligible))[:self.cfg.candidates]
                # A rotating rank must not remove execution depth for an open
                # paper position. Keep its exit feed until the ledger is flat.
                import os
                from pathlib import Path
                from .store import Store
                paper_path = Path(os.getenv('QUANT_PAPER_DB_PATH','/data/quant-aggressive-paper.db'))
                if paper_path.exists():
                    held = tuple(Store(str(paper_path)).get('positions',{}))
                    selected = tuple(dict.fromkeys(selected+held))
                if selected != self.selected:
                    if getattr(self,'micro_task',None):
                        self.micro_task.cancel()
                        await asyncio.gather(self.micro_task,return_exceptions=True)
                    for task in list(self.snapshot_tasks.values()):
                        task.cancel()
                    await asyncio.gather(*list(self.snapshot_tasks.values()),return_exceptions=True)
                    self.prepare_selected(selected,int(time.time()*1000))
                    self.selected = selected
                    if selected:
                        self.micro_task = asyncio.create_task(self.micro(selected))
                # Refresh the heartbeat even when ranking has not changed;
                # otherwise a stable pool would expire and block all entries.
                await asyncio.to_thread(self.bus.put,'candidates',{'symbols':list(selected),'ts':int(time.time()*1000)},700)
                await self.record_event({'e':'candidates','s':'','symbols':list(selected)},'derived')
                await asyncio.to_thread(self.store.prune,self.cfg.retention_days)
                self.archive.prune(self.cfg.retention_days)
                await asyncio.to_thread(self.lake.prune)
                if self.cfg.auto_train and time.time()-self.last_train > 21600:
                    self.last_train = time.time()
                    # Training is a separate process/container to bound memory and CPU.
                    logging.info('Training handled by lab-trainer; collector remains responsive')
            except Exception as exc:
                logging.warning('Universe maintenance: %s',type(exc).__name__)
                self.errors += 1
            await asyncio.sleep(300)

    async def run(self):
        timeout = aiohttp.ClientTimeout(total=None,sock_connect=10,sock_read=30)
        async with aiohttp.ClientSession(timeout=timeout,trust_env=False) as self.session:
            await asyncio.gather(self.market(),self.maintenance(),self.publish(),self.event_writer(),self.fast_books())


def main():
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
    cfg = LabConfig().validate()
    if not cfg.enabled:
        raise SystemExit('QUANT_LAB_ENABLED is false')
    asyncio.run(Collector(cfg).run())


if __name__ == '__main__':
    main()
