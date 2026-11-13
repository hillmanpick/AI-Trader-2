"""Read-only radar: reuse Redis flow, enrich bounded candidates, observe outcomes."""
import asyncio
import json
import logging
import os
import sys
import time
from collections import OrderedDict

import aiohttp
import config  # noqa: F401

from .lab_config import LabConfig
from .lab_store import RealtimeBus
from .potential_radar import BAR_MS,SCHEMA,candidate,closed_bars,fast_valid,evaluate_path
from .potential_store import PotentialStore


class PublicMarket:
    PATHS={'futures':{'/fapi/v1/klines','/fapi/v1/premiumIndex','/futures/data/openInterestHist'},
           'spot':{'/api/v3/klines'}}
    BASE={'futures':'https://fapi.binance.com','spot':'https://api.binance.com'}

    def __init__(self,session,budget=12):
        self.session=session;self.maximum=budget;self.remaining=budget
        self.backoff_until=0.;self.next_request=0.;self.requests=0

    async def get(self,market,path,params=None):
        if market not in self.PATHS or path not in self.PATHS[market]:
            raise ValueError('Radar forbids account endpoints and exchange writes')
        if self.remaining<=0 or time.monotonic()<self.backoff_until:
            raise ValueError('Public request budget or backoff active')
        self.remaining-=1
        await asyncio.sleep(max(0,self.next_request-time.monotonic()))
        self.next_request=time.monotonic()+1
        self.requests+=1
        async with self.session.get(self.BASE[market]+path,params=params) as response:
            if response.status in (418,429):
                retry=response.headers.get('Retry-After','60')
                try:delay=float(retry)
                except ValueError:delay=60
                self.backoff_until=time.monotonic()+min(3600,max(900 if response.status==418 else 60,delay))
                raise ValueError('Exchange rate limit: enrichment paused')
            response.raise_for_status()
            value=await response.json()
            if isinstance(value,dict) and value.get('code',0)<0:raise ValueError('Public market response error')
            return value


class Radar:
    def __init__(self,client,bus,store,min_volume=5000000):
        self.client,self.bus,self.store=client,bus,store
        self.min_volume=min_volume;self.cache=OrderedDict();self.scan_cursor=0;self.leader_cursor=0
        self.funding={};self.funding_at=0;self.errors=0;self.unavailable={};self.model=None

    async def cached(self,market,path,params,ttl):
        key=(market,path,tuple(sorted(params.items())))
        old=self.cache.get(key)
        now=int(time.time()*1000)
        if now<self.unavailable.get(key,0):raise ValueError('No comparable public market available')
        same_candle=(path.endswith('/klines') and 'startTime' not in params)
        if old and 0<=now-old[0]<=ttl and (not same_candle or old[0]//BAR_MS==now//BAR_MS):
            self.cache.move_to_end(key);return old[1]
        try:value=await self.client.get(market,path,params)
        except aiohttp.ClientResponseError as exc:
            if exc.status in (400,404):self.unavailable[key]=now+6*3600000
            raise
        self.cache[key]=(int(time.time()*1000),value);self.cache.move_to_end(key)
        while len(self.cache)>160:self.cache.popitem(last=False)
        return value

    async def tick(self):
        now=int(time.time()*1000);self.client.remaining=self.client.maximum
        snapshot=await asyncio.to_thread(self.bus.get,'ranking') or {}
        if not 0<=now-int(snapshot.get('ts',0))<=30000:
            self.store.put('status',{'ts':now,'status':'waiting_data','reason':'全市场实时成交流过期或尚未就绪',
                'candidates':[],'schema':SCHEMA,'requests':self.client.requests})
            return
        fast=[r for r in snapshot.get('rows',[]) if fast_valid(r,now)]
        eligible=[r for r in fast if r['vector'][6]>=__import__('math').log1p(self.min_volume)]
        fast_map={r['symbol']:r for r in fast}
        # Mature labels have a separate bounded share of the request budget.
        evaluated=0
        for obs in self.store.due(now,limit=3):
            try:
                raw=await self.cached('futures','/fapi/v1/klines',{'symbol':obs['symbol'],'interval':'5m',
                    'startTime':obs['start_ms'],'endTime':obs['end_ms']-1,'limit':50},86400000)
                self.store.finish(obs,evaluate_path(raw,obs['start_ms'],now));evaluated+=1
            except (ValueError,aiohttp.ClientError,asyncio.TimeoutError,KeyError,TypeError):
                self.errors+=1
                # Never invent an outcome; missing observations remain pending.
                self.store.defer(obs,now+15*60000)
        try:
            if now-self.funding_at>=60000:
                raw=await self.client.get('futures','/fapi/v1/premiumIndex')
                self.funding={v['symbol']:v for v in raw};self.funding_at=int(time.time()*1000)
        except (ValueError,aiohttp.ClientError,asyncio.TimeoutError,TypeError,KeyError):self.errors+=1
        try:
            btc=closed_bars(await self.cached('futures','/fapi/v1/klines',
                {'symbol':'BTCUSDT','interval':'5m','limit':289},240000),int(time.time()*1000))
        except (ValueError,aiohttp.ClientError,asyncio.TimeoutError,KeyError,TypeError):
            self.store.put('status',{'ts':now,'status':'waiting_history','reason':'等待有效 BTC 长周期基准',
                'candidates':[],'schema':SCHEMA,'requests':self.client.requests,'errors':self.errors})
            return
        # Every pass includes fast leaders and rotating controls, avoiding only
        # retaining future winners. The watchlist never modifies the trade pool.
        leaders=sorted(eligible,key=lambda r:-(max(0,r['direction_score'])*max(1,r['vector'][5])))[:12]
        ordered=sorted(eligible,key=lambda r:r['symbol'])
        controls=[ordered[self.scan_cursor%len(ordered)]] if ordered else []
        self.scan_cursor=(self.scan_cursor+1)%max(1,len(ordered))
        selected=[leaders[(self.leader_cursor+i)%len(leaders)] for i in range(min(2,len(leaders)))] if leaders else []
        self.leader_cursor=(self.leader_cursor+2)%max(1,len(leaders))
        # Reserve a control before leader enrichment: 3 outcome requests + BTC
        # + funding + 1 control + 2*(candles, OI, spot) fit the 12-request cap.
        scheduled=list(dict.fromkeys([r['symbol'] for r in controls+selected]))
        wanted=list(scheduled)
        # Re-score recent cached symbols as well as the current leaders.
        known=[key[2] for key in self.cache if key[0]=='futures' and key[1]=='/fapi/v1/klines'
               and dict(key[2]).get('limit')==289]
        wanted=list(dict.fromkeys(wanted+[dict(k)['symbol'] for k in known if dict(k)['symbol'] in fast_map]))[:48]
        rows=[];waiting=[]
        for symbol in wanted:
            params={'symbol':symbol,'interval':'5m','limit':289}
            key=('futures','/fapi/v1/klines',tuple(sorted(params.items())))
            if symbol not in scheduled:
                old=self.cache.get(key)
                if not old or now-old[0]>240000 or old[0]//BAR_MS!=now//BAR_MS:
                    continue  # Cached rescoring must not spend the reserved budget.
            try:
                raw=await self.cached('futures','/fapi/v1/klines',params,240000)
                bars=closed_bars(raw,int(time.time()*1000))
            except (ValueError,aiohttp.ClientError,asyncio.TimeoutError,KeyError,TypeError):
                waiting.append(symbol);continue
            oi=funding=spot=liquidity=None
            if symbol in [r['symbol'] for r in selected]:
                try:oi=await self.cached('futures','/futures/data/openInterestHist',
                    {'symbol':symbol,'period':'5m','limit':13},240000)
                except (ValueError,aiohttp.ClientError,asyncio.TimeoutError,KeyError,TypeError):pass
                try:spot=closed_bars(await self.cached('spot','/api/v3/klines',
                    {'symbol':symbol,'interval':'5m','limit':14},240000),int(time.time()*1000),minimum=13)
                except (ValueError,aiohttp.ClientError,asyncio.TimeoutError,KeyError,TypeError):pass
            funding=self.funding.get(symbol)
            try:liquidity=await asyncio.to_thread(self.bus.get,'liquidity:'+symbol)
            except Exception:pass
            try:
                stamp=int(time.time()*1000)
                # Reload current flow after REST I/O; old flow never masquerades
                # as a current candidate or a training observation.
                current=await asyncio.to_thread(self.bus.get,'ranking') or {}
                current_row=next((r for r in current.get('rows',[]) if r['symbol']==symbol),None)
                if not current_row:raise ValueError('Symbol left active public universe')
                row=candidate(current_row,bars,btc,stamp,oi,funding,spot,liquidity,self.min_volume)
                self.store.observe([row],row['ts']);rows.append(row)
            except (ValueError,KeyError,TypeError,OverflowError):waiting.append(symbol)
        stamp=int(time.time()*1000)
        # Remove previous-pass candidates when their source has become stale.
        rows=[r for r in rows if 0<=stamp-r['ts']<=30000]
        rows.sort(key=lambda r:-r['score'])
        from .potential_train import model_status,predict
        model_state,self.model=model_status(self.store.directory)
        if self.model:
            for row in rows:row['prediction']=predict(self.model,row['vector'])
            rows.sort(key=lambda r:-r['prediction'])
        status={'ts':stamp,'status':'running','schema':SCHEMA,'universe_ready':len(fast),
            'liquid_universe':len(eligible),'enriched_candidates':len(rows),'candidates':rows[:20],
            'history_waiting':waiting[:30],'requests':self.client.requests,'requests_per_minute_limit':self.client.maximum,
            'errors':self.errors,'evaluated_this_pass':evaluated,'model':model_state,
            'mode':'validated_probability' if self.model else 'explainable_rule_score',
            'note':'观察雷达，不能保证暴涨；当前排名不下单、不调用付费 AI；仅覆盖币安已上市 USDT 永续',
            'label_sample':'hourly observations of enriched leaders and rotating controls; not a complete market census'}
        self.store.put('status',status);self.store.prune(stamp)


async def run():
    lab=LabConfig();store=PotentialStore(lab.directory);bus=RealtimeBus(lab.redis_url)
    if os.getenv('QUANT_RADAR_ENABLED','false').lower()!='true':
        store.put('status',{'ts':int(time.time()*1000),'status':'disabled','candidates':[]});return
    timeout=aiohttp.ClientTimeout(total=8)
    async with aiohttp.ClientSession(timeout=timeout,trust_env=False) as session:
        radar=Radar(PublicMarket(session),bus,store)
        while True:
            started=time.monotonic()
            try:await radar.tick()
            except Exception as exc:
                logging.warning('Radar observation failed: %s',type(exc).__name__)
                store.put('status',{'ts':int(time.time()*1000),'status':'error',
                    'reason':'行情或数据处理失败；等待下一次观察','candidates':[],'error_type':type(exc).__name__})
            if (os.getenv('QUANT_RADAR_AUTO_TRAIN','true').lower()=='true'
                    and int(time.time()*1000)-store.get('last_training_attempt',0)>=86400000):
                store.put('last_training_attempt',int(time.time()*1000))
                # Small daily JSON-tree training is separate from the market loop.
                process=await asyncio.create_subprocess_exec(sys.executable,'-m','quant.potential_train',
                    '--directory',str(store.directory),stdout=asyncio.subprocess.DEVNULL,stderr=asyncio.subprocess.DEVNULL)
                async def finish_training(child):
                    if await child.wait():store.put('training',{'status':'error','reason':'训练失败，规则雷达继续观察'})
                asyncio.create_task(finish_training(process))
            await asyncio.sleep(max(10,60-(time.monotonic()-started)))


if __name__=='__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run())
