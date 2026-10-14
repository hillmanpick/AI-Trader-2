"""Official USDⓈ-M REST API; writes are never blindly retried."""
import hashlib
import hmac
import time
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from urllib.parse import urlencode

import requests


class ExchangeError(RuntimeError):
    def __init__(self, status, code=None):
        self.status, self.code = status, code
        message = 'Binance region restriction (HTTP 451)' if status == 451 else f'Binance request failed: HTTP {status}, code {code}'
        super().__init__(message)


def rounded(value, step, up=False):
    number, increment = Decimal(str(value)), Decimal(str(step))
    return format((number / increment).to_integral_value(rounding=ROUND_UP if up else ROUND_DOWN) * increment, 'f')


class Binance:
    def __init__(self, cfg):
        self.cfg = cfg
        self.session = requests.Session()
        self.session.trust_env = False
        self.offset = 0
        self.rules = {}
        self.last_sync = 0

    def request(self, method, path, params=None, signed=False):
        params = dict(params or {})
        headers = {}
        if signed:
            params.update(timestamp=int(time.time()*1000)+self.offset, recvWindow=5000)
            headers['X-MBX-APIKEY'] = self.cfg.api_key
        query = urlencode(params)
        if signed:
            query += '&signature=' + hmac.new(self.cfg.api_secret.encode(),query.encode(),hashlib.sha256).hexdigest()
        try:
            response = self.session.request(method,self.cfg.base_url+path,params=query,headers=headers,timeout=(5,12))
        except requests.RequestException:
            # Do not log exception URLs: signed query strings contain signatures.
            raise RuntimeError('Binance network error; order result may be unknown') from None
        try:
            data = response.json()
        except ValueError:
            raise ExchangeError(response.status_code) from None
        if response.status_code >= 400:
            raise ExchangeError(response.status_code,data.get('code') if isinstance(data,dict) else None)
        return data

    def sync_time(self):
        before = int(time.time()*1000)
        server = self.request('GET','/fapi/v1/time')['serverTime']
        self.offset = int(server) - (before+int(time.time()*1000))//2
        self.last_sync = time.time()

    def load_rules(self):
        data = self.request('GET','/fapi/v1/exchangeInfo')
        self.rules = {s['symbol']:s for s in data['symbols']}
        for symbol in self.cfg.symbols:
            r = self.rules.get(symbol, {})
            if r.get('status') != 'TRADING' or r.get('contractType') != 'PERPETUAL' or r.get('marginAsset') != 'USDT':
                raise ValueError(f'{symbol} is not an active USDT perpetual')

    def closed_candles(self, symbol):
        bars = self.request('GET','/fapi/v1/klines',{'symbol':symbol,'interval':self.cfg.interval,'limit':499})
        now = int(time.time()*1000)+self.offset
        return [r for r in bars if int(r[6]) < now]

    def mark(self,symbol):
        return float(self.request('GET','/fapi/v1/premiumIndex',{'symbol':symbol})['markPrice'])

    def quantity(self,symbol,notional,price):
        filters = {f['filterType']:f for f in self.rules[symbol]['filters']}
        lot = filters.get('MARKET_LOT_SIZE', filters['LOT_SIZE'])
        if Decimal(lot['stepSize']) == 0:
            lot = filters['LOT_SIZE']
        qty = rounded(notional/price,lot['stepSize'])
        minimum = float(filters.get('MIN_NOTIONAL',{}).get('notional',0))
        if float(qty) < float(lot['minQty']) or float(qty) > float(lot['maxQty']) or float(qty)*price < minimum:
            raise ValueError(f'{symbol}: configured order notional does not meet exchange filters; it will not be increased automatically')
        return qty

    def positions(self):
        return self.request('GET','/fapi/v3/positionRisk',signed=True)

    def account(self):
        return self.request('GET','/fapi/v3/account',signed=True)

    def prepare_account(self):
        mode = self.request('GET','/fapi/v1/positionSide/dual',signed=True)
        if mode['dualSidePosition']:
            raise ValueError('One-way position mode is required; switch manually after checking account positions')
        for symbol in self.cfg.symbols:
            try:
                self.request('POST','/fapi/v1/marginType',{'symbol':symbol,'marginType':'ISOLATED'},signed=True)
            except ExchangeError as e:
                if e.code != -4046:
                    raise
            self.request('POST','/fapi/v1/leverage',{'symbol':symbol,'leverage':self.cfg.leverage},signed=True)

    def order(self,order_id,params):
        return self.request('POST','/fapi/v1/order', params | {'newClientOrderId':order_id,'newOrderRespType':'RESULT','type':'MARKET'},signed=True)

    def query_order(self,order_id,symbol):
        return self.request('GET','/fapi/v1/order',{'symbol':symbol,'origClientOrderId':order_id},signed=True)

    def cancel_order(self,order_id,symbol):
        return self.request('DELETE','/fapi/v1/order',{'symbol':symbol,'origClientOrderId':order_id},signed=True)

    def algos(self,symbol):
        data = self.request('GET','/fapi/v1/openAlgoOrders',{'symbol':symbol,'algoType':'CONDITIONAL'},signed=True)
        return data if isinstance(data,list) else data.get('orders',[])

    def protect(self,symbol,amount,entry):
        tick = next(f['tickSize'] for f in self.rules[symbol]['filters'] if f['filterType']=='PRICE_FILTER')
        side = 'SELL' if amount > 0 else 'BUY'
        existing = self.algos(symbol)
        owned = [a for a in existing if str(a.get('clientAlgoId','')).startswith('qt_')]
        for kind,pct in [('STOP_MARKET',self.cfg.stop_pct),('TAKE_PROFIT_MARKET',self.cfg.take_pct)]:
            direction = (-1 if kind=='STOP_MARKET' else 1) * (1 if amount>0 else -1)
            trigger = rounded(entry*(1+direction*pct),tick,up=side=='BUY')
            candidates = [a for a in owned if a.get('orderType',a.get('type'))==kind]
            matches = [a for a in candidates if a.get('side')==side and str(a.get('closePosition')).lower()=='true' and a.get('workingType')=='MARK_PRICE' and Decimal(str(a.get('triggerPrice',0)))==Decimal(trigger)]
            if matches:
                continue
            for stale in candidates:
                self.request('DELETE','/fapi/v1/algoOrder',{'algoId':stale['algoId']},signed=True)
            client_id = 'qt_' + hashlib.sha256(f'{symbol}:{amount}:{entry}:{kind}'.encode()).hexdigest()[:28]
            self.request('POST','/fapi/v1/algoOrder',{'algoType':'CONDITIONAL','symbol':symbol,'side':side,'type':kind,'triggerPrice':trigger,'workingType':'MARK_PRICE','closePosition':'true','clientAlgoId':client_id},signed=True)

    def clear_protection(self,symbol):
        for a in self.algos(symbol):
            if str(a.get('clientAlgoId','')).startswith('qt_'):
                self.request('DELETE','/fapi/v1/algoOrder',{'algoId':a['algoId']},signed=True)
