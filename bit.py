import time
import aiohttp
import json
import requests
from datetime import datetime
import asyncio
import threading
from core.wrappers import try_exc_regular, try_exc_async


class BitClient:
    PUBLIC_WS_ENDPOINT = 'wss://ws.bit.com'
    EXCHANGE_NAME = 'BIT'

    def __init__(self, keys=None, leverage=None, state='Bot', markets_list=[], max_pos_part=20, finder=None, ob_len=4):
        self.finder = finder
        self.ob_len = ob_len
        self.headers = {'Content-Type': 'application/json'}
        self.markets = self.get_markets()
        self._loop_public = asyncio.new_event_loop()
        self._connected = asyncio.Event()
        self.wst_public = threading.Thread(target=self._run_ws_forever, args=[self._loop_public])
        self._wst_orderbooks = threading.Thread(target=self._process_ws_line)
        self.message_queue = asyncio.Queue(loop=self._loop_public)
        self.orderbook = {}
        self.taker_fee = float(keys['TAKER_FEE'])

    @try_exc_regular
    def _process_ws_line(self):
        # self._loop.create_task(self.process_messages())
        asyncio.run_coroutine_threadsafe(self.process_messages(), self._loop_public)

    @try_exc_regular
    def get_markets(self):
        way = "https://api.bit.com/linear/v1/instruments"
        resp = requests.get(url=way, headers=self.headers).json()
        markets = {}
        for market in resp['data']:
            if market['active'] and market['status'] == 'online' and 'PERPETUAL' in market['instrument_id']:
                markets.update({market['contract_size_currency']: market['instrument_id']})
        return markets

    @try_exc_regular
    def get_all_tops(self):
        tops = {}
        for symbol, orderbook in self.orderbook.items():
            coin = symbol.upper().split('-')[0]
            if len(orderbook['bids']) and len(orderbook['asks']):
                tops.update({self.EXCHANGE_NAME + '__' + coin: {
                    'top_bid': orderbook['bids'][0][0], 'top_ask': orderbook['asks'][0][0],
                    'bid_vol': orderbook['bids'][0][1], 'ask_vol': orderbook['asks'][0][1],
                    'ts_exchange': orderbook['timestamp']}})
        return tops

    @try_exc_regular
    def _run_ws_forever(self, loop):
        while True:
            loop.run_until_complete(self._run_ws_loop())

    @try_exc_async
    async def _run_ws_loop(self):
        async with aiohttp.ClientSession() as s:
            async with s.ws_connect(self.PUBLIC_WS_ENDPOINT) as ws:
                self._connected.set()
                self._ws_public = ws
                asyncio.run_coroutine_threadsafe(self.subscribe_orderbooks(), self._loop_public)
                async for msg in ws:
                    await self.message_queue.put(msg)

    @try_exc_async
    async def process_messages(self):
        while True:
            msg = await self.message_queue.get()
            data = json.loads(msg.data)
            if 'order_book' in data['channel']:
                self.update_orderbook(data)

    @try_exc_regular
    def get_orderbook(self, symbol):
        ob = self.orderbook[symbol]
        if ob['asks'][0][0] <= ob['bids'][0][0]:
            print(f"ALARM! ORDERBOOK ERROR {self.EXCHANGE_NAME}: {ob}")
            return {}
        return ob

    @try_exc_async
    async def subscribe_orderbooks(self):
        instruments = list(self.markets.values())
        method = {"type": "subscribe",
                  "instruments": instruments,
                  "channels": ["order_book.1.10"],
                  "interval": "raw"}
        await self._connected.wait()
        await self._ws_public.send_json(method)

    @try_exc_regular
    def update_orderbook(self, data):
        if not data.get('data'):
            return
        market = data['data']['instrument_id']
        if data['data']['asks'] and data['data']['bids']:
            if not self.orderbook.get(market):
                self.orderbook.update({market: {'asks': [], 'bids': []}})
            snap = self.orderbook[market].copy()
            self.orderbook[market].update({'asks': [[float(x[0]), float(x[1])] for x in data['data']['asks']],
                                           'bids': [[float(x[0]), float(x[1])] for x in data['data']['bids']],
                                           'timestamp': data['timestamp'],
                                           'ts_ms': time.time()})
            if self.finder and snap['asks']:
                if_new_top_ask = snap['asks'][0][0] > self.orderbook[market]['asks'][0][0]
                if_new_top_bid = snap['bids'][0][0] < self.orderbook[market]['bids'][0][0]
                if if_new_top_ask or if_new_top_bid:
                    coin = market.split('USDT')[0]
                    self.finder.coins_to_check.add(coin)
                    self.finder.update = True

    @try_exc_regular
    def run_updater(self):
        self.wst_public.daemon = True
        self.wst_public.start()
        self._wst_orderbooks.daemon = True
        self._wst_orderbooks.start()


if __name__ == '__main__':
    client = BitClient()
    print(len(client.get_markets()))
    client.run_updater()
    while True:
        time.sleep(5)
        print(client.get_all_tops())

