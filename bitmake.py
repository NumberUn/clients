import time
import aiohttp
import json
import requests
from datetime import datetime
import asyncio
import threading
import zlib
from core.wrappers import try_exc_regular, try_exc_async


class BitmakeClient:
    PUBLIC_WS_ENDPOINT = 'wss://ws.bitmake.com/t/v1/ws'
    EXCHANGE_NAME = 'BITMAKE'

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
        self.getting_ob = asyncio.Event()
        self.now_getting = ''
        self.orderbook = {}
        self.taker_fee = float(keys['TAKER_FEE'])

    @try_exc_regular
    def _process_ws_line(self):
        # self._loop.create_task(self.process_messages())
        asyncio.run_coroutine_threadsafe(self.process_messages(), self._loop_public)

    @try_exc_regular
    def get_markets(self):
        way = "https://api.bitmake.com/u/v1/base/symbols"
        resp = requests.get(url=way, headers=self.headers).json()
        markets = {}
        for market in resp:
            if market.get('contractConfig') and market['contractConfig']['isPerpetual']:
                markets.update({market['baseToken']: market['symbol']})
        return markets

    @try_exc_regular
    def get_all_tops(self):
        tops = {}
        for coin, symbol in self.markets.items():
            orderbook = self.get_orderbook(symbol)
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
                for symbol in self.markets.values():
                    asyncio.run_coroutine_threadsafe(self.subscribe_orderbooks(symbol), self._loop_public)
                async for msg in ws:
                    await self.message_queue.put(msg)

    @try_exc_async
    async def process_messages(self):
        while True:
            msg = await self.message_queue.get()
            data = json.loads(self.decode_gzip_data(msg.data))
            if data.get('d') and data['f']:
                if data['d'][0]['s'] == self.now_getting:
                    while self.getting_ob.is_set():
                        time.sleep(0.00001)
                self.update_orderbook_snapshot(data)
            if data.get('d') and not data['f']:
                if data['d'][0]['s'] == self.now_getting:
                    while self.getting_ob.is_set():
                        time.sleep(0.00001)
                self.update_orderbook(data)

    @staticmethod
    @try_exc_regular
    def decode_gzip_data(data):
        # Decompress the Gzip data
        decompressed_data = zlib.decompress(data, 16 + zlib.MAX_WBITS)

        # Convert decompressed data from binary to a UTF-8 encoded string
        string_data = decompressed_data.decode('utf-8')
        return string_data

    @try_exc_async
    async def subscribe_orderbooks(self, symbol):
        method = {"tp": "diffMergedDepth",
                  "e": "sub",
                  "ps": {"symbol": symbol,
                         "depthLevel": 2}}
        await self._connected.wait()
        await self._ws_public.send_json(method)

    @try_exc_regular
    def update_orderbook(self, data):
        flag = False
        ob = data['d'][0]
        symbol = ob['s']
        new_ob = self.orderbook[symbol].copy()
        ts_ms = time.time()
        new_ob['ts_ms'] = ts_ms
        ts_ob = ob['t']
        if isinstance(ts_ob, int):
            ts_ob = ts_ob / 1000
        new_ob['timestamp'] = ts_ob
        for new_bid in ob['b']:
            if float(new_bid[0]) >= new_ob['top_bid'][0]:
                new_ob['top_bid'] = [float(new_bid[0]), float(new_bid[1])]
                new_ob['top_bid_timestamp'] = ob['t']
                flag = True
            if new_ob['bids'].get(new_bid[0]) and new_bid[1] == '0':
                del new_ob['bids'][new_bid[0]]
                if float(new_bid[0]) == new_ob['top_bid'][0] and len(new_ob['bids']):
                    top = sorted(new_ob['bids'])[-1]
                    new_ob['top_bid'] = [float(top), float(new_ob['bids'][top])]
                    new_ob['top_bid_timestamp'] = ob['t']
            elif new_bid[1] != '0':
                new_ob['bids'][new_bid[0]] = new_bid[1]
        for new_ask in ob['a']:
            if float(new_ask[0]) <= new_ob['top_ask'][0]:
                new_ob['top_ask'] = [float(new_ask[0]), float(new_ask[1])]
                new_ob['top_ask_timestamp'] = ob['t']
                flag = True
            if new_ob['asks'].get(new_ask[0]) and new_ask[1] == '0':
                del new_ob['asks'][new_ask[0]]
                if float(new_ask[0]) == new_ob['top_ask'][0] and len(new_ob['asks']):
                    top = sorted(new_ob['asks'])[0]
                    new_ob['top_ask'] = [float(top), float(new_ob['asks'][top])]
                    new_ob['top_ask_timestamp'] = ob['t']
            elif new_ask[1] != '0':
                new_ob['asks'][new_ask[0]] = new_ask[1]
        self.orderbook[symbol] = new_ob
        if flag and ts_ms - ts_ob < 0.1:
            coin = symbol.split('-')[0]
            if self.finder:
                self.finder.coins_to_check.add(coin)
                self.finder.update = True

    @try_exc_regular
    def update_orderbook_snapshot(self, data):
        ob = data['d'][0]
        symbol = ob['s']
        self.orderbook[symbol] = {'asks': {x[0]: x[1] for x in ob['a']},
                                  'bids': {x[0]: x[1] for x in ob['b']},
                                  'top_ask': [float(min(ob['a'], key=lambda x: float(x[0]))[0]),0],
                                  'top_bid': [float(max(ob['b'], key=lambda x: float(x[0]))[0]),0],
                                  'timestamp': ob['t'],
                                  'ts_ms': time.time()}

    @try_exc_regular
    def get_orderbook(self, symbol) -> dict:
        if not self.orderbook.get(symbol):
            return {}
        self.getting_ob.set()
        self.now_getting = symbol
        snap = self.orderbook[symbol]
        ob = {'timestamp': snap['timestamp'],
              'asks': [[float(x), float(snap['asks'][x])] for x in sorted(snap['asks']) if snap['asks'].get(x)],
              'bids': [[float(x), float(snap['bids'][x])] for x in sorted(snap['bids']) if snap['bids'].get(x)][::-1],
              'ts_ms': snap['ts_ms']}
        if ob['asks'][0][0] <= ob['bids'][0][0]:
            print(f"ALARM! ORDERBOOK ERROR {self.EXCHANGE_NAME}: {snap}")
            return {}
        self.now_getting = ''
        self.getting_ob.clear()
        return ob

    @try_exc_regular
    def run_updater(self):
        self.wst_public.daemon = True
        self.wst_public.start()
        self._wst_orderbooks.daemon = True
        self._wst_orderbooks.start()


if __name__ == '__main__':
    client = BitmakeClient()
    # print(client.get_markets())
    client.run_updater()
    while True:
        time.sleep(5)
        print(client.get_all_tops())
