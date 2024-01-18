import time
import aiohttp
import json
import requests
from datetime import datetime
import asyncio
import threading
from core.wrappers import try_exc_regular, try_exc_async
import hmac
import hashlib
import base64


class GlobeClient:
    PUBLIC_WS_ENDPOINT = 'wss://globe.exchange/api/v1/ws'
    BASE_URL = 'https://globe.exchange'
    EXCHANGE_NAME = 'GLOBE'

    def __init__(self, multibot=None, keys=None, leverage=None, state='Bot', markets_list=[], max_pos_part=20, finder=None, ob_len=4):
        self.multibot = multibot
        self.state = state
        self.finder = finder
        self.ob_len = ob_len
        if self.state == 'Bot':
            self.api_key = keys['API_KEY']
            self.api_secret = keys['API_SECRET']
            self.passphrase = keys['PASSPHRASE']
        self.headers = {'Content-Type': 'application/json'}
        self.markets = self.get_markets()
        self._loop_public = asyncio.new_event_loop()
        self._connected = asyncio.Event()
        self.wst_public = threading.Thread(target=self._run_ws_forever, args=[self._loop_public])
        # self._wst_orderbooks = threading.Thread(target=self._process_ws_line)
        self.message_queue = asyncio.Queue(loop=self._loop_public)
        self.orderbook = {}
        self.positions = {}
        self.taker_fee = float(keys['TAKER_FEE'])

    @try_exc_regular
    def _process_ws_line(self):
        # self._loop.create_task(self.process_messages())
        asyncio.run_coroutine_threadsafe(self.process_messages(), self._loop_public)

    @try_exc_regular
    def get_markets(self):
        path = "/api/v1/ticker/contracts"
        resp = requests.get(url=self.BASE_URL + path, headers=self.headers).json()
        markets = {}
        for market in resp:
            if market['product_type'] == 'perpetual' and market['product_status'] == 'Active':
                markets.update({market['base_symbol']: market['instrument']})
        return markets

    def _hash(self, sign_txt):
        sign_txt = bytes(sign_txt, "utf-8")
        secret = base64.b64decode(bytes(self.api_secret, "utf-8"))
        signature = base64.b64encode(hmac.new(secret, sign_txt, digestmod=hashlib.sha256).digest())
        return signature

    @try_exc_regular
    def get_private_headers(self, url):
        """
        Generate new authentication headers
        """
        headers = {
            "X-Access-Key": self.api_key,
            "X-Access-Signature": "",
            "X-Access-Nonce": str(int(datetime.utcnow().timestamp() * 1000)),
            "X-Access-Passphrase": self.passphrase,
        }
        sign_txt = headers["X-Access-Nonce"] + url
        headers["X-Access-Signature"] = str(self._hash(sign_txt), "utf-8")
        return headers

    @try_exc_regular
    def cancel_all_orders(self):
        path = '/api/v4/order/cancel/all'
        params, headers = self.get_auth_for_request({}, path)
        path += self._create_uri(params)
        res = requests.post(url=self.BASE_URL + path, headers=headers, json=params)
        return res.json()

    @try_exc_regular
    def get_position(self):
        path = "/api/v1/positions"
        headers = self.get_private_headers(path)
        resp = requests.get(url=self.BASE_URL + path, headers=headers).json()
        for market, position in resp.items():
            ob = self.get_orderbook(market)
            mark_price = (ob['asks'][0][0] + ob['bids'][0][0]) / 2
            self.positions.update({'timestamp': int(datetime.utcnow().timestamp()),
                                   'entry_price': position['avg_price'],
                                   'amount': position['quantity'],
                                   'amount_usd': mark_price * position['quantity']})

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
        i = 0
        async with aiohttp.ClientSession() as s:
            async with s.ws_connect(self.PUBLIC_WS_ENDPOINT) as ws:
                self._connected.set()
                self._ws_public = ws
                for symbol in self.markets.values():
                    asyncio.run_coroutine_threadsafe(self.subscribe_orderbooks(symbol), self._loop_public)

                async for msg in ws:
                    i += 1
                    print(f'{i=}')
                    # msg = await self.message_queue.get()
                    data = json.loads(msg.data)
                    # self.update_orderbook(json.loads(msg.data))
                    ts_ob = data['data']['timestamp'] / 1000
                    print(data)
                    ts_ms = time.time()
                    print(ts_ms - ts_ob)
                    # await self.message_queue.put(msg)

    @try_exc_async
    async def process_messages(self):
        while True:
            msg = await self.message_queue.get()
            data = json.loads(msg.data)
            # self.update_orderbook(json.loads(msg.data))
            ts_ob = data['data']['timestamp']/1000
            # print(data)
            ts_ms = time.time()
            print(ts_ms-ts_ob)


    @try_exc_regular
    def get_orderbook(self, symbol):
        if not self.orderbook.get(symbol):
            return {}
        ob = self.orderbook[symbol]
        if ob['asks'][0][0] <= ob['bids'][0][0]:
            print(f"ALARM! ORDERBOOK ERROR {self.EXCHANGE_NAME}: {ob}")
            return {}
        return ob

    @try_exc_async
    async def subscribe_orderbooks(self, symbol):
        method = {"command": "subscribe",
                  "channel": "depth",
                  "instrument": symbol}
        await self._connected.wait()
        await self._ws_public.send_json(method)

    @try_exc_regular
    async def update_orderbook(self, data):
        flag = False

        if not data.get('data'):
            return
        market = data['subscription']['instrument']
        if not self.orderbook.get(market):
            self.orderbook.update({market: {'asks': [], 'bids': []}})
        new_ob = self.orderbook[market].copy()
        ts_ms = time.time()
        new_ob['ts_ms'] = ts_ms
        ts_ob = data['data']['timestamp']
        if isinstance(ts_ob, int):
            ts_ob = ts_ob / 1000
        new_ob['timestamp'] = ts_ob
        self.orderbook[market].update({'asks': [[x['price'], x['volume']] for x in data['data']['asks']],
                                       'bids': [[x['price'], x['volume']] for x in data['data']['bids']],
                                       'timestamp': data['data']['timestamp'],
                                       'ts_ms': time.time()})
        if self.finder and new_ob['asks']:
            if_new_top_ask = new_ob['asks'][0][0] > self.orderbook[market]['asks'][0][0]
            flag = True
            side = 'buy'
        if self.finder and new_ob['bids']:
            if_new_top_bid = new_ob['bids'][0][0] < self.orderbook[market]['bids'][0][0]
            flag = True
            side = 'sell'
        # if flag and ts_ms - ts_ob < 1:
        #     coin = market.split('USDT')[0]
        #     if self.state == 'Bot':
        #         await self.finder.count_one_coin(coin, self.EXCHANGE_NAME, side, self.multibot.run_arbitrage)
        #     else:
        #         await self.finder.count_one_coin(coin, self.EXCHANGE_NAME, side)

    @try_exc_regular
    def __generate_signature(self, data):
        return hmac.new(self.api_secret.encode(), data.encode(), hashlib.sha256).hexdigest()

    @try_exc_regular
    def run_updater(self):
        self.wst_public.daemon = True
        self.wst_public.start()
        # self._wst_orderbooks.daemon = True
        # self._wst_orderbooks.start()


if __name__ == '__main__':
    import configparser

    config = configparser.ConfigParser()
    config.read('config_parser.ini', "utf-8")

    keys = config['GLOBE']
    client = GlobeClient(keys=keys, state='Parser')
    # print(client.get_markets())
    client.run_updater()
    # while True:
    #     time.sleep(1)
    #     print(client.get_all_tops())

