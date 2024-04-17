import time
import json
import requests
import hmac
import hashlib
import aiohttp
import asyncio
import threading
from core.wrappers import try_exc_regular, try_exc_async


class BitKubClient:
    PUBLIC_WS_ENDPOINT = 'wss://api.bitkub.com/websocket-api/orderbook/'
    BASE_URL = 'https://api.bitkub.com'
    EXCHANGE_NAME = 'BITKUB'
    headers = {"Accept": "application/json",
               "Content-Type": "application/json",
               'Connection': 'keep-alive'}

    def __init__(self, multibot=None, keys=None, leverage=None, state='Bot',
                 markets_list=[], max_pos_part=20, finder=None, ob_len=5, market_finder=None):
        super().__init__()
        self.nonces = []
        self.market_finder = market_finder
        self.multibot = multibot
        self.state = state
        self.finder = finder
        self.markets_list = markets_list
        self.session = requests.session()
        self.session.headers.update(self.headers)
        self.instruments = {}
        self.markets = {}
        self.market_id_list = {}
        self.orderbook = {}
        self.get_http_orderbook('THB_USDT')
        self.get_active_markets_names()
        if self.state == 'Bot':
            self.api_key = keys['API_KEY']
            self.api_secret = keys['API_SECRET']
        self.ob_len = ob_len
        self.leverage = leverage
        self.max_pos_part = max_pos_part
        self.error_info = None
        self.LAST_ORDER_ID = 'default'
        self.taker_fee = 0.0025
        self.maker_fee = 0.0025
        self.async_tasks = []
        self.responses = {}
        self.orders = {}
        self.balance = {'total': 0,
                        'free': 0}

    @try_exc_regular
    def get_balance(self):
        return self.balance['total']

    @try_exc_regular
    def get_orderbook(self, market):
        return self.orderbook[market]

    @try_exc_regular
    def get_server_time(self):
        path = '/api/v3/servertime'
        resp = self.session.get(url=self.BASE_URL + path)
        diff = time.time() * 1000 - resp.json()
        print('Timestamp difference, ms', int(diff))

    @try_exc_regular
    def run_updater(self):
        wst_public = threading.Thread(target=self._run_ws_forever, args=[asyncio.new_event_loop()])
        wst_public.daemon = True
        wst_public.start()

    @try_exc_regular
    def _run_ws_forever(self, loop):
        while True:
            for market in self.markets_list[:-1]:
                loop.create_task(self._run_ws_loop(loop, market))
            loop.run_until_complete(self._run_ws_loop(loop, self.markets_list[-1]))

    @try_exc_async
    async def _run_ws_loop(self, loop: asyncio.new_event_loop, market: str):
        while True:
            async with aiohttp.ClientSession() as session:
                endpoint = self.PUBLIC_WS_ENDPOINT
                for id, market_name in self.market_id_list.items():
                    if market in market_name:
                        break
                async with session.ws_connect(endpoint + str(id)) as ws:
                    self._ws_public = ws
                    loop.create_task(self._ping(ws))
                    async for msg in ws:
                        await self.process_ws_msg(msg)
                await ws.close()
                time.sleep(5)

    @staticmethod
    @try_exc_regular
    def get_positions():
        return {}

    @try_exc_regular
    def merge_similar_orders(self, ob: list) -> list:
        last_order = None
        for order in ob:
            if last_order:
                if last_order[0] == order[0]:
                    order[1] += last_order[1]
                    del ob[ob.index(order) - 1]
                    self.merge_similar_orders(ob)
                    break
            last_order = order
        return ob

    @staticmethod
    @try_exc_regular
    def get_available_balance():
        return {'buy': 0,
                'sell': 0,
                'balance': 0}

    @try_exc_async
    async def process_ws_msg(self, msg: aiohttp.WSMessage):
        data = json.loads(msg.data)
        if market_id := data.get('pairing_id'):
            market = self.market_id_list[market_id]
            event = data['event']
            side = None
            top_bid = None
            top_ask = None
            if len(self.orderbook[market]['bids']):
                top_bid = self.orderbook[market]['bids'][0][0]
            if len(self.orderbook[market]['asks']):
                top_ask = self.orderbook[market]['asks'][0][0]
            ts = time.time()
            if event == 'bidschanged':
                if market != 'THB_USDT':
                    change = self.get_thb_rate()
                    new_bids = [[x[1] / change, x[2]] for x in data['data'][:self.ob_len]]
                else:
                    new_bids = [[x[1], x[2]] for x in data['data'][:self.ob_len]]
                new_bids = self.merge_similar_orders(new_bids)
                self.orderbook[market].update({'ts_ms': ts,
                                               'timestamp': ts})
                self.orderbook[market]['bids'] = new_bids
                if top_ask and top_ask > self.orderbook[market]['asks'][0][0]:
                    side = 'buy'
                elif top_bid and top_bid < self.orderbook[market]['asks'][0][0]:
                    side = 'sell'
                if self.finder and side:  # and ts_ms - ts_ob < self.top_ws_ping:
                    coin = market.split('-')[0]
                    await self.finder.count_one_coin(coin, self.EXCHANGE_NAME, side, 'ob')
            elif event == 'askschanged':
                if market != 'THB_USDT':
                    change = self.get_thb_rate()
                    new_asks = [[x[1] / change, x[2]] for x in data['data'][:self.ob_len]]
                else:
                    new_asks = [[x[1], x[2]] for x in data['data'][:self.ob_len]]
                self.orderbook[market].update({'ts_ms': ts,
                                               'timestamp': ts})
                new_asks = self.merge_similar_orders(new_asks)
                self.orderbook[market]['asks'] = new_asks
                if top_ask and top_ask > self.orderbook[market]['asks'][0][0]:
                    side = 'buy'
                elif top_bid and top_bid < self.orderbook[market]['asks'][0][0]:
                    side = 'sell'
                if self.finder and side:  # and ts_ms - ts_ob < self.top_ws_ping:
                    coin = market.split('-')[0]
                    await self.finder.count_one_coin(coin, self.EXCHANGE_NAME, side, 'ob')
            elif event == 'tradeschanged':
                timestamp = min([data['data'][0][0][0], data['data'][0][1][0]])
                if market != 'THB_USDT':
                    change = self.get_thb_rate()
                    new_asks = [[x[1] / change, x[2]] for x in data['data'][2][:self.ob_len]]
                    new_bids = [[x[1] / change, x[2]] for x in data['data'][1][:self.ob_len]]
                else:
                    new_asks = [[x[1], x[2]] for x in data['data'][2][:self.ob_len]]
                    new_bids = [[x[1], x[2]] for x in data['data'][1][:self.ob_len]]
                new_asks = self.merge_similar_orders(new_asks)
                new_bids = self.merge_similar_orders(new_bids)
                self.orderbook[market].update({'ts_ms': ts,
                                               'timestamp': timestamp})
                self.orderbook[market]['asks'] = new_asks
                self.orderbook[market]['bids'] = new_bids
                if top_ask and top_ask > self.orderbook[market]['asks'][0][0]:
                    side = 'buy'
                elif top_bid and top_bid < self.orderbook[market]['asks'][0][0]:
                    side = 'sell'
                if self.finder and side:  # and ts_ms - ts_ob < self.top_ws_ping:
                    coin = market.split('-')[0]
                    await self.finder.count_one_coin(coin, self.EXCHANGE_NAME, side, 'ob')

    @staticmethod
    @try_exc_async
    async def _ping(ws: aiohttp.ClientSession.ws_connect):
        while True:
            await asyncio.sleep(25)
            # Adjust the ping interval as needed
            await ws.ping()
        # print(f'PING SENT: {datetime.utcnow()}')

    @try_exc_regular
    def get_active_markets_names(self):
        path = '/api/market/symbols'
        response = self.session.get(url=self.BASE_URL + path)
        resp = response.json()
        if not resp['error']:
            for market in resp['result']:
                self.market_id_list.update({market['id']: market['symbol']})
                coin = market['symbol'].split('_')[1]
                self.markets.update({coin: market['symbol']})
                self.get_http_orderbook(market['symbol'])
                time.sleep(0.1)

    @try_exc_regular
    def get_markets(self):
        return self.markets

    @try_exc_regular
    def get_thb_rate(self):
        ob = self.orderbook['THB_USDT']
        change_rate = (ob['asks'][0][0] + ob['bids'][0][0]) / 2
        return change_rate

    @try_exc_regular
    def get_http_orderbook(self, market: str, limit: int = 10):
        path = '/api/market/depth'
        params = {'sym': market,
                  'lmt': limit}
        post_string = '?' + "&".join([f"{key}={params[key]}" for key in sorted(params)])
        resp = self.session.get(url=self.BASE_URL + path + post_string)
        response = resp.json()
        ts = time.time()
        if error_code := response.get('error'):
            print(market, response)
            if error_code == 11:
                coin = market.split('_')[1]
                self.markets.pop(coin)
            else:
                print(f"RATE LIMIT REACHED")
                time.sleep(30)
                self.get_http_orderbook(market)
        else:
            if market != 'THB_USDT':
                change_rate = self.get_thb_rate()
                for ask in response['asks']:
                    ask[0] = ask[0] / change_rate
                for bid in response['bids']:
                    bid[0] = bid[0] / change_rate
            response.update({'ts_ms': ts,
                             'timestamp': ts})
            self.orderbook.update({market: response})

    @try_exc_regular
    def get_signature(self, timestamp: int, req_type: str, path: str, body: dict = None):
        payload = list()
        payload.append(str(timestamp))
        payload.append(req_type)
        payload.append(path)
        if body:
            payload.append(json.dumps(body))
        payload_string = ''.join(payload)
        return hmac.new(self.api_secret.encode('utf-8'), payload_string.encode('utf-8'), hashlib.sha256).hexdigest()

    @try_exc_regular
    def get_auth_for_request(self, params: dict, path: str, method: str):
        ts = int(round(time.time() * 1000))
        signature = self.get_signature(ts, method, path, params)
        self.session.headers.update({
            'Accept': 'application/json',
            'Content-type': 'application/json',
            'X-BTK-APIKEY': self.api_key,
            'X-BTK-TIMESTAMP': ts,
            'X-BTK-SIGN': signature,
            'Connection': 'keep-alive'
        })


if __name__ == '__main__':
    import configparser

    config = configparser.ConfigParser()
    config.read('config.ini', "utf-8")
    client = BitKubClient(keys=config['BITKUB'],
                          leverage=float(config['SETTINGS']['LEVERAGE']),
                          max_pos_part=int(config['SETTINGS']['PERCENT_PER_MARKET']),
                          markets_list=['TWT', 'USDT'])

    client.run_updater()

    while True:
        time.sleep(1)
        # client.get_server_time()
        # client.get_markets_names()
        # asyncio.run(test_order())
