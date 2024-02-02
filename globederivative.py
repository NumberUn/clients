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
from clients.core.enums import ResponseStatus, OrderStatus
import uuid


class GlobeClient:
    PUBLIC_WS_ENDPOINT = 'wss://globe.exchange/api/v1/ws'
    BASE_URL = 'https://globe.exchange'
    EXCHANGE_NAME = 'GLOBE'
    LAST_ORDER_ID = 'default'

    def __init__(self, multibot=None, keys=None, leverage=None, state='Bot', markets_list=[], max_pos_part=20,
                 finder=None, ob_len=4):
        self.multibot = multibot
        self.markets_list = markets_list
        self.state = state
        self.finder = finder
        self.ob_len = ob_len
        if self.state == 'Bot':
            self.api_key = keys['API_KEY']
            self.api_secret = keys['API_SECRET']
            self.passphrase = keys['PASSPHRASE']
        self.headers = {'Content-Type': 'application/json',
                        'Connection': 'keep-alive'}
        self.markets = self.get_markets()
        self._loop_public = asyncio.new_event_loop()
        self.orders_loop = asyncio.new_event_loop()
        self._connected = asyncio.Event()
        self.wst_public = threading.Thread(target=self._run_ws_forever, args=[self._loop_public])
        self.wst_orders = threading.Thread(target=self.run_orders_loop, args=[self.orders_loop])
        # self._wst_orderbooks = threading.Thread(target=self._process_ws_line)
        self.orderbook = {}
        self.positions = {}
        self.taker_fee = 0.0005
        self.open_orders = {}
        self.positions = {}
        self.balance = {}
        self.orders = {}
        self.fills = {}
        self.async_tasks = []
        self.responses = {}
        self.deleted_orders = []
        self.last_keep_alive = 0
        self.last_websocket_ping = 0
        self.original_sizes = {}
        self.clients_ids = {}
        self.ws_orders = None
        self._ws_public = None

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
        headers = {
            "X-Access-Key": self.api_key,
            "X-Access-Signature": "",
            "X-Access-Nonce": str(int(datetime.utcnow().timestamp() * 1000)),
            "X-Access-Passphrase": self.passphrase,
            "Content-Type": "application/json",
            'Connection': 'keep-alive'
        }
        sign_txt = headers["X-Access-Nonce"] + url
        headers["X-Access-Signature"] = str(self._hash(sign_txt), "utf-8")
        return headers

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
            loop.run_until_complete(self._run_ws_loop(loop))

    def run_orders_loop(self, loop):
        headers = self.get_private_headers("GET/api/v1/ws")
        async with aiohttp.ClientSession() as s:
            async with s.ws_connect(url=self.PUBLIC_WS_ENDPOINT, headers=headers) as ws:
                self.ws_orders = ws
                while True:
                    for task in self.async_tasks:
                        if task[0] == 'create_order':
                            price = task[1]['price']
                            size = task[1]['size']
                            side = task[1]['side']
                            market = task[1]['market']
                            client_id = task[1].get('client_id')
                            loop.create_task(self.create_fast_order(price, size, side, market, client_id))
                        elif task[0] == 'cancel_order':
                            loop.create_task(self.cancel_order(task[1]['market'], task[1]['order_id']))
                        elif task[0] == 'amend_order':
                            price = task[1]['price']
                            size = task[1]['size']
                            side = task[1]['side']
                            order_id = task[1]['order_id']
                            market = task[1]['market']
                            client_id = task[1].get('client_id')
                            loop.create_task(self.create_fast_order(price, size, side, market, client_id, order_id))
                        self.async_tasks.remove(task)
                    loop.create_task(self.ping_websocket(ws))

    @try_exc_async
    async def cancel_order(self, market, order_id):
        pass

    @try_exc_async
    async def ping_websocket(self, ws):
        ts = time.time()
        if ts - self.last_websocket_ping > 10:
            await ws.ping()
            self.last_websocket_ping = ts

    @try_exc_async
    async def _run_ws_loop(self, loop):
        headers = self.get_private_headers("GET/api/v1/ws")
        async with aiohttp.ClientSession() as s:
            async with s.ws_connect(url=self.PUBLIC_WS_ENDPOINT, headers=headers) as ws:
                self._connected.set()
                self._ws_public = ws
                await loop.create_task(self.subscribe_positions())
                await loop.create_task(self.subscribe_balances())
                for coin, symbol in self.markets.items():
                    if coin in self.markets_list:
                        await asyncio.sleep(0.08)
                        await loop.create_task(self.subscribe_market_events(symbol))
                        await loop.create_task(self.subscribe_open_orders(symbol))
                        await loop.create_task(self.subscribe_orderbooks(symbol))
                async for msg in ws:
                    # try:
                    data = json.loads(msg.data)
                    channel = data.get('subscription', {}).get('channel')
                    if channel == 'depth':
                        loop.create_task(self.update_orderbook(data))
                    elif channel == 'my-market-events':
                        loop.create_task(self.update_events(data, channel))
                    elif channel == 'my-positions':
                        loop.create_task(self.update_positions(data))
                    elif channel == "my-account-overview":
                        loop.create_task(self.update_balances(data))
                    elif channel == "my-orders":
                        loop.create_task(self.update_orders(data, channel))
                    else:
                        print(data)

    @try_exc_async
    async def update_events(self, data, channel):
        if data['data']['event'] == 'traded':
            print(f'FILLS {self.EXCHANGE_NAME} WS MESSAGE {datetime.utcnow()}:', data)
            await self.update_fills(data)
        else:
            await self.update_orders(data, channel)

    @try_exc_regular
    def get_closed_order_update_status(self, order_id):
        if order := self.open_orders.get(order_id):
            if order.get('quantity') and order['quantity'] == order['filled_quantity']:
                return OrderStatus.FULLY_EXECUTED
            elif order.get('filled_quantity') == 0:
                return OrderStatus.NOT_EXECUTED
            elif order.get('filled_quantity'):
                return OrderStatus.PARTIALLY_EXECUTED

    @try_exc_regular
    def update_orders_list(self, order_id, timestamp):
        if self.orders.get(order_id):
            return
        status = self.get_closed_order_update_status(order_id)
        fill = self.fills.get(order_id)
        if fill:
            fill.update({'status': status,
                         'datetime_update': datetime.utcnow(),
                         'ts_update': timestamp if timestamp else fill['timestamp']})
            self.orders.update({order_id: fill})
            self.fills.pop(order_id)

    @try_exc_regular
    def update_non_filled_response(self, data, order_id):
        client_id = self.clients_ids[order_id][0]
        time_start = self.clients_ids[order_id][1]
        # example = {'type': 'order', 'filled_quantity': 0.0, 'order_id': 'c6fd6229-3d30-4843-a66c',
        #            'order_type': 'limit', 'price': 0.434, 'quantity': 5.0, 'side': 'buy',
        #            'timestamp': 1706701632780}
        order_res = {'exchange_name': self.EXCHANGE_NAME,
                     'exchange_order_id': order_id,
                     'timestamp': data['data']['timestamp'] / 1000,
                     'status': ResponseStatus.SUCCESS,
                     'api_response': data,
                     'size': 0,
                     'price': 0,
                     'time_order_sent': time_start,
                     'create_order_time': data['data']['timestamp'] / 1000 - time_start}
        self.responses.update({client_id: order_res})
        self.clients_ids.pop(order_id)

    @try_exc_async
    async def update_orders(self, data, channel):
        if channel == 'my_orders':
            for order_id, data in data['data'].items():
                if data:
                    if not data['data']['filled_quantity'] and self.clients_ids.get(order_id):
                        self.update_non_filled_response(data, order_id)
                    self.open_orders.update({order_id: data})
                    self.open_orders[order_id].update({'status': ResponseStatus.SUCCESS})
                else:
                    self.update_orders_list(order_id, None)
        else:
            if data['data']['event'] == 'rejected':
                print(f"ORDER REJECTED {data=}")
                order_id = data['data']['order_id']
                if self.clients_ids.get(order_id):
                    self.update_error_response(order_id, data)
                self.orders.update({data['data']['order_id']: {'status': ResponseStatus.ERROR,
                                                               'response': data}})
            elif data['data']['event'] == 'cancelled':
                order_id = data['data']['order_id']
                self.update_orders_list(order_id, data['data']['timestamp'])

    @try_exc_regular
    def update_error_response(self, order_id, data):
        client_id = self.clients_ids[order_id][0]
        time_start = self.clients_ids[order_id][1]
        order_res = {'exchange_name': self.EXCHANGE_NAME,
                     'exchange_order_id': order_id,
                     'timestamp': time.time(),
                     'status': ResponseStatus.ERROR,
                     'api_response': data,
                     'size': 0,
                     'price': 0,
                     'time_order_sent': time_start,
                     'create_order_time': time.time() - time_start}
        self.responses.update({client_id: order_res})
        self.clients_ids.pop(order_id)
        # example_orders = {'subscription': {'channel': 'my-orders', 'instrument': 'MANA-PERP', 'account': 'Main'},
        #                   'data': {'c6fd6229-3d30-4843-a66c-6976c16bd311':
        #            {'type': 'order', 'filled_quantity': 0.0, 'order_id': 'c6fd6229-3d30-4843-a66c',
        #             'order_type': 'limit', 'price': 0.434, 'quantity': 5.0, 'side': 'buy',
        #             'timestamp': 1706701632780}}}
        # example_orders_done = {
        #     'subscription': {'channel': 'my-orders', 'instrument': 'MANA-PERP', 'account': 'Main'},
        #     'data': {'925719f8-fc3d-4118-bbf8-d2822f6a5c55': None}}
        # example_canceled = {
        #     'subscription': {'channel': 'my-market-events', 'instrument': 'BTC-PERP', 'account': 'Main'},
        #     'data': {'event': 'cancelled', 'order_id': '6e11e455-d036-425e-bb4c-b37453db746e',
        #              'cancel_id': '4f8e8cb2-aaf0-4f7c-81d3-2999e0f0e9d0', 'timestamp': 1706626380089}}

    @try_exc_regular
    def update_filled_response(self, data, result, order_id):
        client_id = self.clients_ids[order_id][0]
        time_start = self.clients_ids[order_id][1]
        # example = {'type': 'order', 'filled_quantity': 0.0, 'order_id': 'c6fd6229-3d30-4843-a66c',
        #            'order_type': 'limit', 'price': 0.434, 'quantity': 5.0, 'side': 'buy',
        #            'timestamp': 1706701632780}
        order_res = {'exchange_name': self.EXCHANGE_NAME,
                     'exchange_order_id': order_id,
                     'timestamp': result['ts_update'],
                     'status': ResponseStatus.SUCCESS,
                     'api_response': data,
                     'size': result['factual_amount_coin'],
                     'price': result['factual_price'],
                     'time_order_sent': time_start,
                     'create_order_time': result['ts_update'] / 1000 - time_start}
        self.responses.update({client_id: order_res})
        self.clients_ids.pop(order_id)

    @try_exc_async
    async def update_fills(self, data):
        print(f"{self.EXCHANGE_NAME} GOT FILL {datetime.utcnow()}")
        if self.multibot:
            loop = asyncio.get_event_loop()
            loop.create_task(self.multibot.update_all_av_balances())
        order_id = data['data']['order_id']
        if old_data := self.fills.get(order_id):
            new_filled_size = old_data['factual_amount_coin'] + data['data']['quantity']
            new_filled_usd = old_data['factual_amount_usd'] + data['data']['price'] * data['data']['quantity']
            new_price = new_filled_usd / new_filled_size
            status = self.get_order_status_by_fill(order_id, new_filled_size)
            result = self.fills[order_id]
            result.update({'status': status,
                           'factual_price': new_price,
                           'factual_amount_coin': new_filled_size,
                           'factual_amount_usd': new_filled_usd,
                           'ts_update': data['data']['timestamp'] / 1000,
                           'datetime_update': datetime.utcnow()})
        else:
            status = self.get_order_status_by_fill(order_id, data['data']['quantity'])
            result = {'exchange_order_id': order_id,
                      'exchange': self.EXCHANGE_NAME,
                      'status': status,
                      'factual_price': data['data']['price'],
                      'factual_amount_coin': data['data']['quantity'],
                      'factual_amount_usd': data['data']['price'] * data['data']['quantity'],
                      'datetime_update': datetime.utcnow(),
                      'ts_update': data['data']['timestamp'] / 1000}
        if self.clients_ids.get(order_id):
            self.update_filled_response(data, result, order_id)
        if status == OrderStatus.FULLY_EXECUTED:
            self.orders.update({order_id: result})
            self.fills.pop(order_id, None)
        else:
            self.fills.update({order_id: result})
        # example = {'subscription': {'channel': 'my-market-events', 'instrument': 'BTC-PERP', 'account': 'Main'},
        #            'data': {'event': 'traded', 'price': 43374.0, 'quantity': 0.001, 'side': 'sell',
        #                     'order_id': '1f0f69dd-6dde-4174-b9f8-177d429ebaf9', 'timestamp': 1706626411802,
        #                     'role': 'taker'/'maker', 'trade_no': '139709082596'}}

    @try_exc_async
    async def create_fast_order(self, price, size, side, market, client_id=None, last_id=None):
        order_id = uuid.uuid4()
        time_start = time.time()
        data = {"command": "place-order",
                "instrument": market,
                "price": price,
                "quantity": size,
                "order_type": "limit",
                "side": side.lower(),
                "order_id": order_id
                }
        if last_id:
            data.update({'order_id_to_replace': last_id})
        await self.ws_orders.send_json(data)
        self.clients_ids.update({order_id: [client_id, time_start]})
        # while True:
        #     if order := self.open_orders.get(order_id):
        #         break
        #     elif order := self.fills.get(order_id):
        #         break
        #     await asyncio.sleep(0.001)
        # self.LAST_ORDER_ID = order_id
        # order_res = {'exchange_name': self.EXCHANGE_NAME,
        #              'exchange_order_id': self.LAST_ORDER_ID,
        #              'timestamp': response[0]['timestamp'] / 1000 if response[0].get('timestamp') else time.time(),
        #              'status': status,
        #              'api_response': response,
        #              'size': response[0]['fillSize'],
        #              'price': response[0]['avgFillPrice'],
        #              'create_order_time': response[0]['timestamp'] / 1000 - time_start}
        # if response[0].get("clOrderID"):
        #     self.responses.update({response[0]["clOrderID"]: order_res})
        # else:
        #     self.responses.update({response[0]['orderId']: order_res})

    def get_order_status_by_fill(self, order_id, fill_size):
        orig_size = self.open_orders.get(order_id, {}).get('quantity')
        print(f'Label 3 {orig_size=},{fill_size=}')
        if orig_size == fill_size or not orig_size:
            return OrderStatus.FULLY_EXECUTED
        else:
            return OrderStatus.PARTIALLY_EXECUTED

    @try_exc_async
    async def update_positions(self, data):
        for market, position in data['data'].items():
            if position:
                change_price = self.get_orderbook(market)['asks'][0][0]
                self.positions.update({market: {'timestamp': datetime.utcnow().timestamp(),
                                                'entry_price': position['avg_price'],
                                                'amount': position['quantity'],
                                                'amount_usd': position['quantity'] * change_price}})
            else:
                self.positions.pop(market)
        # example = {'subscription': {'channel': 'my-positions', 'account': 'Main'}, 'data': {
        #     'BTC-PERP': {'quantity': 0.001, 'avg_price': 43394.0, 'cost': 43.394, 'long_open_qty': 0.0,
        #                  'short_open_qty': 0.0, 'adl_quartile': 1}}}
        # example_opened_order = {'subscription': {'channel': 'my-positions', 'account': 'Main'},
        #                         'data': {'BTC-PERP': None}}

    @try_exc_async
    async def update_balances(self, data):
        self.balance = {'total': data['data']['margin_balance'],
                        'free': data['data']['available_balance'],
                        'timestamp': time.time()}

    @try_exc_async
    async def subscribe_balances(self):
        method = {"command": "subscribe",
                  "channel": "my-account-overview"}
        await self._connected.wait()
        await self._ws_public.send_json(method)

    @try_exc_async
    async def subscribe_positions(self):
        method = {"command": "subscribe",
                  "channel": "my-positions"}
        await self._connected.wait()
        await self._ws_public.send_json(method)

    @try_exc_async
    async def subscribe_market_events(self, symbol):
        method = {"command": "subscribe",
                  "channel": "my-market-events",
                  "instrument": symbol}
        await self._connected.wait()
        await self._ws_public.send_json(method)

    @try_exc_async
    async def subscribe_open_orders(self, symbol):
        method = {"command": "subscribe",
                  "channel": "my-orders",
                  "instrument": symbol}
        await self._connected.wait()
        await self._ws_public.send_json(method)

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
        if self.state == 'Bot':
            self.wst_orders.daemon = True
            self.wst_orders.start()


if __name__ == '__main__':
    import configparser

    config = configparser.ConfigParser()
    config.read('config.ini')
    for key in config:
        print(key)
    keys = config['GLOBE']
    client = GlobeClient(keys=keys)
    print(len(client.get_markets()))
    client.run_updater()
    while True:
        time.sleep(1)
    #     print(client.get_all_tops())
