import time
import json
import traceback

import requests
import hmac
import hashlib
import aiohttp
import asyncio
import threading
from core.wrappers import try_exc_regular, try_exc_async
from datetime import datetime
import uvloop
import gc
import socket
import aiodns
from aiohttp.resolver import AsyncResolver
from clients.core.enums import ResponseStatus, OrderStatus
import string
import random

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


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
        self.positions = {}
        self.balance = {'total': 0,
                        'free': 0}
        self.leverage = leverage
        if keys:
            self.api_key = keys['API_KEY']
            self.api_secret = keys['API_SECRET']
            self.order_loop = asyncio.new_event_loop()
        self.get_orderbook_by_symbol_reg('THB_USDT')
        self.get_markets_names()
        self.get_real_balance()
        self.fill_instruments()
        self.clean_empty_markets()
        self.ob_len = ob_len
        self.max_pos_part = max_pos_part
        self.error_info = None
        self.LAST_ORDER_ID = 'default'
        self.taker_fee = 0.0025
        self.maker_fee = 0.0025
        self.async_tasks = []
        self.responses = {}
        self.orders = {}
        self.rate_limit_orders = 200
        self.cancel_responses = {}
        self.top_ws_ping = 5
        print(f"{self.EXCHANGE_NAME} INITIALIZED. STATE: {self.state}\n")

    @try_exc_regular
    def get_balance(self):
        return self.balance['total']

    @staticmethod
    @try_exc_regular
    def id_generator(size=4, chars=string.ascii_letters):
        return "".join(random.choice(chars) for _ in range(size))

    @try_exc_async
    async def _run_order_loop(self, loop):
        request_pause = 1.02 / self.rate_limit_orders
        # connector = aiohttp.TCPConnector(family=socket.AF_INET6)
        resolver = AsyncResolver()
        connector = aiohttp.TCPConnector(resolver=resolver, family=socket.AF_INET)
        async with aiohttp.ClientSession(connector=connector) as self.async_session:
            self.async_session.headers.update(self.headers)
            loop.create_task(self.keep_alive_order())
            while True:
                for task in self.async_tasks:
                    if task[0] == 'create_order':
                        price = task[1]['price']
                        size = task[1]['size']
                        side = task[1]['side']
                        market = task[1]['market']
                        client_id = task[1].get('client_id')
                        if task[1].get('hedge'):
                            await self.create_fast_order(price, size, side, market, client_id)
                        else:
                            loop.create_task(self.create_fast_order(price, size, side, market, client_id))
                        await asyncio.sleep(request_pause)
                    elif task[0] == 'cancel_order':
                        if task[1]['order_id'] not in self.deleted_orders:
                            if len(self.deleted_orders) > 100:
                                self.deleted_orders = []
                            self.deleted_orders.append(task[1]['order_id'])
                            loop.create_task(self.cancel_order(task[1]['order_id']))
                            await asyncio.sleep(request_pause)
                    elif task[0] == 'amend_order':
                        if task[1]['order_id'] not in self.deleted_orders:
                            price = task[1]['price']
                            size = task[1]['size']
                            order_id = task[1]['order_id']
                            market = task[1]['market']
                            old_order_size = task[1]['old_order_size']
                            loop.create_task(self.amend_order(price, size, order_id, market, old_order_size))
                            await asyncio.sleep(request_pause)
                    self.async_tasks.remove(task)
                await asyncio.sleep(0.00001)

    @try_exc_async
    async def keep_alive_order(self):
        while True:
            await asyncio.sleep(3)
            # if self.market_finder:
            # loop.create_task(self.check_extra_orders())
            await self.get_balance_async()
            if self.multibot:
                if self.multibot.market_maker:
                    if self.multibot.mm_exchange == self.EXCHANGE_NAME:
                        return
            # market = self.markets[self.markets_list[random.randint(0, len(self.markets_list) - 1)]]
            market = 'THB_USDT'
            price = self.get_orderbook(market)['bids'][0][0] * .9
            await self.create_fast_order(price, 10, "buy", market, "keep-alive")
            resp = self.responses.pop('keep-alive', {})
            if resp:
                ex_order_id = resp['exchange_order_id']
                canceled = await self.cancel_order(ex_order_id)
                # print(f"Canceled order resp: {canceled}")
            else:
                print(f'KEEP ALIVE ORDER WAS NOT CREATED')

    @try_exc_async
    async def amend_order(self, price: float, size: float, order_id: str, market: str, old_order_size: float):
        pass

    @try_exc_regular
    def fit_sizes(self, price: float, amount: float, market: str) -> (float, float):
        return price, amount

    @try_exc_regular
    def get_available_balance(self):
        max_pos_part = self.max_pos_part
        positions = self.positions
        balance = self.balance
        available_balances = {}
        position_value_abs = 0
        available_margin = balance['total']
        avl_margin_per_market = available_margin / 100 * max_pos_part
        for symbol, position in positions.items():
            if position.get('amount_usd'):
                # position_value += position['amount_usd']
                position_value_abs += position['amount_usd']
                available_balances.update({symbol: {'buy': avl_margin_per_market - position['amount_usd'],
                                                    'sell': position['amount_usd']}})
        if position_value_abs <= available_margin:
            # Это по сути доступный баланс для открытия новых позиций
            available_balances['buy'] = available_margin - position_value_abs
            available_balances['sell'] = position_value_abs
        else:
            for symbol, position in positions.items():
                if position.get('amount_usd'):
                    if position['amount_usd'] < 0:
                        available_balances.update({symbol: {'buy': abs(position['amount_usd']), 'sell': 0}})
                    else:
                        available_balances.update({symbol: {'buy': 0, 'sell': abs(position['amount_usd'])}})
            available_balances['buy'] = 0
            available_balances['sell'] = 0
        available_balances['balance'] = balance['total']
        return available_balances

    @try_exc_regular
    def get_orderbook(self, market):
        ob = self.orderbook.get(market)
        if not ob:
            ob = self.get_orderbook_by_symbol_reg(market)
        return ob

    @try_exc_regular
    def create_order(self, price: float, size: float, side: str, market: str, client_id: str = None):
        bid_ask = 'bid' if side == 'buy' else 'ask'
        path = f'/api/v3/market/place-{bid_ask}'
        market = self.market_rename(market)
        req_body = {
            'sym': market.lower(),  # {quote}_{base}
            'amt': size,
            'rat': price,
            'typ': 'limit'  # limit, market
        }
        print(self.EXCHANGE_NAME, 'CREATING ORDER', req_body)
        if market != 'USDT_THB':
            change = self.get_thb_rate()
            req_body['rat'] = req_body['rat'] * change
        headers = self.get_auth_for_request(path=path, method='POST', body=req_body)
        response = self.session.post(self.BASE_URL + path, data=json.dumps(req_body), headers=headers)
        resp = response.json()
        if resp['error']:
            self.error_info = response.text
            return {'exchange_name': self.EXCHANGE_NAME,
                    'exchange_order_id': None,
                    'timestamp': int(round((datetime.utcnow().timestamp()) * 1000)),
                    'status': ResponseStatus.ERROR}
        else:
            exchange_order_id = resp['result']['hash']
            self.LAST_ORDER_ID = exchange_order_id
            return {'exchange_name': self.EXCHANGE_NAME,
                    'exchange_order_id': exchange_order_id,
                    'timestamp': int(resp['result']['ts']),
                    'status': ResponseStatus.SUCCESS}
        # example = {"error": 0,
        #            "result": {"id": "46850668", "hash": "fwQ6dnQjgbQVtCX1PXkFRuLJNXu", "typ": "limit", "amt": 30,
        #                       "rat": 1846.75, "fee": 138.51, "cre": 0, "rec": 55263.99, "ts": "1713445973"}}

    @try_exc_regular
    def market_rename(self, market: str) -> str:
        if market.startswith('THB') or market.startswith('thb'):
            return market.split('_')[1] + '_THB'
        return market

    @try_exc_async
    async def create_fast_order(self, price: float, size: float, side: str, market: str, client_id: str = None):
        time_start = time.time()
        bid_ask = 'bid' if side == 'buy' else 'ask'
        path = f'/api/v3/market/place-{bid_ask}'
        market = self.market_rename(market)
        req_body = {
            'sym': market.lower(),  # {quote}_{base}
            'amt': size,
            'rat': price,
            'typ': 'limit'  # limit, market
        }
        # print(self.EXCHANGE_NAME, req_body)
        if market != 'USDT_THB':
            change = self.get_thb_rate()
            req_body['rat'] = req_body['rat'] * change
        headers = self.get_auth_for_request(path=path, method='POST', body=req_body)
        async with self.async_session.post(self.BASE_URL + path, data=json.dumps(req_body), headers=headers) as resp:
            response = await resp.json()
            if client_id != 'keep-alive':
                print(f'{self.EXCHANGE_NAME} order response: {response}')
                print(f"{self.EXCHANGE_NAME} create order time: {time.time() - time_start}")
            if response['error']:
                print(f'{self.EXCHANGE_NAME} create order error: {response}')
            else:
                order_id = response['result'].get('hash', 'default')
                result = self.get_order_by_id(market, order_id)
                order_res = {'exchange_name': self.EXCHANGE_NAME,
                             'exchange_order_id': order_id,
                             'timestamp': float(response['result']['ts']) if response['result'].get('ts') else time.time(),
                             'status': result['status'] if result else OrderStatus.NOT_PLACED,
                             'api_response': response['result'],
                             'size': result['factual_amount_coin'] if result else 0,
                             'price': result['factual_price'] if result else 0,
                             'time_order_sent': time_start,
                             'create_order_time': float(response['result']['ts']) - time_start}
                if client_id:
                    self.responses.update({client_id: order_res})
                    self.LAST_ORDER_ID = order_id
                    if not client_id.startswith('keep'):
                        print(self.responses)
                else:
                    self.responses.update({order_id: order_res})
                # example = {'error': 0,
                #  'result': {'id': '46999726', 'hash': 'fwQ6dnQjgbQVtT8Lu9MLodY7mpP', 'typ': 'limit', 'amt': 1, 'rat': 37.01,
                #             'fee': 0.1, 'cre': 0, 'rec': 36.91, 'ts': '1713692356'}}
                # example_get = {'error': 0,
                #                'result': {'amount': 9.71, 'client_id': '', 'credit': 0, 'fee': 0.03, 'filled': 0,
                #                           'first': '57987204', 'history': [], 'id': '57987204', 'last': '',
                #                           'parent': '0', 'partial_filled': False, 'post_only': False, 'rate': 33.36,
                #                           'remaining': 9.71, 'side': 'buy', 'status': 'unfilled', 'total': 9.71}}

    @try_exc_regular
    def get_order_status(self, response):
        status = OrderStatus.PROCESSING
        if response['result']['status'] == 'filled':
            status = OrderStatus.FULLY_EXECUTED
        elif response['result']['status'] == 'unfilled' and response['result']['partial_filled']:
            status = OrderStatus.PARTIALLY_EXECUTED
        elif response['result']['status'] == 'cancelled' and not response['result']['partial_filled']:
            status = OrderStatus.NOT_EXECUTED
        return status

    @try_exc_regular
    def get_orders(self):
        return self.orders

    @try_exc_regular
    def get_order_by_id(self, symbol, order_id: str):
        path = '/api/v3/market/order-info'
        query = {'hash': order_id}
        post_string = '?' + "&".join([f"{key}={query[key]}" for key in sorted(query)])
        # print(f"{post_string=}")
        headers = self.get_auth_for_request(path=path + post_string, method='GET')
        response = self.session.get(self.BASE_URL + path + post_string, headers=headers)
        resp = response.json()
        # print('GET ORDER BY ID RESPONSE', self.EXCHANGE_NAME, resp)
        if resp['error']:
            print(f"GET ORDER BY ID ERROR {self.EXCHANGE_NAME}: {resp}")
        else:
            timestamp = float(resp['result']['history'][0]['timestamp']) / 1000 if len(
                resp['result']['history']) else time.time()
            real_price = 0
            real_size = 0
            real_size_usd = 0
            thb_rate = self.get_thb_rate()
            for fill in resp['result']['history']:
                real_size += fill['amount']
                real_size_usd += fill['rate'] * fill['amount']
            if symbol != 'THB_USDT':
                if real_size:
                    real_price = real_size_usd / real_size / thb_rate
            else:
                if real_size:
                    real_price = real_size_usd / real_size
            result = {'exchange_order_id': order_id,
                      'exchange_name': self.EXCHANGE_NAME,
                      'status': self.get_order_status(resp),
                      'factual_price': real_price,
                      'factual_amount_coin': real_size,
                      'factual_amount_usd': real_size_usd,
                      'datetime_update': datetime.utcnow(),
                      'ts_update': timestamp}
            self.orders.update({order_id: result})
            return result

    @try_exc_async
    async def cancel_order(self, order_id):
        path = f'/api/v3/market/cancel-order'
        req_body = {
            'hash': order_id
        }
        headers = self.get_auth_for_request(path=path, method='POST', body=req_body)
        async with self.async_session.post(self.BASE_URL + path,
                                           data=json.dumps(req_body),
                                           headers=headers) as response:
            resp = await response.json()
            if resp['error']:
                print(f"{self.EXCHANGE_NAME} canceling order error: {resp}")
            else:
                self.cancel_responses.update({order_id: resp})
                return resp

    @try_exc_regular
    def get_all_open_orders(self):
        path = '/api/v3/market/my-open-orders'
        open_orders = []
        for _market in self.markets.values():
            market = _market.split('_')[1] + '_THB'
            req_body = {'sym': market}
            post_string = '?' + "&".join([f"{key}={req_body[key]}" for key in sorted(req_body)])
            headers = self.get_auth_for_request(path=path + post_string, method='GET')
            response = self.session.get(url=self.BASE_URL + path + post_string, headers=headers)
            resp = response.json()
            if resp['error']:
                print(f'Fetching open orders for {market} error', self.EXCHANGE_NAME, response, response.text)
            else:
                for order in resp['result']:
                    order.update({'market': market})
                    open_orders.append(order)
        return open_orders

    @try_exc_regular
    def cancel_all_orders(self):
        open_orders = self.get_all_open_orders()
        for order in open_orders:
            self.cancel_order_reg(order['hash'])

    @try_exc_regular
    def cancel_order_reg(self, order_id):
        path = f'/api/v3/market/cancel-order'
        req_body = {
            'hash': order_id
        }
        headers = self.get_auth_for_request(path=path, method='POST', body=req_body)
        response = self.session.post(self.BASE_URL + path, data=json.dumps(req_body), headers=headers)
        resp = response.json()
        if resp['error']:
            print(f"{self.EXCHANGE_NAME} canceling order error: {resp}")
        else:
            self.cancel_responses.update({order_id: resp})
            return resp

    @try_exc_async
    async def get_balance_async(self):
        path = '/api/v3/market/wallet'
        # ts = str(int(round(time.time() * 1000)))
        req_body = {}  # {'ts': ts}
        headers = self.get_auth_for_request(path=path, method='POST', body=req_body)
        async with self.async_session.post(url=self.BASE_URL + path,
                                           headers=headers,
                                           data=json.dumps(req_body)) as response:
            resp = await response.json()
            if resp['error']:
                print('Fetching http async balance error', self.EXCHANGE_NAME, response)
            else:
                self.unpack_wallet_data(resp)

    @try_exc_regular
    def get_position(self):
        self.get_real_balance()

    @try_exc_regular
    def get_real_balance(self):
        path = '/api/v3/market/wallet'
        # ts = str(int(round(time.time() * 1000)))
        req_body = {}  # {'ts': ts}
        headers = self.get_auth_for_request(path=path, method='POST', body=req_body)
        response = self.session.post(url=self.BASE_URL + path, data=json.dumps(req_body), headers=headers)
        resp = response.json()
        if resp['error']:
            print('Fetching http balance error', self.EXCHANGE_NAME, response)
        else:
            self.unpack_wallet_data(resp)

    @try_exc_regular
    def unpack_wallet_data(self, resp):
        total_balance_usdt = 0
        for coin, amount in resp['result'].items():
            if amount:
                if coin == 'USDT':
                    total_balance_usdt += amount
                elif coin == 'THB':
                    change_rate = self.get_thb_rate()
                    total_balance_usdt += amount / change_rate
                else:
                    change_ob = self.get_orderbook(self.markets[coin])
                    change_rate = (change_ob['asks'][0][0] + change_ob['bids'][0][0]) / 2
                    total_balance_usdt += amount * change_rate
        resp['result'].update({'total': total_balance_usdt,
                               'timestamp': round(datetime.utcnow().timestamp())})
        if resp['result'] != self.balance and self.state == 'Bot' and self.multibot:
            self.order_loop.create_task(self.multibot.update_all_av_balances())
        self.balance.update(resp['result'])
        self.update_positions()

    @try_exc_regular
    def update_positions(self):
        for coin, position in self.balance.items():
            if coin in ['timestamp', 'total', 'THB', 'USDT']:
                continue
            if position:
                market = self.markets[coin]
                change_ob = self.get_orderbook(market)
                change_rate = (change_ob['asks'][0][0] + change_ob['bids'][0][0]) / 2
                self.positions.update({market: {'side': 'LONG',
                                                'amount_usd': position * change_rate,
                                                'amount': position,
                                                'entry_price': 0,
                                                'unrealized_pnl_usd': 0,
                                                'realized_pnl_usd': 0,
                                                'lever': self.leverage}})

    @try_exc_regular
    def deals_thread_func(self, loop):
        while True:
            loop.run_until_complete(self._run_order_loop(loop))

    @try_exc_regular
    def run_updater(self):
        wst_public = threading.Thread(target=self._run_ws_forever, args=[asyncio.new_event_loop()])
        wst_public.daemon = True
        wst_public.start()
        if self.state == 'Bot':
            orders_thread = threading.Thread(target=self.deals_thread_func, args=[self.order_loop])
            orders_thread.daemon = True
            orders_thread.start()

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
                        async with session.ws_connect(endpoint + str(id)) as ws:
                            self._ws_public = ws
                            loop.create_task(self._ping(ws))
                            async for msg in ws:
                                await self.process_ws_msg(msg)
                        await ws.close()
                        time.sleep(5)
                        break

    @try_exc_regular
    def get_positions(self):
        return self.positions

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

    @try_exc_async
    async def process_ws_msg(self, msg: aiohttp.WSMessage):
        data = json.loads(msg.data)
        if market_id := data.get('pairing_id'):
            market = self.market_id_list[market_id]
            event = data['event']
            # print(market, event)
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
                                               'timestamp': ts,
                                               'bids': new_bids})
                if top_ask and top_ask > self.orderbook[market]['asks'][0][0]:
                    side = 'buy'
                elif top_bid and top_bid < self.orderbook[market]['bids'][0][0]:
                    side = 'sell'
            elif event == 'askschanged':
                if market != 'THB_USDT':
                    change = self.get_thb_rate()
                    new_asks = [[x[1] / change, x[2]] for x in data['data'][:self.ob_len]]
                else:
                    new_asks = [[x[1], x[2]] for x in data['data'][:self.ob_len]]
                new_asks = self.merge_similar_orders(new_asks)
                self.orderbook[market].update({'ts_ms': ts,
                                               'timestamp': ts,
                                               'asks': new_asks})
                if top_ask and top_ask > self.orderbook[market]['asks'][0][0]:
                    side = 'buy'
                elif top_bid and top_bid < self.orderbook[market]['bids'][0][0]:
                    side = 'sell'
            elif event == 'tradeschanged':
                # if len(data['data'][0]):
                #     timestamp = min([data['data'][0][0][0], data['data'][0][1][0]])
                # else:
                timestamp = time.time()
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
                                               'timestamp': timestamp,
                                               'asks': new_asks,
                                               'bids': new_bids})
                if top_ask and top_ask > self.orderbook[market]['asks'][0][0]:
                    side = 'buy'
                elif top_bid and top_bid < self.orderbook[market]['asks'][0][0]:
                    side = 'sell'
            if self.finder and side:  # and ts_ms - ts_ob < self.top_ws_ping:
                coin = market.split('_')[1]
                await self.finder.count_one_coin(coin, self.EXCHANGE_NAME, side, 'ob')

    @staticmethod
    async def _ping(ws: aiohttp.ClientSession.ws_connect):
        while True:
            await asyncio.sleep(10)
            # Adjust the ping interval as needed
            try:
                await ws.ping()
            except:
                return
            # print(f'PING SENT: {datetime.utcnow()}')

    @try_exc_regular
    def get_markets_names(self):
        path = '/api/market/symbols'
        response = self.session.get(url=self.BASE_URL + path)
        resp = response.json()
        if not resp['error']:
            for market in resp['result']:
                self.market_id_list.update({market['id']: market['symbol']})
                coin = market['symbol'].split('_')[1]
                self.markets.update({coin: market['symbol']})

    @try_exc_regular
    def fill_instruments(self):
        for coin, market in self.markets.items():
            if self.state == 'Bot':
                ob = self.get_orderbook(market)
                if ob:
                    px = ob['asks'][0][0]
                else:
                    continue
                self.instruments.update({market: {'coin': coin,
                                                  'quantity_precision': 0.0000000001,
                                                  'tick_size': 0.0000000001,
                                                  'step_size': 0.00000000001,
                                                  'min_size': 20 / px,
                                                  'price_precision': 0.00000000001}})
                time.sleep(0.1)
            else:
                for market in self.positions.keys():
                    px = self.get_orderbook(market)['asks'][0][0]
                    self.instruments.update({market: {'coin': coin,
                                                      'quantity_precision': 0.0000000001,
                                                      'tick_size': 0.0000000001,
                                                      'step_size': 0.00000000001,
                                                      'min_size': 20 / px,
                                                      'price_precision': 0.00000000001}})

    @try_exc_regular
    def clean_empty_markets(self):
        for coin, market in self.markets.items():
            if not market:
                del self.markets[coin]
                self.clean_empty_markets()
                break

    @try_exc_regular
    def get_markets(self):
        return self.markets

    @try_exc_regular
    def get_thb_rate(self):
        ob = self.get_orderbook('THB_USDT')
        change_rate = (ob['asks'][0][0] + ob['bids'][0][0]) / 2
        return change_rate

    @try_exc_async
    async def get_orderbook_by_symbol(self, market: str, limit: int = 10):
        path = '/api/market/depth'
        params = {'sym': market,
                  'lmt': limit}
        post_string = '?' + "&".join([f"{key}={params[key]}" for key in sorted(params)])
        async with aiohttp.ClientSession() as session:
            async with session.get(url=self.BASE_URL + path + post_string, headers=self.headers) as resp:
                response = await resp.json()
                ts = time.time()
                if error_code := response.get('error'):
                    print(market, response)
                    if error_code == 11:
                        coin = market.split('_')[1]
                        self.markets.pop(coin)
                    else:
                        print(f"RATE LIMIT REACHED")
                        time.sleep(30)
                        await self.get_orderbook_by_symbol(market)
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
                    return response

    @try_exc_regular
    def get_orderbook_by_symbol_reg(self, market: str, limit: int = 10):
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
                market = self.market_rename(market)
                coin = market.split('_')[0]
                self.markets.update({coin: None})
            else:
                print(f"RATE LIMIT REACHED")
                time.sleep(30)
                self.get_orderbook_by_symbol_reg(market)
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
            return response

    @try_exc_regular
    def get_signature(self, timestamp: str, req_type: str, path: str, body: dict = {}):
        payload = list()
        payload.append(timestamp)
        payload.append(req_type)
        payload.append(path)
        if req_type == 'POST':
            payload.append(json.dumps(body))
        payload_string = ''.join(payload)
        return hmac.new(self.api_secret.encode('utf-8'), payload_string.encode('utf-8'), hashlib.sha256).hexdigest()

    @try_exc_regular
    def get_auth_for_request(self, path: str, method: str, body: dict = {}, ts: str = ''):
        if not ts:
            ts = str(int(round(time.time() * 1000)))
        signature = self.get_signature(ts, method, path, body)
        headers = {'Accept': 'application/json',
                   'Content-type': 'application/json',
                   'X-BTK-APIKEY': self.api_key,
                   'X-BTK-TIMESTAMP': ts,
                   'X-BTK-SIGN': signature,
                   'Connection': 'keep-alive'}
        return headers


if __name__ == '__main__':
    import configparser

    config = configparser.ConfigParser()
    config.read('config.ini', "utf-8")
    client = BitKubClient(keys=config['BITKUB'],
                          leverage=float(config['SETTINGS']['LEVERAGE']),
                          max_pos_part=int(config['SETTINGS']['PERCENT_PER_MARKET']),
                          markets_list=['USDT'])
    client.markets_list = list(client.markets.values())

    client.run_updater()

    # time.sleep(3)
    # price = client.get_orderbook('THB_USDT')['bids'][0][0] * 0.95
    # order_data = client.create_order(price, 32, 'sell', 'THB_USDT')
    time.sleep(3)
    # print(f"{order_data=}")
    # cancel_data = client.cancel_order(order_data['exchange_order_id'])
    client.get_real_balance()
    print(client.balance)
    print(client.positions)
    while True:
        # print(client.get_all_open_orders())
        for market, book in client.orderbook.items():
            print(market, time.time() - book['timestamp'])
        print('\n\n\n')
        time.sleep(1)
        # print(client.balance)
        # print(client.responses)

    # print(f"{cancel_data=}")
    # print(f"{client.balance=}")
    # print(f"{client.positions=}")
    # print(f"{client.get_available_balance()}")
    # price = client.get_orderbook('THB_USDT')['bids'][0][0]
    # order_data = {'market': 'THB_USDT',
    #               'client_id': f'takerxxx{client.EXCHANGE_NAME}xxx' + client.id_generator() + 'xxx' + 'THB',
    #               'price': price,
    #               'size': 1,
    #               'side': 'sell'}
    # print(f"{order_data=}")
    # client.async_tasks.append(['create_order', order_data])
    # time.sleep(1)
    # print(client.responses)
    # time.sleep(1)

    # client.get_server_time()
    # client.get_markets_names()
    # asyncio.run(test_order())
