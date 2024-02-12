import time
import traceback
import aiohttp
import json
import requests
from datetime import datetime
import asyncio
import threading
import hmac
import hashlib
import base64
from clients.core.enums import ResponseStatus, OrderStatus
from core.wrappers import try_exc_regular, try_exc_async
from clients.core.base_client import BaseClient
from aiohttp.client_exceptions import ContentTypeError
import random
import string
import uvloop
import gc

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class WhiteBitClient(BaseClient):
    PUBLIC_WS_ENDPOINT = 'wss://api.whitebit.com/ws'
    BASE_URL = 'https://whitebit.com'
    EXCHANGE_NAME = 'WHITEBIT'
    headers = {"Accept": "application/json;charset=UTF-8",
               "Content-Type": "application/json",
               'User-Agent': 'python-whitebit-sdk',
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
        self.order_loop = asyncio.new_event_loop()
        self.instruments = {}
        self.markets = self.get_markets()
        self.orderbook = {}
        if self.state == 'Bot':
            self.api_key = keys['API_KEY']
            self.api_secret = keys['API_SECRET']
            self.websocket_token = self.get_ws_token()
            # self.deals_thread_func()
            self.orders = {}
            self.balance = {}
            self.positions = {}
            self.get_real_balance()
        self.ob_len = ob_len
        self.leverage = leverage
        self.max_pos_part = max_pos_part
        self.error_info = None
        self.LAST_ORDER_ID = 'default'
        self.taker_fee = 0.00035 * 0.6
        self.maker_fee = 0.0001 * 0.6
        self.last_keep_alive = 0
        self.last_websocket_ping = 0
        self.async_tasks = []
        self.responses = {}
        self.deleted_orders = []
        self.request_timer = time.time()
        self.requests_counter = 0
        self.total_requests = 0
        self.total_start_time = time.time()
        self.top_ws_ping = 0.06
        self.stop_all = False
        self.cancel_all_orders()

    @try_exc_regular
    def deals_thread_func(self, loop):
        while True:
            loop.run_until_complete(self._run_order_loop(loop))

    @try_exc_async
    async def cancel_all_tasks(self, loop):
        tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
            try:
                await task  # Wait for the task to be cancelled
            except asyncio.CancelledError:
                pass

    @try_exc_async
    async def _run_order_loop(self, loop):
        async with aiohttp.ClientSession() as self.async_session:
            self.async_session.headers.update(self.headers)
            while True:
                await self.get_position_async()
                market = self.markets[self.markets_list[random.randint(0, len(self.markets_list) - 1)]]
                price = self.get_orderbook(market)['bids'][0][0] * 0.95
                price, size = self.fit_sizes(price, self.instruments[market]['min_size'], market)
                await self.create_fast_order(price, size, 'buy', market, 'keep-alive')
                resp = self.responses.get('keep-alive')
                ex_order_id = resp['exchange_order_id']
                await self.cancel_order(market, ex_order_id)
                await asyncio.sleep(3)
            # while True:
            #     for task in self.async_tasks:
            #         if task[0] == 'create_order':
            #             price = task[1]['price']
            #             size = task[1]['size']
            #             side = task[1]['side']
            #             market = task[1]['market']
            #             client_id = task[1].get('client_id')
            #             if task[1].get('hedge'):
            #                 await self.create_fast_order(price, size, side, market, client_id)
            #             else:
            #                 loop.create_task(self.create_fast_order(price, size, side, market, client_id))
            #         elif task[0] == 'cancel_order':
            #             loop.create_task(self.cancel_order(task[1]['market'], task[1]['order_id']))
            #         elif task[0] == 'amend_order':
            #             market = task[1]['market']
            #             loop.create_task(self.cancel_order(market, task[1]['order_id']))
            #             loop.create_task(self.create_fast_order(task[1]['price'], task[1]['size'], task[1]['side'],
            #                                                     market, task[1]['client_id'], amend=True))
            #         self.async_tasks.remove(task)
            #     await asyncio.sleep(0.001)

    @try_exc_async
    async def keep_alive_order(self):
        while True:
            await self.get_position_async()
            market = self.markets[self.markets_list[random.randint(0, len(self.markets_list) - 1)]]
            price = self.get_orderbook(market)['bids'][0][0] * 0.95
            price, size = self.fit_sizes(price, self.instruments[market]['min_size'], market)
            await self.create_fast_order(price, size, 'buy', market, 'keep-alive')
            resp = self.responses.get('keep-alive')
            ex_order_id = resp['exchange_order_id']
            await self.cancel_order(market, ex_order_id)
            await asyncio.sleep(3)

    @staticmethod
    @try_exc_regular
    def id_generator(size=6, chars=string.ascii_letters):
        return ''.join(random.choice(chars) for _ in range(size))

    @try_exc_regular
    def get_markets(self):
        path = "/api/v4/public/markets"
        resp = self.session.get(url=self.BASE_URL + path).json()
        markets = {}
        for market in resp:
            if market['type'] == 'futures' and market['tradesEnabled']:
                markets.update({market['stock']: market['name']})
                price_precision = int(market['moneyPrec'])
                step_size = float(market['minAmount'])
                tick_size = self.calculate_tick_size(price_precision)
                quantity_precision = int(market['stockPrec'])
                self.instruments[market['name']] = {'tick_size': tick_size,
                                                    'step_size': step_size,
                                                    'quantity_precision': quantity_precision,
                                                    'price_precision': price_precision,
                                                    'min_size': step_size}
        return markets

    @try_exc_regular
    def set_leverage(self, leverage):
        path = "/api/v4/collateral-account/leverage"
        params = {'leverage': leverage}
        params = self.get_auth_for_request(params, path)
        path += self._create_uri(params)
        res = self.session.post(url=self.BASE_URL + path, json=params)
        return res.json()

    @try_exc_regular
    def cancel_all_orders(self):
        path = '/api/v4/order/cancel/all'
        params = self.get_auth_for_request({}, path)
        path += self._create_uri(params)
        res = self.session.post(url=self.BASE_URL + path, json=params)
        return res.json()

    @try_exc_async
    async def cancel_order(self, symbol: str, order_id: int):
        # time_start = time.time()
        # message = f'ORDER CANCEL ERROR\n'
        # message += f"{time_start=}\n"
        # message += f"{self.requests_counter=}\n"
        # message += f"{time_start - self.request_timer=}\n"
        # message += f"{self.total_requests=}\n"
        # message += f"{time_start - self.total_start_time=}\n"
        path = '/api/v4/order/cancel'
        params = {"market": symbol,
                  "orderId": order_id}
        if order_id in self.deleted_orders:
            return
        self.deleted_orders.append(order_id)
        params = self.get_auth_for_request(params, path)
        async with self.async_session.post(url=self.BASE_URL + path, headers=self.session.headers, json=params) as resp:
            try:
                response = await resp.json()
                # print(f'ORDER CANCELED {self.EXCHANGE_NAME}', response)
                if order_id in self.multibot.deleted_orders:
                    self.multibot.deleted_orders.remove(order_id)
                if 'maker' in response.get('clientOrderId', '') and self.EXCHANGE_NAME == self.multibot.mm_exchange:
                    coin = symbol.split('_')[0]
                    if self.multibot.open_orders.get(coin + '-' + self.EXCHANGE_NAME)[0] == response['orderId']:
                        self.multibot.open_orders.pop(coin + '-' + self.EXCHANGE_NAME)
            except:
                # print(resp.text)
                await asyncio.sleep(0.1)
                # await self.cancel_order(symbol, order_id)
                self.cancel_all_orders()
                if self.EXCHANGE_NAME == self.multibot.mm_exchange:
                    self.multibot.open_orders = {}

    # example = {'orderId': 422806159063, 'clientOrderId': 'maker-WHITEBIT-XRP-1003947', 'market': 'XRP_PERP',
    #            'side': 'buy', 'type': 'margin limit', 'timestamp': 1705924757.295865, 'dealMoney': '0',
    #            'dealStock': '0', 'amount': '110', 'takerFee': '0.00035', 'makerFee': '0.0001', 'left': '110',
    #            'dealFee': '0', 'ioc': False, 'postOnly': False, 'price': '0.531'}

    def get_auth_for_request(self, params, uri):
        params['request'] = uri
        nonce = int(time.time() * 1000)
        nonce += random.randint(-4000, 4000)
        if nonce not in self.nonces:
            self.nonces.append(nonce)
            if len(self.nonces) >= 100:
                self.nonces = self.nonces[::-1][:30]
        else:
            while nonce in self.nonces:
                nonce += 1
        params['nonce'] = nonce
        params['nonceWindow'] = True
        signature, payload = self.get_signature(params)
        self.session.headers.update({
            'X-TXC-APIKEY': self.api_key,
            'X-TXC-SIGNATURE': signature,
            'X-TXC-PAYLOAD': payload.decode('ascii')
        })
        return params

    @try_exc_async
    async def get_position_async(self):
        path = "/api/v4/collateral-account/positions/open"
        params = self.get_auth_for_request({}, path)
        path += self._create_uri(params)
        async with self.async_session.post(url=self.BASE_URL + path, json=params, headers=self.session.headers) as res:
            try:
                response = await res.json()
            except:
                return
            self.positions = {}
            for pos in response:
                if isinstance(pos, str):
                    print(f"{self.EXCHANGE_NAME} position update in get_position mistake {response}")
                    continue
                market = pos['market']
                ob = self.get_orderbook(market)
                if not ob:
                    ob = await self.get_orderbook_by_symbol(market)
                change = (ob['asks'][0][0] + ob['bids'][0][0]) / 2
                if pos['basePrice'] and pos['amount']:
                    unrealised_pnl = (change - float(pos['basePrice'])) * float(pos['amount'])
                else:
                    unrealised_pnl = 0
                self.positions.update({market: {'timestamp': int(datetime.utcnow().timestamp()),
                                                'entry_price': float(pos['basePrice']) if pos['basePrice'] else 0,
                                                'amount': float(pos['amount']),
                                                'amount_usd': change * float(pos['amount']),
                                                'unrealised_pnl': unrealised_pnl}})

    @try_exc_regular
    def get_position(self):
        self.positions = {}
        path = "/api/v4/collateral-account/positions/open"
        params = self.get_auth_for_request({}, path)
        path += self._create_uri(params)
        res = self.session.post(url=self.BASE_URL + path, json=params)
        response = res.json()
        for pos in response:
            if isinstance(pos, str):
                print(f"{self.EXCHANGE_NAME} position update in get_position mistake {response}")
                continue
            market = pos['market']
            ob = self.get_orderbook(market)
            if not ob:
                ob = self.get_orderbook_http_reg(market)
            change = (ob['asks'][0][0] + ob['bids'][0][0]) / 2
            if pos['basePrice'] and pos['amount']:
                unrealised_pnl = (change - float(pos['basePrice'])) * float(pos['amount'])
            else:
                unrealised_pnl = 0
            self.positions.update({market: {'timestamp': int(datetime.utcnow().timestamp()),
                                            'entry_price': float(pos['basePrice']) if pos['basePrice'] else 0,
                                            'amount': float(pos['amount']),
                                            'amount_usd': change * float(pos['amount']),
                                            'unrealised_pnl': unrealised_pnl}})
        # example = [{'positionId': 3634420, 'market': 'BTC_PERP', 'openDate': 1703664697.619855,
        #             'modifyDate': 1703664697.619855,
        #             'amount': '0.001', 'basePrice': '42523.8', 'liquidationPrice': '0', 'pnl': '0.2',
        #             'pnlPercent': '0.47',
        #             'margin': '8.6', 'freeMargin': '41.6', 'funding': '0', 'unrealizedFunding': '0',
        #             'liquidationState': None},
        #            {'positionId': 3642507, 'market': 'BTC_PERP', 'openDate': 1703752465.897688,
        #             'modifyDate': 1703752465.897688, 'amount': '0', 'basePrice': '', 'liquidationPrice': None,
        #             'pnl': None,
        #             'pnlPercent': None, 'margin': '8.7', 'freeMargin': '13.6', 'funding': '0',
        #             'unrealizedFunding': '0',
        #             'liquidationState': None}]
        # example_negative_pos = [
        #     {'positionId': 3635477, 'market': 'BTC_PERP', 'openDate': 1703677698.102418,
        #      'modifyDate': 1703677698.102418,
        #      'amount': '-0.002', 'basePrice': '43131.7', 'liquidationPrice': '66743.8', 'pnl': '0',
        #      'pnlPercent': '0.00',
        #      'margin': '17.3', 'freeMargin': '33.2', 'funding': '0', 'unrealizedFunding': '0',
        #      'liquidationState': None}]

    @try_exc_regular
    def get_real_balance(self):
        path = "/api/v4/collateral-account/balance"
        params = self.get_auth_for_request({}, path)
        path += self._create_uri(params)
        res = self.session.post(url=self.BASE_URL + path, json=params)
        response = res.json()
        self.balance = {'timestamp': datetime.utcnow().timestamp(),
                        'total': float(response['USDT']),
                        'free': float(response['USDT'])}

    # example = {'ADA': '0', 'APE': '0', 'ARB': '0', 'ATOM': '0', 'AVAX': '0', 'BCH': '0', 'BTC': '0', 'DOGE': '0',
    #            'DOT': '0', 'EOS': '0', 'ETC': '0', 'ETH': '0', 'LINK': '0', 'LTC': '0', 'MATIC': '0', 'NEAR': '0',
    #            'OP': '0', 'SHIB': '0', 'SOL': '0', 'TRX': '0', 'UNI': '0', 'USDC': '0', 'USDT': '50', 'WBT': '0',
    #            'XLM': '0', 'XRP': '0'}

    @staticmethod
    @try_exc_regular
    def calculate_precision(precision):
        return max(len(str(precision).split('.')[-1]), 0) if '.' in str(precision) else 0

    @staticmethod
    @try_exc_regular
    def calculate_tick_size(precision):
        return float(f"1e-{precision}")

    @try_exc_async
    async def get_orderbook_by_symbol(self, symbol):
        async with aiohttp.ClientSession() as session:
            path = f'/api/v4/public/orderbook/{symbol}'
            params = {'limit': 10}  # Adjusting parameters as per WhiteBit's documentation
            path += self._create_uri(params)
            async with session.get(url=self.BASE_URL + path, headers=self.headers) as resp:
                ob = await resp.json()
                # Check if the response is a dictionary and has 'asks' and 'bids' directly within it
                if isinstance(ob, dict) and 'asks' in ob and 'bids' in ob:
                    orderbook = {
                        'asks': [[float(ask[0]), float(ask[1])] for ask in ob['asks']],
                        'bids': [[float(bid[0]), float(bid[1])] for bid in ob['bids']],
                        'timestamp': datetime.utcnow().timestamp(),
                        'top_ask_ts': datetime.utcnow().timestamp(),
                        'top_bid_ts': datetime.utcnow().timestamp()}
                    return orderbook

    @try_exc_regular
    def get_orderbook_http_reg(self, symbol):
        path = f'/api/v4/public/orderbook/{symbol}'
        params = {'limit': 10}  # Adjusting parameters as per WhiteBit's documentation
        path += self._create_uri(params)
        resp = self.session.get(url=self.BASE_URL + path)
        ob = resp.json()
        # Check if the response is a dictionary and has 'asks' and 'bids' directly within it
        if isinstance(ob, dict) and 'asks' in ob and 'bids' in ob:
            orderbook = {
                'asks': [[float(ask[0]), float(ask[1])] for ask in ob['asks']],
                'bids': [[float(bid[0]), float(bid[1])] for bid in ob['bids']]
            }
            return orderbook

    @try_exc_regular
    def get_signature(self, data: dict):
        data_json = json.dumps(data, separators=(',', ':'))  # use separators param for deleting spaces
        payload = base64.b64encode(data_json.encode('ascii'))
        signature = hmac.new(self.api_secret.encode('ascii'), payload, hashlib.sha512).hexdigest()
        return signature, payload

    @staticmethod
    @try_exc_regular
    def _create_uri(params) -> str:
        data = ''
        strl = []
        for key in sorted(params):
            strl.append(f'{key}={params[key]}')
        data += '&'.join(strl)
        return f'?{data}'.replace(' ', '%20')

    @try_exc_regular
    def _run_ws_forever(self, loop):
        while True:
            loop.run_until_complete(self._run_ws_loop(loop))

    @try_exc_regular
    def run_updater(self):
        _wst_ = threading.Thread(target=self._run_ws_forever, args=[asyncio.new_event_loop()])
        _wst_.daemon = True
        _wst_.start()
        while set(self.orderbook) < set([self.markets[x] for x in self.markets_list if self.markets.get(x)]):
            time.sleep(0.01)
        if self.state == 'Bot':
            orders_thread = threading.Thread(target=self.deals_thread_func, args=[self.order_loop])
            orders_thread.daemon = True
            orders_thread.start()
            self.first_positions_update()

    @try_exc_regular
    def get_orders(self):
        return self.orders

    @try_exc_regular
    def get_balance(self):
        if not self.balance.get('total'):
            self.get_real_balance()
        tot_unrealised_pnl = sum([x['unrealised_pnl'] for x in self.positions.values()])
        return self.balance['total'] + tot_unrealised_pnl

    @try_exc_async
    async def _run_ws_loop(self, loop):
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(self.PUBLIC_WS_ENDPOINT) as ws:
                self._ws = ws
                if self.state == 'Bot':
                    await loop.create_task(self.subscribe_privates())
                for symbol in self.markets_list:
                    if market := self.markets.get(symbol):
                        await loop.create_task(self.subscribe_orderbooks(market))
                loop.create_task(self._ping(ws))
                async for msg in ws:
                    if self.stop_all:
                        await asyncio.sleep(0.0015)
                        self.stop_all = False
                    await self.process_ws_msg(msg)
                await ws.close()

    @try_exc_async
    async def process_ws_msg(self, msg: aiohttp.WSMessage):
        data = json.loads(msg.data)
        if data.get('method') == 'depth_update':
            if data['params'][0]:
                await self.update_orderbook_snapshot(data)
            else:
                await self.update_orderbook(data)
        elif data.get('method') == 'balanceMargin_update':
            await self.update_balances(data)
        elif data.get('method') in ['ordersExecuted_update', 'ordersPending_update']:
            await self.update_orders(data)
            print(data, datetime.utcnow())

    @try_exc_async
    async def _ping(self, ws):
        while True:
            await asyncio.sleep(25)  # Adjust the ping interval as needed
            await ws.ping()

    @try_exc_async
    async def update_balances(self, data):
        # print('BALANCES', data)
        self.balance = {'timestamp': datetime.utcnow().timestamp(),
                        'total': float(data['params'][0]['B']),
                        'free': float(data['params'][0]['av'])}
        # example = {'method': 'balanceMargin_update',
        #            'params': [{'a': 'USDT', 'B': '50', 'b': '0', 'av': '41.4684437', 'ab': '41.4684437'}], 'id': None}

    @try_exc_async
    async def update_orders(self, data):
        loop = asyncio.get_event_loop()
        status_id = 0
        stored = []
        for order in data['params']:
            if isinstance(order, int):
                status_id = order
                continue
            if order['deal_stock'] != '0':
                factual_price = float(order['deal_money']) / float(order['deal_stock'])
                coin = order['market'].split('_')[0]
                stored = self.multibot.open_orders.get(coin + '-' + self.EXCHANGE_NAME, [])
                if 'maker' in order.get('client_order_id', '') or order['id'] in stored:
                    if self.multibot.mm_exchange == self.EXCHANGE_NAME:
                        own_ts = time.time()
                        deal = {'side': 'sell' if order['side'] == 1 else 'buy',
                                'size': float(order['deal_stock']),
                                'coin': coin,
                                'price': factual_price,
                                'timestamp': order['mtime'],
                                'ts_ms': own_ts,
                                'order_id': order['id']}
                        loop.create_task(self.multibot.hedge_maker_position(deal))
                # self.get_position()
            else:
                factual_price = 0
            result = {'exchange_order_id': order['id'],
                      'exchange_name': self.EXCHANGE_NAME,
                      'status': self.get_order_status(order, status_id),
                      'factual_price': factual_price,
                      'factual_amount_coin': float(order['deal_stock']),
                      'factual_amount_usd': float(order['deal_money']),
                      'datetime_update': datetime.utcnow(),
                      'ts_update': order['mtime']}
            self.orders.update({order['id']: result})
        loop.create_task(self.multibot.update_all_av_balances())
        print(f'ORDERS UPDATE {self.EXCHANGE_NAME} {datetime.utcnow()}', data)
        if stored:
            print(f"STORED ORDER: {stored}")
        print()

        # orders_update = {'method': 'ordersExecuted_update',
        #                  'params': [{'id': 424298129261, 'market': 'DOGE_PERP', 'type': 7,
        #                              'side': 2, 'post_only': False, 'ioc': False,
        #                              'ctime': 1706050928.250344, 'mtime': 1706050929.606137,
        #                              'price': '0.077767', 'amount': '800', 'taker_fee':
        #                                  '0.00035', 'maker_fee': '0.0001', 'left': '0',
        #                              'deal_stock': '800', 'deal_money': '62.2136',
        #                              'deal_fee': '0.00622136', 'client_order_id': ''}], 'id': None}
        # example = {'method': 'ordersExecuted_update', 'params': [
        #     {'id': 395248275015, 'market': 'BTC_PERP', 'type': 7, 'side': 2, 'post_only': False, 'ioc': False,
        #      'ctime': 1703664697.619855, 'mtime': 1703664697.619855, 'price': '42511.7', 'amount': '0.001',
        #      'taker_fee': '0.00035', 'maker_fee': '0.0001', 'left': '0', 'deal_stock': '0.001', 'deal_money': '42.509',
        #      'deal_fee': '0.01487815', 'client_order_id': ''}], 'id': None}

    @try_exc_regular
    def get_order_status(self, order, status_id):
        # api_statuses = {1: 'new_order',
        #                 2: 'update_order',
        #                 3: 'finish(executed or canceled)'}
        if not order:
            return OrderStatus.NOT_EXECUTED
        key = 'deal_stock' if order.get('deal_stock') else 'dealStock'
        rest = float(order['left']) if order.get('left') else float(order['amount']) - float(order[key])
        if status_id:
            if status_id == 1 and order[key] == '0':
                return OrderStatus.PROCESSING
            if status_id == 3 and order[key] == '0':
                return OrderStatus.NOT_EXECUTED
        if rest == 0:
            return OrderStatus.FULLY_EXECUTED
        elif order[key] != '0':
            return OrderStatus.PARTIALLY_EXECUTED
        elif order[key] == '0':
            return OrderStatus.NOT_EXECUTED

    @try_exc_regular
    def fit_sizes(self, price, amount, symbol):
        # NECESSARY
        instr = self.instruments[symbol]
        tick_size = instr['tick_size']
        quantity_precision = instr['quantity_precision']
        price_precision = instr['price_precision']
        amount = round(amount, quantity_precision)
        rounded_price = round(price / tick_size) * tick_size
        price = round(rounded_price, price_precision)
        return price, amount

    @try_exc_regular
    def get_available_balance(self):
        return super().get_available_balance(
            leverage=self.leverage,
            max_pos_part=self.max_pos_part,
            positions=self.positions,
            balance=self.balance)

    @try_exc_regular
    def get_positions(self):
        return self.positions

    @try_exc_regular
    def get_order_by_id(self, symbol: str, order_id: int):
        time.sleep(0.5)
        path = '/api/v1/account/order_history'
        params = {'limit': 100}
        params = self.get_auth_for_request(params, path)
        path += self._create_uri(params)
        res = self.session.post(url=self.BASE_URL + path, json=params)
        response = res.json()
        right_order = {}
        factual_price = 0
        if response.get('success'):
            for market in response['result']:
                if not right_order:
                    for order in response['result'][market]:
                        if order['id'] == order_id:
                            right_order = order
                            if right_order['dealStock'] != '0':
                                factual_price = float(right_order['dealMoney']) / float(right_order['dealStock'])
                            break
                else:
                    break
        return {'exchange_order_id': order_id,
                'exchange_name': self.EXCHANGE_NAME,
                'status': self.get_order_status(right_order, 0),
                'factual_price': factual_price,
                'factual_amount_coin': float(right_order.get('dealStock', 0)),
                'factual_amount_usd': float(right_order.get('dealMoney', 0)),
                'datetime_update': datetime.utcnow(),
                'ts_update': right_order.get('ftime', datetime.utcnow().timestamp())}
        # example = {'success': True, 'message': '', 'result': {'BTC_PERP': [
        #     {'amount': '0.001', 'price': '43192.7', 'type': 'margin_limit', 'id': 395373055942, 'clientOrderId': '',
        #      'side': 'buy', 'ctime': 1703673670.631547, 'takerFee': '0.00035', 'ftime': 1703673672.240763,
        #      'makerFee': '0.0001', 'dealFee': '0', 'dealStock': '0', 'dealMoney': '0', 'status': 2,
        #      'marketName': 'BTC_PERP'},
        #     {'amount': '0.001', 'price': '43142.3', 'type': 'margin_limit', 'id': 395371130816, 'clientOrderId': '',
        #      'side': 'buy', 'ctime': 1703673540.805917, 'takerFee': '0.00035', 'ftime': 1703673542.389305,
        #      'makerFee': '0.0001', 'dealFee': '0', 'dealStock': '0', 'dealMoney': '0', 'status': 2,
        #      'marketName': 'BTC_PERP'}]}}

    @try_exc_async
    async def create_order(self, symbol, side, price, size, session, expire=10000, client_id=None):
        path = "/api/v4/order/collateral/limit"
        params = {"market": symbol,
                  "side": side,
                  "amount": size,
                  "price": price}
        params = self.get_auth_for_request(params, path)
        path += self._create_uri(params)
        async with session.post(url=self.BASE_URL + path, headers=self.session.headers, json=params) as resp:
            try:
                response = await resp.json()
            except ContentTypeError as e:
                content = await resp.text()
                print(f"{self.EXCHANGE_NAME} CREATE ORDER ERROR\nAPI RESPONSE: {content}")
            print(f"{self.EXCHANGE_NAME} ORDER CREATE RESPONSE: {response}")
            self.update_order_after_deal(response)
            status = self.get_order_response_status(response)
            self.LAST_ORDER_ID = response.get('orderId', 'default')
            return {'exchange_name': self.EXCHANGE_NAME,
                    'exchange_order_id': response.get('orderId'),
                    'timestamp': response.get('timestamp', time.time()),
                    'status': status}

    @try_exc_async
    async def create_fast_order(self, price, sz, side, market, client_id=None, session=None):
        time_start = time.time()
        # message = f'ORDER CREATE ERROR\n'
        # message += f"{time_start=}\n"
        # message += f"{self.requests_counter=}\n"
        # message += f"{time_start - self.request_timer=}\n"
        # message += f"{self.total_requests=}\n"
        # message += f"{time_start - self.total_start_time=}\n"
        path = "/api/v4/order/collateral/limit"
        body = {"market": market,
                "side": side,
                "amount": sz,
                "price": price}
        body = self.get_auth_for_request(body, path)
        path += self._create_uri(body)
        async with self.async_session.post(url=self.BASE_URL + path, headers=self.session.headers, json=body) as resp:
            try:
                response = await resp.json()
                if self.EXCHANGE_NAME != self.multibot.mm_exchange:
                    if not client_id or 'taker' in client_id:
                        print(f"{self.EXCHANGE_NAME} ORDER CREATE RESPONSE: {response}")
                        print(f"{self.EXCHANGE_NAME} ORDER CREATE PING: {response['timestamp'] - time_start}")
                        self.update_order_after_deal(response)
                if response.get('code'):
                    print(f"CREATE ORDER RESPONSE: {body=}")
                    print(response)
                    return
                price = float(response['dealMoney']) / float(response['dealStock']) if response[
                                                                                           'dealStock'] != '0' else 0
                order_res = {'exchange_name': self.EXCHANGE_NAME,
                             'exchange_order_id': response.get('orderId'),
                             'timestamp': response.get('timestamp', time.time()),
                             'status': self.get_order_response_status(response),
                             'api_response': response,
                             'size': float(response['dealStock']),
                             'price': price,
                             'time_order_sent': time_start,
                             'create_order_time': response['timestamp'] - time_start}
                if client_id:
                    self.responses.update({client_id: order_res})
                else:
                    self.responses.update({response['orderId']: order_res})
            except:
                # if self.EXCHANGE_NAME != self.multibot.mm_exchange:
                # print(message)
                # print(f"{self.EXCHANGE_NAME} ORDER CREATE FAILURE\nBODY: {body}\nRESP: {resp.text}")
                return

            # example_executed = {'orderId': 395248275015, 'clientOrderId': '', 'market': 'BTC_PERP', 'side': 'buy',
            # 'type': 'margin limit',
            #  'timestamp': 1703664697.619855, 'dealMoney': '42.509', 'dealStock': '0.001', 'amount': '0.001',
            #  'takerFee': '0.00035', 'makerFee': '0.0001', 'left': '0', 'dealFee': '0.01487815', 'ioc': False,
            #  'postOnly': False, 'price': '42511.7'}
            # example_failed = {'code': 17, 'message': 'Inner validation failed',
            #                   'errors': {'amount': ['Not enough balance.']}}

    @try_exc_regular
    def update_order_after_deal(self, resp):
        factual_price = 0 if resp.get('dealStock', '0') == '0' else float(resp['dealMoney']) / float(resp['dealStock'])
        self.orders.update({resp.get('orderId'): {'exchange_order_id': resp.get('orderId', 'default'),
                                                  'exchange_name': self.EXCHANGE_NAME,
                                                  'status': self.get_order_status(resp, 0),
                                                  'factual_price': factual_price,
                                                  'factual_amount_coin': float(resp.get('dealStock', 0)),
                                                  'factual_amount_usd': float(resp.get('dealMoney', 0)),
                                                  'datetime_update': datetime.utcnow(),
                                                  'ts_update': datetime.utcnow().timestamp()}})

    @try_exc_regular
    def get_order_response_status(self, response):
        if response.get('left'):
            return ResponseStatus.SUCCESS
        else:
            self.error_info = response
            return ResponseStatus.ERROR

    @try_exc_async
    async def subscribe_orderbooks(self, symbol):
        data = [symbol, 100, '0', True]
        method = {"id": 0,
                  "method": "depth_subscribe",
                  "params": data}
        await self._ws.send_json(method)

    @try_exc_regular
    def get_ws_token(self):
        path = "/api/v4/profile/websocket_token"
        params = self.get_auth_for_request({}, path)
        path += self._create_uri(params)
        res = self.session.post(url=self.BASE_URL + path, json=params).json()
        return res['websocket_token']

    @try_exc_async
    async def subscribe_privates(self):
        method_auth = {"id": 1, "method": "authorize", "params": [self.websocket_token, "public"]}
        orders_ex = {"id": 2, "method": "ordersExecuted_subscribe", "params": [list(self.markets.values()), 0]}
        balance = {"id": 3, "method": "balanceMargin_subscribe", "params": ["USDT"]}
        await self._ws.send_json(method_auth)
        time.sleep(1)
        await self._ws.send_json(orders_ex)
        await self._ws.send_json(balance)

    @try_exc_regular
    def get_all_tops(self):
        tops = {}
        for coin, symbol in self.markets.items():
            orderbook = self.get_orderbook(symbol)
            if orderbook and orderbook.get('bids') and orderbook.get('asks'):
                tops.update({self.EXCHANGE_NAME + '__' + coin: {
                    'top_bid': orderbook['bids'][0][0], 'top_ask': orderbook['asks'][0][0],
                    'bid_vol': orderbook['bids'][0][1], 'ask_vol': orderbook['asks'][0][1],
                    'ts_exchange': orderbook['timestamp']}})
        return tops

    @try_exc_async
    async def update_orderbook(self, data):
        side = None
        # flag_market = False
        symbol = data['params'][2]
        new_ob = self.orderbook[symbol].copy()
        ts_ms = time.time()
        new_ob['ts_ms'] = ts_ms
        ts_ob = data['params'][1]['timestamp']
        new_ob['timestamp'] = ts_ob
        for new_bid in data['params'][1].get('bids', []):
            if float(new_bid[0]) >= new_ob['top_bid'][0]:
                new_ob['top_bid'] = [float(new_bid[0]), float(new_bid[1])]
                new_ob['top_bid_ts'] = ts_ob
                # flag_market = True
                side = 'sell'
            if new_ob['bids'].get(new_bid[0]) and new_bid[1] == '0':
                del new_ob['bids'][new_bid[0]]
                if float(new_bid[0]) == new_ob['top_bid'][0] and len(new_ob['bids']):
                    top = sorted(new_ob['bids'])[-1]
                    new_ob['top_bid'] = [float(top), float(new_ob['bids'][top])]
                    new_ob['top_bid_ts'] = ts_ob
                    # flag_market = True
                    # side = 'sell'
            elif new_bid[1] != '0':
                new_ob['bids'][new_bid[0]] = new_bid[1]
        for new_ask in data['params'][1].get('asks', []):
            if float(new_ask[0]) <= new_ob['top_ask'][0]:
                new_ob['top_ask'] = [float(new_ask[0]), float(new_ask[1])]
                new_ob['top_ask_ts'] = ts_ob
                # flag_market = True
                side = 'buy'
            if new_ob['asks'].get(new_ask[0]) and new_ask[1] == '0':
                del new_ob['asks'][new_ask[0]]
                if float(new_ask[0]) == new_ob['top_ask'][0] and len(new_ob['asks']):
                    top = sorted(new_ob['asks'])[0]
                    new_ob['top_ask'] = [float(top), float(new_ob['asks'][top])]
                    new_ob['top_ask_ts'] = ts_ob
                    # flag_market = True
                    # side = 'buy'
            elif new_ask[1] != '0':
                new_ob['asks'][new_ask[0]] = new_ask[1]
        if new_ob['top_ask'][0] <= new_ob['top_bid'][0]:
            new_ob = self.cut_extra_orders_from_ob(symbol, data, new_ob)
        self.orderbook[symbol] = new_ob
        # if self.market_finder and flag_market:
        #     # if self.multibot.mm_exchange == self.EXCHANGE_NAME:
        #     #     if ts_ms - ts_ob < 0.035:
        #     #         await self.market_finder.count_one_coin(symbol.split('_')[0], self.EXCHANGE_NAME)
        #     # else:
        #     #     if ts_ms - ts_ob < 0.120:
        #     await self.market_finder.count_one_coin(symbol.split('_')[0], self.EXCHANGE_NAME)
        if side and self.finder and ts_ms - ts_ob < self.top_ws_ping:
            coin = symbol.split('_')[0]
            await self.finder.count_one_coin(coin, self.EXCHANGE_NAME, side, 'ob')

    @try_exc_regular
    def cut_extra_orders_from_ob(self, symbol, data, new_ob):
        if new_ob['top_ask_ts'] < new_ob['top_bid_ts']:
            top_ask = [999999999, 0]
            new_asks = {}
            for new_ask in data['params'][1].get('asks', []):
                if new_ask[1] != '0':
                    new_asks[new_ask[0]] = new_ask[1]
                    if top_ask:
                        if float(new_ask[0]) < top_ask[0]:
                            top_ask = [float(new_ask[0]), float(new_ask[1])]
                    else:
                        top_ask = [float(new_ask[0]), float(new_ask[1])]
            new_ob.update({'asks': new_asks,
                           'top_ask': top_ask,
                           'top_ask_ts': data['params'][1]['timestamp']})
        else:
            top_bid = [0, 0]
            new_bids = {}
            for new_bid in data['params'][1].get('bids', []):
                if new_bid[1] != '0':
                    new_bids[new_bid[0]] = new_bid[1]
                    if top_bid:
                        if float(new_bid[0]) > top_bid[0]:
                            top_bid = [float(new_bid[0]), float(new_bid[1])]
                    else:
                        top_bid = [float(new_bid[0]), float(new_bid[1])]
            new_ob.update({'bids': new_bids,
                           'top_bid': top_bid,
                           'top_bid_ts': data['params'][1]['timestamp']})
        return new_ob

    @try_exc_async
    async def update_orderbook_snapshot(self, data):
        symbol = data['params'][2]
        ob = data['params'][1]
        if ob.get('asks') and ob.get('bids'):
            self.orderbook[symbol] = {'asks': {x[0]: x[1] for x in ob['asks']},
                                      'bids': {x[0]: x[1] for x in ob['bids']},
                                      'timestamp': data['params'][1]['timestamp'],
                                      'top_ask': [float(ob['asks'][0][0]), float(ob['asks'][0][1])],
                                      'top_bid': [float(ob['bids'][0][0]), float(ob['bids'][0][1])],
                                      'top_ask_ts': data['params'][1]['timestamp'],
                                      'top_bid_ts': data['params'][1]['timestamp'],
                                      'ts_ms': time.time()}

    @try_exc_regular
    def get_orderbook(self, symbol, necessary=False) -> dict:
        if not self.orderbook.get(symbol):
            return {}
        if necessary:
            orderbook = None
            while not orderbook:
                orderbook = self.get_orderbook(symbol)
            return orderbook
        snap = self.orderbook[symbol].copy()
        if isinstance(snap['asks'], list):
            return snap
        if snap['top_ask'][0] <= snap['top_bid'][0]:
            return {}
        ob = {'timestamp': snap['timestamp'],
              'asks': sorted([[float(x), float(y)] for x, y in snap['asks'].copy().items()])[:self.ob_len],
              'bids': sorted([[float(x), float(y)] for x, y in snap['bids'].copy().items()])[::-1][:self.ob_len],
              'top_ask_ts': snap['top_ask_ts'],
              'top_bid_ts': snap['top_bid_ts'],
              'ts_ms': snap['ts_ms']}
        return ob

    @try_exc_regular
    def first_positions_update(self):
        while set(self.orderbook.keys()) < set([self.markets[x] for x in self.markets_list if self.markets.get(x)]):
            time.sleep(0.01)
            print('WHITEBIT.GOT ALL MARKETS')
            self.get_position()


if __name__ == '__main__':
    import configparser

    config = configparser.ConfigParser()
    config.read('config.ini', "utf-8")
    client = WhiteBitClient(keys=config['WHITEBIT'],
                            leverage=float(config['SETTINGS']['LEVERAGE']),
                            max_pos_part=int(config['SETTINGS']['PERCENT_PER_MARKET']),
                            markets_list=['EOS'])


    async def test_order():
        async with aiohttp.ClientSession() as session:
            ob = client.get_orderbook('EOS_PERP')
            client.amount = client.instruments['EOS_PERP']['min_size']
            client.price = ob['bids'][4][0] - (client.instruments['EOS_PERP']['tick_size'] * 100)
            await client.create_order('EOS_PERP', 'buy', session)
            time.sleep(1)
            client.cancel_all_orders()


    client.markets_list = list(client.markets.keys())
    client.run_updater()

    while True:
        time.sleep(1)
        asyncio.run(test_order())
