import time
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


class WhiteBitClient(BaseClient):
    PUBLIC_WS_ENDPOINT = 'wss://api.whitebit.com/ws'
    BASE_URL = 'https://whitebit.com'
    EXCHANGE_NAME = 'WHITEBIT'
    headers = {"Accept": "application/json;charset=UTF-8",
               "Content-Type": "application/json",
               'User-Agent': 'python-whitebit-sdk',
               'Connection': 'keep-alive'}

    def __init__(self, multibot=None, keys=None, leverage=None, state='Bot',
                 markets_list=[], max_pos_part=20, finder=None, ob_len=5):
        super().__init__()
        self.multibot = multibot
        self.state = state
        self.finder = finder
        self.markets_list = markets_list
        self.session = requests.session()
        self.session.headers.update(self.headers)
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
        self.price = 0
        self.amount = 0
        self.side = None
        self.symbol = None
        self.error_info = None
        self.loop = asyncio.new_event_loop()
        self._connected = asyncio.Event()
        self._wst_ = threading.Thread(target=self._run_ws_forever)
        self.order_loop = asyncio.new_event_loop()
        self.orders_thread = threading.Thread(target=self.deals_thread_func)
        self.last_price = {}
        self.LAST_ORDER_ID = 'default'
        self.taker_fee = float(keys['TAKER_FEE']) * 0.6
        self.subs = {}
        self.ob_push_limit = 0.1
        self.last_keep_alive = 0
        self.deal = False
        self.response = None
        self.side = 'buy'
        self.last_symbol = None

    @try_exc_regular
    def deals_thread_func(self):
        while True:
            self.order_loop.run_until_complete(self._run_order_loop())
            # await self.cancel_all_tasks(self.order_loop)
            # self.order_loop.stop()
        # print(f"Thread {market} started")

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
    async def _run_order_loop(self):
        async with aiohttp.ClientSession() as self.async_session:
            self.async_session.headers.update(self.headers)
            while True:
                if self.deal:
                    # print(f"{self.EXCHANGE_NAME} GOT DEAL {time.time()}")
                    order = await self.create_fast_order(self.symbol, self.side)
                    self.response = order
                    self.last_keep_alive = order['timestamp']
                    self.deal = False
                else:
                    ts_ms = time.time()
                    if ts_ms - self.last_keep_alive > 5:
                        self.last_keep_alive = ts_ms
                        self.order_loop.create_task(self.get_position_async())
                        # print(f"keep-alive {self.EXCHANGE_NAME} time: {time.time() - ts_ms}")

                        # path = self.BASE_URL + "/api/v4/order/"
                        # async with self.async_session.head(path) as response:
                        #     print(f"Keep alive status {self.EXCHANGE_NAME}: {response.status}")
                        #     print(f"{response.text}")

                        # if not self.last_symbol:
                        #     self.last_symbol = self.markets[self.markets_list[0]]
                        # self.amount = self.instruments[self.last_symbol]['min_size']
                        # self.fit_sizes(self.orderbook[self.last_symbol]['top_bid'][0] * 0.9, self.last_symbol)
                        # self.side = 'buy'
                        # order = await self.create_fast_order(self.last_symbol, self.side)
                        # print(f"Create {self.EXCHANGE_NAME} keep-alive order time: {order['timestamp'] - ts_ms}")
                        # self.LAST_ORDER_ID = 'default'
                        # await self.cancel_order(self.last_symbol, order['exchange_order_id'], self.async_session)
                await asyncio.sleep(0.0001)

                # params = {"market": symbol,
                #           "side": side,
                #           "amount": self.amount,
                #           "price": self.price}
                # print(f"{self.EXCHANGE_NAME} SENDING ORDER: {params}")
                # params = self.get_auth_for_request(params, path)
                # path += self._create_uri(params)
                # async session.post()

    # @try_exc_regular
    # def run_loopie_loop(self, loop):
    #     while True:
    #         time.sleep(1)
    #         loop.run_until_complete(self._run_deals_ws_loop())

    # @try_exc_async
    # async def get_ws_executed_deals(self, market, websocket):
    #     id = randint(1, 10000000000000)
    #     method = {"id": id,
    #               "method": "ordersExecuted_request",
    #               "params": {"market": market,
    #                          "order_types": [1, 2]}}
    #     await websocket.send_json(method)
    #     self.subs.update({id: market})
    # @try_exc_async
    # async def _run_deals_ws_loop(self):
    #     async with aiohttp.ClientSession() as s:
    #         while True:
    #             if not self.market_to_check:
    #                 await asyncio.sleep(0.01)
    #                 continue
    #             await asyncio.sleep(1)
    #             async with s.ws_connect(self.PUBLIC_WS_ENDPOINT) as ws:
    #                 method_auth = {"id": 303, "method": "authorize", "params": [self.websocket_token, "public"]}
    #                 await ws.send_json(method_auth)
    #                 await ws.receive_json()
    #                 market = self.market_to_check
    #                 self.market_to_check = None
    #                 method = {"id": 101,
    #                           "method": "deals_request",
    #                           "params": [market, 0, 100]}
    #                 await ws.send_json(method)
    #                 resp = await ws.receive_json()
    #                 if not resp['error']:
    #                     self.update_own_orders(resp['result']['records'])
    #             await ws.close()

    # data = {"error": None, "result": {"offset": 0, "limit": 100, "records": [
                #     {"time": 1704523361.6664, "id": 3386587209, "side": 1, "role": 2, "price": "0.16806",
                #      "amount": "80", "deal": "13.4448", "fee": "0.00470568", "order_id": 406582635829,
                #      "deal_order_id": 406582569838, "market": "GRT_PERP", "client_order_id": ""},
                #     {"time": 1704523285.8136549, "id": 3386582439, "side": 2, "role": 2, "price": "0.16852",
                #      "amount": "170", "deal": "28.6484", "fee": "0.01002694", "order_id": 406581486894,
                #      "deal_order_id": 406581480011, "market": "GRT_PERP", "client_order_id": ""},
                #     {"time": 1704520372.1594241, "id": 3386386367, "side": 1, "role": 2, "price": "0.17101",
                #      "amount": "90", "deal": "15.3909", "fee": "0.005386815", "order_id": 406535978735,
                #      "deal_order_id": 406535915525, "market": "GRT_PERP", "client_order_id": ""}]}}

    # @try_exc_regular
    # def update_own_orders(self, data):
    #     temp = {}
    #     for deal in data:
    #         if exist_deal := temp.get(deal['deal_order_id']):
    #             tot_amnt_usd = exist_deal['factual_amount_usd'] + float(deal["deal"])
    #             tot_amnt_coin = exist_deal['factual_amount_coin'] + float(deal['amount'])
    #             av_price = tot_amnt_usd / tot_amnt_coin
    #             ts_update = deal["time"]
    #             dt_update = datetime.fromtimestamp(ts_update)
    #             fills = exist_deal['fills'] + 1
    #         else:
    #             tot_amnt_usd = float(deal["deal"])
    #             tot_amnt_coin = float(deal['amount'])
    #             av_price = float(deal['price'])
    #             ts_update = deal["time"]
    #             dt_update = datetime.fromtimestamp(ts_update)
    #             fills = 1
    #         temp.update({deal['deal_order_id']: {'exchange_order_id': deal['deal_order_id'],
    #                                              'exchange': self.EXCHANGE_NAME,
    #                                              'status': OrderStatus.FULLY_EXECUTED,
    #                                              'factual_price': av_price,
    #                                              'factual_amount_coin': tot_amnt_coin,
    #                                              'factual_amount_usd': tot_amnt_usd,
    #                                              'datetime_update': dt_update,
    #                                              'ts_update': ts_update,
    #                                              'fills': fills}})
    #     for deal_to_update in temp.keys():
    #         if self.orders.get(deal_to_update):
    #             self.orders.update(temp[deal_to_update])
    #     self.own_orders.update(temp)

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
    async def get_position_async(self):
        path = "/api/v4/collateral-account/positions/open"
        params = self.get_auth_for_request({}, path)
        path += self._create_uri(params)
        async with self.async_session.post(url=self.BASE_URL + path, json=params, headers=self.session.headers) as res:
            response = await res.json()
            # print(f'GET_POSITION RESPONSE {self.EXCHANGE_NAME}', response)
            self.positions = {}
            for pos in response:
                if isinstance(pos, str):
                    print(f"{self.EXCHANGE_NAME} position update in get_position mistake {response}")
                    continue
                market = pos['market']
                ob = self.get_orderbook(market)
                change = (ob['asks'][0][0] + ob['bids'][0][0]) / 2
                self.positions.update({market: {'timestamp': int(datetime.utcnow().timestamp()),
                                                'entry_price': float(pos['basePrice']) if pos['basePrice'] else 0,
                                                'amount': float(pos['amount']),
                                                'amount_usd': change * float(pos['amount'])}})

    @try_exc_regular
    def get_position(self):
        path = "/api/v4/collateral-account/positions/open"
        params = self.get_auth_for_request({}, path)
        path += self._create_uri(params)
        res = self.session.post(url=self.BASE_URL + path, json=params)
        response = res.json()
        # print('GET_POSITION RESPONSE', response)
        self.positions = {}
        for pos in response:
            if isinstance(pos, str):
                print(f"{self.EXCHANGE_NAME} position update in get_position mistake {response}")
                continue
            market = pos['market']
            ob = self.get_orderbook(market)
            if not ob:
                ob = self.get_orderbook_http_reg(market)
            change = (ob['asks'][0][0] + ob['bids'][0][0]) / 2
            self.positions.update({market: {'timestamp': int(datetime.utcnow().timestamp()),
                                            'entry_price': float(pos['basePrice']) if pos['basePrice'] else 0,
                                            'amount': float(pos['amount']),
                                            'amount_usd': change * float(pos['amount'])}})

        # print(self.EXCHANGE_NAME, 'POSITIONS AFTER UPDATE:', self.positions)

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
        # print('GET_REAL_BALANCE RESPONSE', response)
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
                        'top_ask_timestamp': datetime.utcnow().timestamp(),
                        'top_bid_timestamp': datetime.utcnow().timestamp()}
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

    # ### SUPERSONIC FEATURE ###
    # @try_exc_async
    # async def super_sonic_ob_update(self, market, coin, session):
    #     path = f'/api/v4/public/orderbook/{market}'
    #     params = {'limit': 1}  # Adjusting parameters as per WhiteBit's documentation
    #     path += self._create_uri(params)
    #     async with session.get(url=self.BASE_URL + path) as resp:
    #         ob = await resp.json()
    #         # print(ob)
    #         # Check if the response is a dictionary and has 'asks' and 'bids' directly within it
    #         if isinstance(ob, dict) and 'asks' in ob and 'bids' in ob:
    #             ask = ob['asks'][0]
    #             bid = ob['bids'][0]
    #             ts = ob['timestamp']
    #             old_top_ask = self.orderbook[market]
    #             old_top_bid = self.orderbook[market]
    #             top_ask_timestamp = ts if old_top_ask != ask else self.orderbook[market]['top_ask_timestamp']
    #             top_bid_timestamp = ts if old_top_bid != bid else self.orderbook[market]['top_bid_timestamp']
    #             orderbook = {
    #                 'asks': {ask[0]: ask[1]},
    #                 'bids': {bid[0]: bid[1]},
    #                 'top_ask': [float(ask[0]), float(ask[1])],
    #                 'top_bid': [float(bid[0]), float(bid[1])],
    #                 'timestamp': ts,
    #                 'top_ask_timestamp': top_ask_timestamp,
    #                 'top_bid_timestamp': top_bid_timestamp}
    #             # print(f"SuperSonic time: {time.time() - ts} sec")
    #             self.orderbook[market] = orderbook
    #             if top_bid_timestamp != ts or top_ask_timestamp != ts:
    #                 self.finder.coins_to_check.append(coin)
    #                 self.finder.update = True
    #
    # @try_exc_regular
    # def find_best_market(self):
    #     tradable_markets = self.finder.tradable_profits.copy()
    #     min_profit_gap = 100
    #     target_coin = None
    #     for coin, exchanges in tradable_markets.items():
    #         for side, profit in exchanges.items():
    #             if self.EXCHANGE_NAME in side:
    #                 if profit < min_profit_gap:
    #                     min_profit_gap = exchanges[self.EXCHANGE_NAME + 'BUY']
    #                     target_coin = coin
    #     return self.markets.get(target_coin), target_coin
    #
    # @try_exc_regular
    # def run_super_sonic(self):
    #     self.loop_supersonic.run_until_complete(self._request_orderbooks())
    #
    # @try_exc_async
    # async def _request_orderbooks(self):
    #     print('SUPERSONIC INITIALIZED')
    #     while not self.finder:
    #         time.sleep(1)
    #     async with aiohttp.ClientSession() as session:
    #         session.headers.update(self.headers)
    #         # count = 0
    #         # time_start = time.time()
    #         while True:
    #             # time.sleep(0.005)
    #             # print(f"SUPERSONIC GO!!!!")
    #             best_market, coin = self.find_best_market()
    #             if best_market:
    #                 await self.loop_supersonic.create_task(self.super_sonic_ob_update(best_market, coin, session))
    #                 # count += 1
    #                 # print(f"REAL REQUESTS FREQUENCY DATA:\nTIME: {time.time() - time_start}\nREQUESTS: {count}")
    # ### SUPERSONIC FEATURE ###
    @try_exc_regular
    def _run_ws_forever(self):
        while True:
            self.loop.run_until_complete(self._run_ws_loop(self.update_orders,
                                                            self.update_orderbook,
                                                            self.update_balances,
                                                            self.update_orderbook_snapshot))
            # await self.cancel_all_tasks(self.loop)
            # self.loop.stop()
    #
    # @try_exc_regular
    # def _process_ws_line(self):
    #     asyncio.run_coroutine_threadsafe(self.process_messages(), self.loop)
    #
    # @try_exc_async
    # async def process_messages(self):
    #     while True:
    #         msg = await self.message_queue.get()
    #         data = json.loads(msg.data)
    #         if data.get('method') == 'depth_update':
    #             # print(data)
    #             if data['params'][0]:
    #                 self.update_orderbook_snapshot(data)
    #             else:
    #                 self.update_orderbook(data)
    #         if self.state == 'Bot':
    #             if data.get('method') == 'balanceMargin_update':
    #                 self.update_balances(data)
    #             elif data.get('method') in ['ordersExecuted_update', 'ordersPending_update']:
    #                 self.update_orders(data)
    #         self.message_queue.task_done()
    #         # if self.message_queue.qsize() > 100:
    #         #     message = f'ALERT! {self.EXCHANGE_NAME} WS LINE LENGTH: {self.message_queue.qsize()}'
    #         #     self.telegram_bot.send_message(message, self.alert_id)

    @try_exc_regular
    def run_updater(self):
        self._wst_.daemon = True
        self._wst_.start()
        while set(self.orderbook) < set([self.markets[x] for x in self.markets_list if self.markets.get(x)]):
            # print(f'{self.EXCHANGE_NAME} waiting for full ob')
            time.sleep(0.01)
        if self.state == 'Bot':
            self.orders_thread.daemon = True
            self.orders_thread.start()
            self.first_positions_update()
        # self._wst_processing_messages.daemon = True
        # self._wst_processing_messages.start()
        # self.crazy_treading_func()
        # if self.finder:
        #     self._extra_speed.daemon = True
        #     self._extra_speed.start()


    @try_exc_regular
    def get_orders(self):
        # NECESSARY
        return self.orders

    @try_exc_regular
    def get_balance(self):
        if not self.balance.get('total'):
            self.get_real_balance()
        return self.balance['total']

    @try_exc_async
    async def _run_ws_loop(self, update_orders, update_orderbook, update_balances, update_orderbook_snapshot):
        async with httpx.AsyncClient(http2=True) as self.httpx_async_session:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(self.PUBLIC_WS_ENDPOINT) as ws:
                    self._connected.set()
                    self._ws = ws
                    # await self.loop.create_task(self.subscribe_privates())
                    if self.state == 'Bot':
                        # asyncio.run_coroutine_threadsafe(self.subscribe_privates(), self.loop)
                        await self.loop.create_task(self.subscribe_privates())
                    for symbol in self.markets_list:
                        if market := self.markets.get(symbol):
                            # asyncio.run_coroutine_threadsafe(self.subscribe_orderbooks(market), self.loop)
                            await self.loop.create_task(self.subscribe_orderbooks(market))
                    async for msg in ws:
                        data = json.loads(msg.data)
                        if data.get('method') == 'depth_update':
                            # print(data)
                            if data['params'][0]:
                                self.loop.create_task(update_orderbook_snapshot(data))
                            else:
                                self.loop.create_task(update_orderbook(data))
                        elif data.get('method') == 'balanceMargin_update':
                            self.loop.create_task(update_balances(data))
                        elif data.get('method') in ['ordersExecuted_update', 'ordersPending_update']:
                            self.loop.create_task(update_orders(data))
                    await ws.close()

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
        print(f'ORDERS UPDATE {self.EXCHANGE_NAME}', data)
        status_id = 0
        for order in data['params']:
            if isinstance(order, int):
                status_id = order
                continue
            if order['deal_stock'] != '0':
                factual_price = float(order['deal_money']) / float(order['deal_stock'])
                side = 'sell' if order['side'] == 1 else 'buy'
                self.last_price.update({side: factual_price})
                self.get_position()
            else:
                factual_price = 0
            result = {'exchange_order_id': order['id'],
                      'exchange': self.EXCHANGE_NAME,
                      'status': self.get_order_status(order, status_id),
                      'factual_price': factual_price,
                      'factual_amount_coin': float(order['deal_stock']),
                      'factual_amount_usd': float(order['deal_money']),
                      'datetime_update': datetime.utcnow(),
                      'ts_update': order['mtime']}
            self.orders.update({order['id']: result})
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
        rest = float(order['left']) if order.get('left') else float(order['amount']) - float(order['dealStock'])
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
    def fit_sizes(self, price, symbol):
        # NECESSARY
        instr = self.instruments[symbol]
        tick_size = instr['tick_size']
        quantity_precision = instr['quantity_precision']
        price_precision = instr['price_precision']
        self.amount = round(self.amount, quantity_precision)
        rounded_price = round(price / tick_size) * tick_size
        self.price = round(rounded_price, price_precision)

    @try_exc_regular
    def get_available_balance(self):
        return super().get_available_balance(
            leverage=self.leverage,
            max_pos_part=self.max_pos_part,
            positions=self.positions,
            balance=self.balance)

    @try_exc_regular
    def get_last_price(self, side):
        return self.last_price.get(side, 0)

    @try_exc_regular
    def get_positions(self):
        return self.positions

    @try_exc_regular
    def get_order_by_id(self, symbol: str, order_id: int):
        # self.market_to_check = symbol
        # time.sleep(1.2)
        # if order := self.own_orders.get(order_id):
        #     return order
        # else:
        #     return {'exchange_order_id': order_id,
        #             'exchange': self.EXCHANGE_NAME,
        #             'status': OrderStatus.NOT_EXECUTED,
        #             'factual_price': 0,
        #             'factual_amount_coin': 0,
        #             'factual_amount_usd': 0,
        #             'datetime_update': datetime.utcnow(),
        #             'ts_update': datetime.utcnow().timestamp()}
        time.sleep(0.5)
        path = '/api/v1/account/order_history'
        params = {'limit': 100}
        params = self.get_auth_for_request(params, path)
        path += self._create_uri(params)
        res = self.session.post(url=self.BASE_URL + path, json=params)
        response = res.json()
        # print(self.EXCHANGE_NAME, 'GET_ORDER_BY_ID STARTED')
        # print(self.EXCHANGE_NAME, 'GET_ORDER_BY_ID RESPONSE', response)
        right_order = {}
        factual_price = 0
        if response.get('success'):
            for market in response['result']:
                if not right_order:
                    for order in response['result'][market]:
                        if order['id'] == order_id:
                            right_order = order
                            # print(f"GET_ORDER_BY_ID ORDER FOUND: {right_order}")
                            if right_order['dealStock'] != '0':
                                factual_price = float(right_order['dealMoney']) / float(right_order['dealStock'])
                            break
                else:
                    break
        return {'exchange_order_id': order_id,
                'exchange': self.EXCHANGE_NAME,
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
    async def create_order(self, symbol, side, session, expire=10000, client_id=None):
        path = "/api/v4/order/collateral/limit"
        params = {"market": symbol,
                  "side": side,
                  "amount": self.amount,
                  "price": self.price}
        params = self.get_auth_for_request(params, path)
        path += self._create_uri(params)
        resp = session.post(url=self.BASE_URL + path, headers=self.session.headers, params=params)
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
    async def create_fast_order(self, symbol, side, expire=10000, client_id=None, mode='Order'):
        # time_start = time.time()
        path = "/api/v4/order/collateral/limit"
        params = {"market": symbol,
                  "side": side,
                  "amount": self.amount,
                  "price": self.price}
        # print(f"{self.EXCHANGE_NAME} SENDING ORDER: {params}")
        # if client_id:
        #     params['clientOrderId'] = client_id
        params = self.get_auth_for_request(params, path)
        path += self._create_uri(params)
        # async with aiohttp.ClientSession() as session:
        async with self.async_session.post(url=self.BASE_URL + path, headers=self.session.headers, json=params) as resp:
            try:
                response = await resp.json()
            except ContentTypeError as e:
                content = await resp.text()
                print(f"{self.EXCHANGE_NAME} CREATE ORDER ERROR\nAPI RESPONSE: {content}")
            # resp = self.session.post(url=self.BASE_URL + path, json=params)
            # response = resp.json()
            # print(resp.headers)
            status = 'KEEP-ALIVE'
            if self.deal:
                print(f"{self.EXCHANGE_NAME} ORDER CREATE RESPONSE: {response}")
                self.update_order_after_deal(response)
                status = self.get_order_response_status(response)
                self.LAST_ORDER_ID = response.get('orderId', 'default')
            # print(f"ORDER PLACING TIME: {time.time() - time_start}")
            # self.aver_time.append(time.time() - time_start)
            # self.aver_time_response.append(response['timestamp'] - time_start)
            # self.aver_time.append(time.time() - time_start)
            return {'exchange_name': self.EXCHANGE_NAME,
                    'exchange_order_id': response.get('orderId'),
                    'timestamp': response.get('timestamp', time.time()),
                    'status': status}
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
                                                  'exchange': self.EXCHANGE_NAME,
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
            # status = ResponseStatus.NO_CONNECTION

    @try_exc_async
    async def subscribe_orderbooks(self, symbol):
        data = [symbol, 100, '0', True]
        method = {"id": 0,
                  "method": "depth_subscribe",
                  "params": data}
        await self._connected.wait()
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
        # orders_pend = {"id": 3, "method": "ordersPending_subscribe", "params": []}
        balance = {"id": 3, "method": "balanceMargin_subscribe", "params": ["USDT"]}
        # deals = {"id": 4, "method": "deals_subscribe", "params": list(self.markets.values())}
        await self._connected.wait()
        await self._ws.send_json(method_auth)
        time.sleep(1)
        # await self._ws.send_json(deals)
        # for market in list(self.markets.values()):
        #     id = randint(1, 1000)
        #     # print(f"SUBSCRIPTION: {id}: {market}")
        #     orders_pend.update({"id": id, 'params': [market]})
        #     await self._ws.send_json(orders_pend)
        #     await asyncio.sleep(0.2)
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
        flag = False
        symbol = data['params'][2]
        self.last_symbol = symbol
        new_ob = self.orderbook[symbol].copy()
        ts_ms = time.time()
        new_ob['ts_ms'] = ts_ms
        ts_ob = data['params'][1]['timestamp']
        # print(f'{self.EXCHANGE_NAME} {ts_ms - ts_ob}')
        # if isinstance(ts_ob, int):
        #     ts_ob = ts_ob / 1000
        new_ob['timestamp'] = ts_ob
        for new_bid in data['params'][1].get('bids', []):
            if float(new_bid[0]) >= new_ob['top_bid'][0]:
                new_ob['top_bid'] = [float(new_bid[0]), float(new_bid[1])]
                new_ob['top_bid_timestamp'] = data['params'][1]['timestamp']
                flag = True
                side = 'sell'
            if new_ob['bids'].get(new_bid[0]) and new_bid[1] == '0':
                del new_ob['bids'][new_bid[0]]
                if float(new_bid[0]) == new_ob['top_bid'][0] and len(new_ob['bids']):
                    top = sorted(new_ob['bids'])[-1]
                    new_ob['top_bid'] = [float(top), float(new_ob['bids'][top])]
                    new_ob['top_bid_timestamp'] = data['params'][1]['timestamp']
            elif new_bid[1] != '0':
                new_ob['bids'][new_bid[0]] = new_bid[1]
        for new_ask in data['params'][1].get('asks', []):
            if float(new_ask[0]) <= new_ob['top_ask'][0]:
                new_ob['top_ask'] = [float(new_ask[0]), float(new_ask[1])]
                new_ob['top_ask_timestamp'] = data['params'][1]['timestamp']
                flag = True
                side = 'buy'
            if new_ob['asks'].get(new_ask[0]) and new_ask[1] == '0':
                del new_ob['asks'][new_ask[0]]
                if float(new_ask[0]) == new_ob['top_ask'][0] and len(new_ob['asks']):
                    top = sorted(new_ob['asks'])[0]
                    new_ob['top_ask'] = [float(top), float(new_ob['asks'][top])]
                    new_ob['top_ask_timestamp'] = data['params'][1]['timestamp']
            elif new_ask[1] != '0':
                new_ob['asks'][new_ask[0]] = new_ask[1]
        self.orderbook[symbol] = new_ob
        if new_ob['top_ask'][0] <= new_ob['top_bid'][0]:
            self.cut_extra_orders_from_ob(symbol, data)
        # if flag and ts_ms - ts_ob < 0.035:
        #     coin = symbol.split('_')[0]
        #     if self.state == 'Bot':
        #         await self.finder.count_one_coin(coin, self.EXCHANGE_NAME, side, self.multibot.run_arbitrage)
        #     else:
        #         await self.finder.count_one_coin(coin, self.EXCHANGE_NAME, side)

    @try_exc_regular
    def cut_extra_orders_from_ob(self, symbol, data):
        if self.orderbook[symbol]['top_ask_timestamp'] < self.orderbook[symbol]['top_bid_timestamp']:
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
            self.orderbook[symbol].update({'asks': new_asks,
                                           'top_ask': top_ask,
                                           'top_ask_timestamp': data['params'][1]['timestamp']})
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
            self.orderbook[symbol].update({'bids': new_bids,
                                           'top_bid': top_bid,
                                           'top_bid_timestamp': data['params'][1]['timestamp']})

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
                                      'top_ask_timestamp': data['params'][1]['timestamp'],
                                      'top_bid_timestamp': data['params'][1]['timestamp'],
                                      'ts_ms': time.time()}

    @try_exc_regular
    def get_orderbook(self, symbol) -> dict:
        if not self.orderbook.get(symbol):
            return {}
        snap = self.orderbook[symbol].copy()
        #     self.cut_extra_orders_from_ob(symbol)
        # snap = self.orderbook[symbol].copy()
        if isinstance(snap['asks'], list):
            return snap
        if snap['top_ask'][0] <= snap['top_bid'][0]:
            # print(f"ALARM! IDK HOW THIS SHIT WORKS BUT: {snap}")
            return {}
        ob = {'timestamp': self.orderbook[symbol]['timestamp'],
              'asks': [[float(x), float(snap['asks'][x])] for x in sorted(snap['asks'])[:self.ob_len]],
              'bids': [[float(x), float(snap['bids'][x])] for x in sorted(snap['bids'])[::-1][:self.ob_len]],
              'top_ask_timestamp': snap['top_ask_timestamp'],
              'top_bid_timestamp': snap['top_bid_timestamp'],
              'ts_ms': snap['ts_ms']}
        return ob

    @try_exc_regular
    def first_positions_update(self):
        while set(self.orderbook.keys()) < set([self.markets[x] for x in self.markets_list if self.markets.get(x)]):
            time.sleep(0.01)
            print('WHITEBIT.GOT ALL MARKETS')
            self.get_position()

    @try_exc_async
    async def create_aiohttp_order(self, symbol, side):
        path = "/api/v4/order/collateral/limit"
        params = {"market": symbol,
                  "side": side,
                  "amount": self.amount,
                  "price": self.price}
        params = self.get_auth_for_request(params, path)
        path += self._create_uri(params)
        time_start = time.time()
        async with self.async_session.post(url=self.BASE_URL + path, headers=self.session.headers, json=params) as resp:
            response = await resp.json()
            # print(f"{self.EXCHANGE_NAME} ORDER CREATE RESPONSE AIOHTTP: {response}")
            # print(f"ORDER PLACING TIME AIOHTTP: {time.time() - time_start}")
            # print()
            self.aver_time_aiohttp.append(time.time() - time_start)
            self.aver_time_aiohttp_response.append(response['timestamp'] - time_start)
            return {'exchange_name': self.EXCHANGE_NAME,
                    'exchange_order_id': response.get('orderId'),
                    'timestamp': response.get('timestamp', time.time())}

    @try_exc_regular
    def create_httpx_order(self, symbol, side, session):
        path = "/api/v4/order/collateral/limit"
        params = {"market": symbol,
                  "side": side,
                  "amount": self.amount,
                  "price": self.price}
        params = self.get_auth_for_request(params, path)
        path += self._create_uri(params)
        time_start = time.time()
        response = session.post(url=self.BASE_URL + path, headers=self.session.headers, json=params)
        response = response.json()
        # print(f"{self.EXCHANGE_NAME} ORDER CREATE RESPONSE HTTPX: {response}")
        # print(f"ORDER PLACING TIME HTTPX: {time.time() - time_start}")
        # print()
        self.aver_time_httpx.append(time.time() - time_start)
        self.aver_time_httpx_response.append(response['timestamp'] - time_start)
        return {'exchange_name': self.EXCHANGE_NAME,
                'exchange_order_id': response.get('orderId'),
                'timestamp': response.get('timestamp', time.time())}

    @try_exc_regular
    def create_requests_order(self, symbol, side):
        path = "/api/v4/order/collateral/limit"
        params = {"market": symbol,
                  "side": side,
                  "amount": self.amount,
                  "price": self.price}
        params = self.get_auth_for_request(params, path)
        path += self._create_uri(params)
        time_start = time.time()
        response = self.session.post(url=self.BASE_URL + path, headers=self.session.headers, json=params).json()
        # print(f"{self.EXCHANGE_NAME} ORDER CREATE RESPONSE HTTPX: {response}")
        # print(f"ORDER PLACING TIME HTTPX: {time.time() - time_start}")
        # print()
        self.aver_time_requests.append(time.time() - time_start)
        self.aver_time_requests_response.append(response['timestamp'] - time_start)
        return {'exchange_name': self.EXCHANGE_NAME,
                'exchange_order_id': response.get('orderId'),
                'timestamp': response.get('timestamp', time.time())}

    @try_exc_async
    async def create_async_httpx_order(self, symbol, side):
        path = "/api/v4/order/collateral/limit"
        params = {"market": symbol,
                  "side": side,
                  "amount": self.amount,
                  "price": self.price}
        params = self.get_auth_for_request(params, path)
        path += self._create_uri(params)
        time_start = time.time()
        response = await self.httpx_async_session.post(url=self.BASE_URL + path, headers=self.session.headers, json=params)
        response = response.json()
        # print(f"{self.EXCHANGE_NAME} ORDER CREATE RESPONSE HTTPX: {response}")
        # print(f"ORDER PLACING TIME HTTPX: {time.time() - time_start}")
        # print()
        self.aver_time_async_httpx.append(time.time() - time_start)
        self.aver_time_async_httpx_response.append(response['timestamp'] - time_start)
        return {'exchange_name': self.EXCHANGE_NAME,
                'exchange_order_id': response.get('orderId'),
                'timestamp': response.get('timestamp', time.time())}

    @try_exc_async
    async def cancel_order_async_httpx(self, symbol: str, order_id: int):
        path = '/api/v4/order/cancel'
        params = {"market": symbol,
                  "orderId": order_id}
        params = self.get_auth_for_request(params, path)
        resp = await self.httpx_async_session.post(url=self.BASE_URL + path, headers=self.session.headers, json=params)
        resp = resp.json()
        return resp

    @try_exc_regular
    def cancel_order_httpx(self, symbol: str, order_id: int, session):
        path = '/api/v4/order/cancel'
        params = {"market": symbol,
                  "orderId": order_id}
        params = self.get_auth_for_request(params, path)
        resp = session.post(url=self.BASE_URL + path, headers=self.session.headers, json=params)
        resp = resp.json()
        return resp

    @try_exc_async
    async def cancel_order_aiohttp(self, symbol: str, order_id: int):
        path = '/api/v4/order/cancel'
        params = {"market": symbol,
                  "orderId": order_id}
        params = self.get_auth_for_request(params, path)
        async with self.async_session.post(url=self.BASE_URL + path, headers=self.session.headers, json=params) as resp:
            resp = await resp.json()
            return resp

    @try_exc_regular
    def cancel_order_requests(self, symbol: str, order_id: int):
        path = '/api/v4/order/cancel'
        params = {"market": symbol,
                  "orderId": order_id}
        params = self.get_auth_for_request(params, path)
        resp = self.session.post(url=self.BASE_URL + path, headers=self.session.headers, json=params).json()
        return resp

    def get_auth_for_request(self, params, uri):
        params['request'] = uri
        params['nonce'] = int(time.time() * 1000)
        params['nonceWindow'] = True
        signature, payload = self.get_signature(params)
        self.session.headers.update({
            'X-TXC-APIKEY': self.api_key,
            'X-TXC-SIGNATURE': signature,
            'X-TXC-PAYLOAD': payload.decode('ascii')
        })
        return params

    async def aiohttp_test_order(self):
        try:
            ob = self.get_orderbook('EOS_PERP')
            self.amount = self.instruments['EOS_PERP']['min_size']
            self.price = ob['bids'][4][0] - (self.instruments['EOS_PERP']['tick_size'] * 100)
            response = await self.create_aiohttp_order('EOS_PERP', 'buy')
            await self.cancel_order_aiohttp('EOS_PERP', response['exchange_order_id'])
        except:
            print('AIOHTTP ERROR')

    async def httpx_async_test_order(self):
        try:
            ob = self.get_orderbook('EOS_PERP')
            self.amount = self.instruments['EOS_PERP']['min_size']
            self.price = ob['bids'][4][0] - (self.instruments['EOS_PERP']['tick_size'] * 100)
            response = await self.create_async_httpx_order('EOS_PERP', 'buy')
            await self.cancel_order_async_httpx('EOS_PERP', response['exchange_order_id'])
        except:
            print(f"HTTPX2 ERROR")


if __name__ == '__main__':
    import configparser

    config = configparser.ConfigParser()
    config.read('config.ini', "utf-8")
    client = WhiteBitClient(keys=config['WHITEBIT'],
                            leverage=float(config['SETTINGS']['LEVERAGE']),
                            max_pos_part=int(config['SETTINGS']['PERCENT_PER_MARKET']),
                            markets_list=['EOS'])

    import httpx

    def httpx_test_order(session):
        ob = client.get_orderbook('EOS_PERP')
        client.amount = client.instruments['EOS_PERP']['min_size']
        client.price = ob['bids'][4][0] - (client.instruments['EOS_PERP']['tick_size'] * 100)
        response = client.create_httpx_order('EOS_PERP', 'buy', session)
        client.cancel_order_httpx('EOS_PERP', response['exchange_order_id'], session)

    def requests_test_order():
        ob = client.get_orderbook('EOS_PERP')
        client.amount = client.instruments['EOS_PERP']['min_size']
        client.price = ob['bids'][4][0] - (client.instruments['EOS_PERP']['tick_size'] * 100)
        response = client.create_requests_order('EOS_PERP', 'buy')
        client.cancel_order_requests('EOS_PERP', response['exchange_order_id'])

    async def aiohttp_test_order():
        ob = client.get_orderbook('EOS_PERP')
        client.amount = client.instruments['EOS_PERP']['min_size']
        client.price = ob['bids'][4][0] - (client.instruments['EOS_PERP']['tick_size'] * 100)
        response = await client.create_aiohttp_order('EOS_PERP', 'buy')
        await client.cancel_order_aiohttp('EOS_PERP', response['exchange_order_id'])

    async def httpx_async_test_order():
        ob = client.get_orderbook('EOS_PERP')
        client.amount = client.instruments['EOS_PERP']['min_size']
        client.price = ob['bids'][4][0] - (client.instruments['EOS_PERP']['tick_size'] * 100)
        response = await client.create_async_httpx_order('EOS_PERP', 'buy')
        await client.cancel_order_async_httpx('EOS_PERP', response['exchange_order_id'])

    client.markets_list = list(client.markets.keys())
    client.run_updater()

    client.aver_time_httpx = []
    client.aver_time_httpx_response = []
    client.aver_time_async_httpx = []
    client.aver_time_async_httpx_response = []
    client.aver_time_requests = []
    client.aver_time_requests_response = []
    client.aver_time_aiohttp = []
    client.aver_time_aiohttp_response = []
    httpx_client = httpx.Client(http2=True)
    time.sleep(1)
    while True:
        time.sleep(1)
        client.order_loop.create_task(aiohttp_test_order())
        client.loop.create_task(httpx_async_test_order())
        requests_test_order()
        httpx_test_order(httpx_client)
        print(f"Repeats (1 sec each): {len(client.aver_time_async_httpx)}")
        print('OWN ASYNC HTTPX')
        print(f"Min order create time: {min(client.aver_time_async_httpx)} sec")
        print(f"Max order create time: {max(client.aver_time_async_httpx)} sec")
        print(f"Aver. order create time: {sum(client.aver_time_async_httpx) / len(client.aver_time_async_httpx)} sec")
        print('RESPONSE ASYNC HTTPX')
        print(f"Min order create time: {min(client.aver_time_async_httpx_response)} sec")
        print(f"Max order create time: {max(client.aver_time_async_httpx_response)} sec")
        print(f"Aver. order create time: {sum(client.aver_time_async_httpx_response) / len(client.aver_time_async_httpx_response)} sec")
        print()
        print(f"Repeats (1 sec each): {len(client.aver_time_aiohttp)}")
        print('OWN AIOHTTP')
        print(f"Min order create time: {min(client.aver_time_aiohttp)} sec")
        print(f"Max order create time: {max(client.aver_time_aiohttp)} sec")
        print(f"Aver. order create time: {sum(client.aver_time_aiohttp) / len(client.aver_time_aiohttp)} sec")
        print('RESPONSE AIOHTTP')
        print(f"Min order create time: {min(client.aver_time_aiohttp_response)} sec")
        print(f"Max order create time: {max(client.aver_time_aiohttp_response)} sec")
        print(f"Aver. order create time: {sum(client.aver_time_aiohttp_response) / len(client.aver_time_aiohttp_response)} sec")
        print()
        print(f"Repeats (1 sec each): {len(client.aver_time_requests)}")
        print('OWN REQUESTS')
        print(f"Min order create time: {min(client.aver_time_requests)} sec")
        print(f"Max order create time: {max(client.aver_time_requests)} sec")
        print(f"Aver. order create time: {sum(client.aver_time_requests) / len(client.aver_time_requests)} sec")
        print('RESPONSE REQUESTS')
        print(f"Min order create time: {min(client.aver_time_requests_response)} sec")
        print(f"Max order create time: {max(client.aver_time_requests_response)} sec")
        print(f"Aver. order create time: {sum(client.aver_time_requests_response) / len(client.aver_time_requests_response)} sec")
        print()
        print(f"Repeats (1 sec each): {len(client.aver_time_httpx)}")
        print('OWN HTTPX')
        print(f"Min order create time: {min(client.aver_time_httpx)} sec")
        print(f"Max order create time: {max(client.aver_time_httpx)} sec")
        print(f"Aver. order create time: {sum(client.aver_time_httpx) / len(client.aver_time_httpx)} sec")
        print('RESPONSE HTTPX')
        print(f"Min order create time: {min(client.aver_time_httpx_response)} sec")
        print(f"Max order create time: {max(client.aver_time_httpx_response)} sec")
        print(f"Aver. order create time: {sum(client.aver_time_httpx_response) / len(client.aver_time_httpx_response)} sec")
        print()

