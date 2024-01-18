import time
import aiohttp
import json
import requests
from datetime import datetime
import threading
import hmac
import hashlib
from clients.core.enums import ResponseStatus, OrderStatus, ClientsOrderStatuses
from core.wrappers import try_exc_regular, try_exc_async
from clients.core.base_client import BaseClient
import asyncio


class BtseClient(BaseClient):
    PUBLIC_WS_ENDPOINT = 'wss://ws.btse.com/ws/oss/futures'
    PRIVATE_WS_ENDPOINT = 'wss://ws.btse.com/ws/futures'
    BASE_URL = f"https://api.btse.com/futures"
    EXCHANGE_NAME = 'BTSE'
    headers = {"Accept": "application/json;charset=UTF-8",
               "Content-Type": "application/json",
               'Connection': 'keep-alive'}
    order_statuses = {2: 'Order Inserted',
                      3: 'Order Transacted',
                      4: 'Order Fully Transacted',
                      5: 'Order Partially Transacted',
                      6: 'Order Cancelled',
                      7: 'Order Refunded',
                      9: 'Trigger Inserted',
                      10: 'Trigger Activated',
                      15: 'Order Rejected',
                      16: 'Order Not Found',
                      17: 'Request failed'}

    def __init__(self, multibot=None, keys=None, leverage=None, state='Bot', markets_list=[],
                 max_pos_part=20, finder=None, ob_len=4):
        super().__init__()
        self.multibot = multibot
        self.state = state
        self.finder = finder
        self.max_pos_part = max_pos_part
        self.leverage = leverage
        self.markets_list = markets_list
        self.session = requests.session()
        self.session.headers.update(self.headers)
        self.instruments = {}
        self.markets = self.get_markets()
        self.orderbook = {}
        if self.state == 'Bot':
            self.api_key = keys['API_KEY']
            self.api_secret = keys['API_SECRET']
            self.positions = {}
            self.balance = {}
            self.get_real_balance()
            self.get_position()
        self.ob_len = ob_len
        self.error_info = None
        self._connected = asyncio.Event()
        self._loop = asyncio.new_event_loop()
        self.wst_public = threading.Thread(target=self._run_ws_forever)
        self._order_loop = asyncio.new_event_loop()
        self.orders_thread = threading.Thread(target=self.deals_thread_func)
        # self._wst_orderbooks = threading.Thread(target=self._process_ws_line)
        # if self.state == 'Bot':
        #     self.wst_private = threading.Thread(target=self._run_ws_forever)
        self.price = 0
        self.amount = 0
        self.orderbook = {}
        self.orders = {}
        self.last_price = {}
        self.taker_fee = float(keys['TAKER_FEE']) * 0.75
        self.orig_sizes = {}
        # self.message_queue = asyncio.Queue(loop=self._loop)
        self.LAST_ORDER_ID = 'default'
        self.ob_push_limit = None
        self.last_keep_alive = 0
        self.deal = False
        self.response = None
        self.side = 'buy'
        self.symbol = None
        self.last_symbol = None

        # self.market_to_check = {}

    @try_exc_regular
    def deals_thread_func(self):
        while True:
            self._order_loop.run_until_complete(self._run_order_loop())
            # await self.cancel_all_tasks(self._order_loop)
            # self._order_loop.stop()
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
                    self.last_keep_alive = order['timestamp'] / 1000
                    self.deal = False
                else:
                    ts_ms = time.time()
                    if ts_ms - self.last_keep_alive > 5:
                        self.last_keep_alive = ts_ms
                        self._order_loop.create_task(self.get_position_async())
                        # print(f"keep-alive {self.EXCHANGE_NAME} time: {time.time() - ts_ms}")
                        # if not self.last_symbol:
                        #     self.last_symbol = self.markets[self.markets_list[0]]
                        # self.amount = self.instruments[self.last_symbol]['min_size']
                        # self.fit_sizes(self.orderbook[self.last_symbol]['top_bid'][0] * 0.9, self.last_symbol)
                        # self.side = 'buy'
                        # order = await self.create_fast_order(self.last_symbol, self.side)
                        # print(f"Create {self.EXCHANGE_NAME} keep-alive order time: {order['timestamp'] - ts_ms}")
                        # self.LAST_ORDER_ID = 'default'
                        # await self.cancel_order(self.last_symbol, order['exchange_order_id'], self.async_session)
                        # self.get_position()
                await asyncio.sleep(0.0001)

    @staticmethod
    @try_exc_regular
    def get_price_precision(tick_size):
        if '.' in str(tick_size):
            price_precision = len(str(tick_size).split('.')[1])
        elif '-' in str(tick_size):
            price_precision = int(str(tick_size).split('-')[1])
        else:
            price_precision = 0
        return price_precision

    @try_exc_regular
    def get_markets(self):
        way = "https://api.btse.com/futures/api/v2.1/market_summary"
        resp = self.session.get(url=way).json()
        markets = {}
        for market in resp:
            if market['active'] and 'PFC' in market['symbol']:
                markets.update({market['base']: market['symbol']})
                price_precision = self.get_price_precision(market['minPriceIncrement'])
                step_size = market['minSizeIncrement'] * market['contractSize']
                quantity_precision = len(str(step_size).split('.')[1]) if '.' in str(step_size) else 1
                min_size = market['minOrderSize'] * market['contractSize']
                self.instruments.update({market['symbol']: {'contract_value': market['contractSize'],
                                                            'tick_size': market['minPriceIncrement'],
                                                            'step_size': step_size,
                                                            'quantity_precision': quantity_precision,
                                                            'price_precision': price_precision,
                                                            'min_size': min_size}})
        return markets

    @try_exc_regular
    def get_all_tops(self):
        tops = {}
        for coin, symbol in self.markets.items():
            orderbook = self.orderbook.get(symbol, {})
            if orderbook and orderbook.get('bids') and orderbook.get('asks'):
                c_v = self.instruments[symbol]['contract_value']
                tops.update({self.EXCHANGE_NAME + '__' + coin: {
                    'top_bid': orderbook['top_bid'][0], 'top_ask': orderbook['top_ask'][0],
                    'bid_vol': orderbook['top_bid'][1] * c_v, 'ask_vol': orderbook['top_ask'][1] * c_v,
                    'ts_exchange': orderbook['timestamp']}})
        return tops

    @try_exc_regular
    def _run_ws_forever(self):
        while True:
            if self.state == 'Bot':
                self._loop.create_task(self._run_ws_loop('private',
                                                         self.update_orderbook_snapshot,
                                                         self.update_orderbook,
                                                         self.update_positions,
                                                         self.update_fills))
            self._loop.run_until_complete(self._run_ws_loop('public',
                                                            self.update_orderbook_snapshot,
                                                            self.update_orderbook,
                                                            self.update_positions,
                                                            self.update_fills))
            # await self.cancel_all_tasks(self._loop)
            # self._loop.stop()

            # self._loop.create_task(self._run_ws_loop(ws_type))

    @try_exc_regular
    def generate_signature(self, path, nonce, data=''):
        language = "latin-1"
        message = path + nonce + data
        signature = hmac.new(bytes(self.api_secret, language),
                             msg=bytes(message, language),
                             digestmod=hashlib.sha384).hexdigest()
        return signature

    @try_exc_regular
    def get_private_headers(self, path, data={}):
        json_data = json.dumps(data) if data else ''
        nonce = str(int(time.time() * 1000))
        signature = self.generate_signature(path, nonce, json_data)
        self.session.headers.update({"request-api": self.api_key,
                                     "request-nonce": nonce,
                                     "request-sign": signature})

    @try_exc_regular
    def get_real_balance(self):
        path = '/api/v2.1/user/wallet'
        self.get_private_headers(path)
        response = self.session.get(url=self.BASE_URL + path)
        if response.status_code in ['200', 200, '201', 201]:
            balance_data = response.json()
            self.balance = {'timestamp': round(datetime.utcnow().timestamp()),
                            'total': balance_data[0]['totalValue'],
                            'free': balance_data[0]['availableBalance']}
        else:
            print(f"ERROR IN GET_REAL_BALANCE RESPONSE BTSE: {response.text=}")

    @try_exc_regular
    def cancel_all_orders(self):
        path = "/api/v2.1/order/cancelAllAfter"
        data = {"timeout": 10}
        self.get_private_headers(path, data)
        response = self.session.post(self.BASE_URL + path, json=data)
        return response.text

    @try_exc_async
    async def get_position_async(self):
        path = "/api/v2.1/user/positions"
        self.get_private_headers(path)
        async with self.async_session.get(self.BASE_URL + path, headers=self.session.headers) as res:
            response = await res.json()
            # print(f'GET_POSITION RESPONSE', response)
            self.positions = {}
            # if await response.status_code in ['200', 200, '201', 201]:
            for pos in response:
                contract_value = self.instruments[pos['symbol']]['contract_value']
                if pos['side'] == 'BUY':
                    size_usd = pos['orderValue']
                    size_coin = pos['size'] * contract_value
                else:
                    size_usd = -pos['orderValue']
                    size_coin = -pos['size'] * contract_value
                self.positions.update({pos['symbol']: {'timestamp': int(datetime.utcnow().timestamp()),
                                                       'entry_price': pos['entryPrice'],
                                                       'amount': size_coin,
                                                       'amount_usd': size_usd}})
            # else:
            #     print(f"ERROR IN GET_POSITION RESPONSE BTSE: {response.text=}")

    @try_exc_regular
    def get_position(self):
        path = "/api/v2.1/user/positions"
        self.get_private_headers(path)
        response = self.session.get(self.BASE_URL + path)
        # print('GET_POSITION RESPONSE', response.json())
        self.positions = {}
        if response.status_code in ['200', 200, '201', 201]:
            for pos in response.json():
                contract_value = self.instruments[pos['symbol']]['contract_value']
                if pos['side'] == 'BUY':
                    size_usd = pos['orderValue']
                    size_coin = pos['size'] * contract_value
                else:
                    size_usd = -pos['orderValue']
                    size_coin = -pos['size'] * contract_value
                self.positions.update({pos['symbol']: {'timestamp': int(datetime.utcnow().timestamp()),
                                                       'entry_price': pos['entryPrice'],
                                                       'amount': size_coin,
                                                       'amount_usd': size_usd}})
        else:
            print(f"ERROR IN GET_POSITION RESPONSE BTSE: {response.text=}")

    # example = [
    #     {'marginType': 91, 'entryPrice': 2285.71, 'markPrice': 2287.538939479, 'symbol': 'ETHPFC', 'side': 'SELL',
    #      'orderValue': 91.5015575792, 'settleWithAsset': 'USDT', 'unrealizedProfitLoss': -0.07315758,
    #      'totalMaintenanceMargin': 0.503258567, 'size': 4, 'liquidationPrice': 2760.8422619841, 'isolatedLeverage': 0,
    #      'adlScoreBucket': 2, 'liquidationInProgress': False, 'timestamp': 0, 'takeProfitOrder': None,
    #      'stopLossOrder': None, 'positionMode': 'ONE_WAY', 'positionDirection': None, 'positionId': 'ETHPFC-USD',
    #      'currentLeverage': 6.0406022256},
    #     {'marginType': 91, 'entryPrice': 15.3456, 'markPrice': 15.448104591, 'symbol': 'LINKPFC', 'side': 'BUY',
    #      'orderValue': 29.3513987229, 'settleWithAsset': 'USDT', 'unrealizedProfitLoss': 0.19475872,
    #      'totalMaintenanceMargin': 0.467254602, 'size': 190, 'liquidationPrice': 5.2669433086, 'isolatedLeverage': 0,
    #      'adlScoreBucket': 2, 'liquidationInProgress': False, 'timestamp': 0, 'takeProfitOrder': None,
    #      'stopLossOrder': None, 'positionMode': 'ONE_WAY', 'positionDirection': None, 'positionId': 'LINKPFC-USD',
    #      'currentLeverage': 6.0406022256}]

    @try_exc_regular
    def get_order_response_status(self, response):
        api_resp = self.order_statuses.get(response[0]['status'], None)
        if api_resp in ['Order Refunded', 'Order Rejected', 'Order Not Found', 'Request failed']:
            status = ResponseStatus.ERROR
            self.error_info = response
        elif api_resp in ['Order Inserted', 'Order Transacted', 'Order Fully Transacted', 'Order Partially Transacted']:
            status = ResponseStatus.SUCCESS
        else:
            status = ResponseStatus.NO_CONNECTION
        return status

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

    @try_exc_async
    async def create_fast_order(self, symbol, side, expire=10000, client_id=None, expiration=None):
        # time_start = time.time()
        path = '/api/v2.1/order'
        contract_value = self.instruments[symbol]['contract_value']
        body = {"symbol": symbol,
                "side": side.upper(),
                "price": self.price,
                "type": "LIMIT",
                'size': int(self.amount / contract_value)}
        # print(f"{self.EXCHANGE_NAME} SENDING ORDER: {body}")
        self.get_private_headers(path, body)
        # async with aiohttp.ClientSession() as session:
        async with self.async_session.post(url=self.BASE_URL + path, headers=self.session.headers, json=body) as resp:
            res = await resp.json()
            # resp = self.session.post(url=self.BASE_URL + path, json=body)
            # res = resp.json()
            #     print(resp.headers)
            # print(f"ORDER PLACING TIME: {time.time() - time_start}")
            # self.aver_time.append(time.time() - time_start)
            # if len(res):
            status = 'KEEP-ALIVE'
            if self.deal:
                print(f"{self.EXCHANGE_NAME} ORDER CREATE RESPONSE: {res}")
                status = self.get_order_response_status(res)
                self.LAST_ORDER_ID = res[0].get('orderID', 'default')
                self.orig_sizes.update({self.LAST_ORDER_ID: res[0].get('originalSize')})
            return {'exchange_name': self.EXCHANGE_NAME,
                    'exchange_order_id': self.LAST_ORDER_ID,
                    'timestamp': res[0]['timestamp'] / 1000 if res[0].get('timestamp') else time.time(),
                    'status': status}
            # res_example = [{'status': 2, 'symbol': 'BTCPFC', 'orderType': 76, 'price': 43490, 'side': 'BUY', 'size': 1,
            #             'orderID': '13a82711-f6e2-4228-bf9f-3755cd8d7885', 'timestamp': 1703535543583,
            #             'triggerPrice': 0, 'trigger': False, 'deviation': 100, 'stealth': 100, 'message': '',
            #             'avgFillPrice': 0, 'fillSize': 0, 'clOrderID': '', 'originalSize': 1, 'postOnly': False,
            #             'remainingSize': 1, 'orderDetailType': None, 'positionMode': 'ONE_WAY',
            #             'positionDirection': None, 'positionId': 'BTCPFC-USD', 'time_in_force': 'GTC'}]

    @try_exc_async
    async def create_order(self, symbol, side, session, expire=10000, client_id=None, expiration=None):
        # time_start = time.time()
        path = '/api/v2.1/order'
        contract_value = self.instruments[symbol]['contract_value']
        body = {"symbol": symbol,
                "side": side.upper(),
                "price": self.price,
                "type": "LIMIT",
                'size': int(self.amount / contract_value)}
        self.get_private_headers(path, body)
        async with session.post(url=self.BASE_URL + path, headers=self.session.headers, json=body) as resp:
            res = await resp.json()

            # self.aver_time.append(time.time() - time_start)
            # self.aver_time_response.append((res[0]['timestamp'] / 1000) - time_start)
            # print(f"{self.EXCHANGE_NAME} ORDER CREATE RESPONSE: {res}")
            if len(res):
                status = self.get_order_response_status(res)
                self.LAST_ORDER_ID = res[0].get('orderID', 'default')
                self.orig_sizes.update({self.LAST_ORDER_ID: res[0].get('originalSize')})
                return {'exchange_name': self.EXCHANGE_NAME,
                        'exchange_order_id': self.LAST_ORDER_ID,
                        'timestamp': res[0]['timestamp'] / 1000 if res[0].get('timestamp') else time.time(),
                        'status': status}

    @try_exc_regular
    def get_status_of_order(self, stat_num):
        if api_status := self.order_statuses.get(stat_num):
            if api_status == 'Order Fully Transacted':
                return OrderStatus.FULLY_EXECUTED
            elif api_status in ['Order Transacted', 'Order Partially Transacted']:
                return OrderStatus.PARTIALLY_EXECUTED
            elif api_status == 'Order Inserted':
                return OrderStatus.PROCESSING
            else:
                return OrderStatus.NOT_EXECUTED
        else:
            return OrderStatus.NOT_EXECUTED

    @try_exc_regular
    def get_order_by_id(self, symbol, order_id, cl_order_id=None):
        if order_id is None and cl_order_id is None:
            raise ValueError("Either orderID or clOrderID must be provided")
        params = {}
        path = "/api/v2.1/order"
        if order_id:
            params['orderID'] = order_id
            final_path = f"/api/v2.1/order?orderID={order_id}"
        else:
            params['clOrderID'] = cl_order_id
            final_path = f"/api/v2.1/order?clOrderID={cl_order_id}"
        self.get_private_headers(path, params)
        response = self.session.get(url=self.BASE_URL + final_path, json=params)
        if response.status_code in ['200', 200, '201', 201]:
            order_data = response.json()
            # print(self.EXCHANGE_NAME, 'GET_ORDER_BY_ID RESPONSE', response)
        else:
            print(f"ERROR IN GET_ORDER_BY_ID RESPONSE BTSE: {response.text=}")
            order_data = {}
        # symbol = order_data.get('symbol').replace('-PERP', 'PFC')
        c_v = self.instruments[symbol]['contract_value']
        return {'exchange_order_id': order_data.get('orderID'),
                'exchange': self.EXCHANGE_NAME,
                'status': self.get_status_of_order(order_data.get('status', 0)),
                'factual_price': order_data.get('avgFilledPrice', 0),
                'factual_amount_coin': order_data.get('filledSize', 0) * c_v,
                'factual_amount_usd': order_data.get('filledSize', 0) * c_v * order_data.get('avgFilledPrice', 0),
                'datetime_update': datetime.utcnow(),
                'ts_update': int(datetime.utcnow().timestamp() * 1000)}
        # get_order_example = {'orderType': 76, 'price': 42424.8, 'size': 1, 'side': 'BUY', 'filledSize': 1,
        #                      'orderValue': 424.248, 'pegPriceMin': 0, 'pegPriceMax': 0, 'pegPriceDeviation': 1,
        #                      'timestamp': 1703584934116, 'orderID': 'fab9af76-8c8f-4fd3-b006-d7da8557d462',
        #                      'stealth': 1, 'triggerOrder': False, 'triggered': False, 'triggerPrice': 0,
        #                      'triggerOriginalPrice': 0, 'triggerOrderType': 0, 'triggerTrailingStopDeviation': 0,
        #                      'triggerStopPrice': 0, 'symbol': 'ETH-PERP', 'trailValue': 0, 'remainingSize': 0,
        #                      'clOrderID': '', 'reduceOnly': False, 'status': 4, 'triggerUseLastPrice': False,
        #                      'avgFilledPrice': 2224.66, 'timeInForce': 'GTC', 'closeOrder': False}

    @try_exc_regular
    def get_positions(self):
        # NECESSARY
        return self.positions

    @try_exc_regular
    def get_orders(self):
        # NECESSARY
        return self.orders

    @try_exc_regular
    def get_balance(self):
        if not self.balance.get('total'):
            self.get_real_balance()
        return self.balance['total']

    @try_exc_regular
    def get_available_balance(self):
        return super().get_available_balance(
            leverage=self.leverage,
            max_pos_part=self.max_pos_part,
            positions=self.positions,
            balance=self.balance)

    @try_exc_async
    async def _run_ws_loop(self, ws_type, update_orderbook_snapshot, update_orderbook, update_positions, update_fills):
        async with aiohttp.ClientSession() as s:
            if ws_type == 'private':
                endpoint = self.PRIVATE_WS_ENDPOINT
            else:
                endpoint = self.PUBLIC_WS_ENDPOINT
                # self.async_session = s
            async with s.ws_connect(endpoint) as ws:
                print(f"BTSE: connected {ws_type}")
                self._connected.set()
                if ws_type == 'private':
                    self._ws_private = ws
                    await self._loop.create_task(self.subscribe_privates())
                    # asyncio.run_coroutine_threadsafe(self.subscribe_privates(), self._loop)
                else:
                    self._ws_public = ws
                    await self._loop.create_task(self.subscribe_orderbooks())
                    # asyncio.run_coroutine_threadsafe(self.subscribe_orderbooks(), self._loop)
                async for msg in ws:
                    data = json.loads(msg.data)
                    if 'update' in data.get('topic', ''):
                        if data.get('data') and data['data']['type'] == 'delta':
                            # print(time.time() - data['data']['timestamp'] / 1000)
                            self._loop.create_task(update_orderbook(data))
                        elif data.get('data') and data['data']['type'] == 'snapshot':
                            self._loop.create_task(update_orderbook_snapshot(data))
                        elif data.get('topic') == 'allPosition':
                            self._loop.create_task(update_positions(data))
                        elif data.get('topic') == 'fills':
                            self._loop.create_task(update_fills(data))
            await ws.close()

    @try_exc_async
    async def cancel_order(self, symbol: str, order_id: str, session: aiohttp.ClientSession):
        path = '/api/v2.1/order'
        params = {'symbol': symbol,
                  'orderID': order_id}
        self.get_private_headers(path, params)
        path += '?' + "&".join([f"{key}={params[key]}" for key in sorted(params)])
        async with session.delete(url=self.BASE_URL + path, headers=self.session.headers, json=params) as resp:
            resp = await resp.json()
            return resp

    # @try_exc_regular
    # def _process_ws_line(self):
    #     # self._loop.create_task(self.process_messages())
    #     asyncio.run_coroutine_threadsafe(self.process_messages(), self._loop)
    #
    # @try_exc_async
    # async def process_messages(self):
    #     while True:
    #         msg = await self.message_queue.get()
    #         data = json.loads(msg.data)
    #         if 'update' in data.get('topic', ''):
    #             if data.get('data') and data['data']['type'] == 'snapshot':
    #                 self.update_orderbook_snapshot(data)
    #             elif data.get('data') and data['data']['type'] == 'delta':
    #                 self.update_orderbook(data)
    #         elif self.state == 'Bot':
    #             if data.get('topic') == 'allPosition':
    #                 self.update_positions(data)
    #             elif data.get('topic') == 'fills':
    #                 self.update_fills(data)
    #         self.message_queue.task_done()
    #         # if self.message_queue.qsize() > 100:
    #         #     message = f'ALERT! {self.EXCHANGE_NAME} WS LINE LENGTH: {self.message_queue.qsize()}'
    #         #     self.telegram_bot.send_message(message, self.alert_id)
    # # @try_exc_regular
    # # def update_private_data(self, data):

    @try_exc_regular
    def get_order_status_by_fill(self, order_id, size):
        orig_size = self.orig_sizes.get(order_id)
        print(f'Label 3 {orig_size=},{size=}')
        if size == 0:
            return OrderStatus.NOT_EXECUTED
        if orig_size == size:
            return OrderStatus.FULLY_EXECUTED
        else:
            return OrderStatus.PARTIALLY_EXECUTED

    @try_exc_async
    async def update_fills(self, data):
        for fill in data['data']:
            start_time = time.time()
            self.last_price.update({fill['side'].lower(): float(fill['price'])})
            order_id = fill['orderId']
            while order_id != self.LAST_ORDER_ID:
                time.sleep(0.1)
                if time.time() - start_time > 2:
                    break
            size = float(fill['size']) * self.instruments[fill['symbol']]['contract_value']
            size_usd = size * float(fill['price'])
            if order := self.orders.get(order_id):
                avg_price = (order['factual_amount_usd'] + size_usd) / (size + order['factual_amount_coin'])
                new_size = order['factual_amount_coin'] + size
                result_exist = {'status': self.get_order_status_by_fill(order_id, new_size),
                                'factual_price': avg_price,
                                'factual_amount_coin': new_size,
                                'factual_amount_usd': order['factual_amount_usd'] + size_usd,
                                'datetime_update': datetime.utcnow(),
                                'ts_update': fill['timestamp']}
                self.orders[order_id].update(result_exist)
                continue
            result_new = {'exchange_order_id': order_id,
                          'exchange': self.EXCHANGE_NAME,
                          'status': self.get_order_status_by_fill(order_id, size),
                          'factual_price': float(fill['price']),
                          'factual_amount_coin': size,
                          'factual_amount_usd': size_usd,
                          'datetime_update': datetime.utcnow(),
                          'ts_update': fill['timestamp']}
            self.orders.update({order_id: result_new})
        # fills_example = {'topic': 'fills', 'id': '', 'data': [
        #     {'orderId': '04e64f39-b715-44e5-a2b8-e60b3379c2f3', 'serialId': 4260246, 'clOrderId': '', 'type': '76',
        #      'symbol': 'ETHPFC', 'side': 'BUY', 'price': '2238.25', 'size': '1.0', 'feeAmount': '0.01119125',
        #      'feeCurrency': 'USDT', 'base': 'ETHPFC', 'quote': 'USD', 'maker': False, 'timestamp': 1703580743141,
        #      'tradeId': 'd6201931-446d-4a1c-ab83-e3ebdcf2f077'}]}

    @try_exc_async
    async def update_positions(self, data):
        for pos in data['data']:
            market = pos['marketName'].split('-')[0]
            contract_value = self.instruments[market]['contract_value']
            size = pos['totalContracts'] * contract_value
            size = -size if pos['orderModeName'] == 'MODE_SELL' else size
            self.positions.update({market: {'timestamp': int(datetime.utcnow().timestamp()),
                                            'entry_price': pos['entryPrice'],
                                            'amount': size,
                                            'amount_usd': pos['totalValue']}})
            # print('POSITIONS AFTER WS UPDATING:', self.positions)
        # positions_example = {'topic': 'allPosition', 'id': '', 'data': [
        #     {'id': 6233130152254608579, 'requestId': 0, 'username': 'nikicha', 'userCurrency': None,
        #      'marketName': 'ETHPFC-USD', 'orderType': 90, 'orderMode': 83, 'status': 65, 'originalAmount': 0.01,
        #      'maxPriceHeld': 0, 'pegPriceMin': 0, 'stealth': 1, 'baseCurrency': None, 'quoteCurrency': None,
        #      'quoteCurrencyFiat': False, 'parents': None, 'makerFeesRatio': None, 'takerFeesRatio': [0.0005],
        #      'ip': None, 'systemId': None, 'orderID': None, 'vendorName': None, 'botID': None, 'poolID': 0,
        #      'maxStealthDisplayAmount': 0, 'sellexchangeRate': 0, 'tag': None, 'triggerPrice': 0, 'closeOrder': False,
        #      'dbBaseBalHeld': 0, 'dbQuoteBalHeld': -0.846941176, 'isFuture': True, 'liquidationInProgress': False,
        #      'marginType': 91, 'entryPrice': 2285.71, 'liquidationPrice': 2760.8226073098,
        #      'markedPrice': 2287.470283012, 'marginHeld': 0, 'unrealizedProfitLoss': -0.07041132,
        #      'totalMaintenanceMargin': 0.503243462, 'totalContracts': 4, 'marginChargedLongOpen': 0,
        #      'marginChargedShortOpen': 0, 'unchargedMarginLongOpen': 0, 'unchargedMarginShortOpen': 0,
        #      'isolatedCurrency': None, 'isolatedLeverage': 0, 'totalFees': 0, 'totalValue': -91.49881132,
        #      'adlScoreBucket': 2, 'adlScorePercentile': 0.8333333333, 'booleanVar1': False, 'char1': '\x00',
        #      'orderTypeName': 'TYPE_FUTURES_POSITION', 'orderModeName': 'MODE_SELL',
        #      'marginTypeName': 'FUTURES_MARGIN_CROSS', 'currentLeverage': 6.0398382482, 'averageFillPrice': 0,
        #      'filledSize': 0, 'takeProfitOrder': None, 'stopLossOrder': None, 'positionId': 'ETHPFC-USD',
        #      'positionMode': 'ONE_WAY', 'positionDirection': None, 'future': True, 'settleWithNonUSDAsset': 'USDT'},
        #
        #     {'id': 280658257410787295, 'requestId': 0, 'username': 'nikicha', 'userCurrency': None,
        #      'marketName': 'LINKPFC-USD', 'orderType': 90, 'orderMode': 66, 'status': 65, 'originalAmount': 0.01,
        #      'maxPriceHeld': 0, 'pegPriceMin': 0, 'stealth': 1, 'baseCurrency': None, 'quoteCurrency': None,
        #      'quoteCurrencyFiat': False, 'parents': None, 'makerFeesRatio': None, 'takerFeesRatio': [0.0005],
        #      'ip': None, 'systemId': None, 'orderID': None, 'vendorName': None, 'botID': None, 'poolID': 0,
        #      'maxStealthDisplayAmount': 0, 'sellexchangeRate': 0, 'tag': None, 'triggerPrice': 0, 'closeOrder': False,
        #      'dbBaseBalHeld': 0, 'dbQuoteBalHeld': -0.846941176, 'isFuture': True, 'liquidationInProgress': False,
        #      'marginType': 91, 'entryPrice': 15.3456, 'liquidationPrice': 5.2654664485, 'markedPrice': 15.447681802,
        #      'marginHeld': 0, 'unrealizedProfitLoss': 0.19395542, 'totalMaintenanceMargin': 0.467241814,
        #      'totalContracts': 190, 'marginChargedLongOpen': 0, 'marginChargedShortOpen': 0,
        #      'unchargedMarginLongOpen': 0, 'unchargedMarginShortOpen': 0, 'isolatedCurrency': None,
        #      'isolatedLeverage': 0, 'totalFees': 0, 'totalValue': 29.350595424, 'adlScoreBucket': 2,
        #      'adlScorePercentile': 0.25, 'booleanVar1': False, 'char1': '\x00',
        #      'orderTypeName': 'TYPE_FUTURES_POSITION', 'orderModeName': 'MODE_BUY',
        #      'marginTypeName': 'FUTURES_MARGIN_CROSS', 'currentLeverage': 6.0398382482, 'averageFillPrice': 0,
        #      'filledSize': 0, 'takeProfitOrder': None, 'stopLossOrder': None, 'positionId': 'LINKPFC-USD',
        #      'positionMode': 'ONE_WAY', 'positionDirection': None, 'future': True, 'settleWithNonUSDAsset': 'USDT'}]}

    @try_exc_async
    async def subscribe_orderbooks(self):
        args = [f"update:{self.markets[x]}_0" for x in self.markets_list if self.markets.get(x)]
        method = {"op": "subscribe",
                  "args": args}
        await self._connected.wait()
        await self._ws_public.send_json(method)

    @try_exc_regular
    def get_wss_auth(self):
        url = "/ws/futures"
        self.get_private_headers(url)
        data = {"op": "authKeyExpires",
                "args": [self.session.headers["request-api"],
                         self.session.headers["request-nonce"],
                         self.session.headers["request-sign"]]}
        return data

    @try_exc_async
    async def subscribe_privates(self):
        method_pos = {"op": "subscribe",
                      "args": ["allPosition"]}
        method_fills = {"op": "subscribe",
                        "args": ["fills"]}
        auth = self.get_wss_auth()
        await self._connected.wait()
        await self._ws_private.send_json(auth)
        await self._ws_private.send_json(method_pos)
        await self._ws_private.send_json(method_fills)

    @try_exc_async
    async def update_orderbook(self, data):
        flag = False
        symbol = data['data']['symbol']
        self.last_symbol = symbol
        new_ob = self.orderbook[symbol].copy()
        ts_ms = time.time()
        new_ob['ts_ms'] = ts_ms
        ts_ob = data['data']['timestamp']
        # print(f'{self.EXCHANGE_NAME} {ts_ms - ts_ob / 1000}')
        if isinstance(ts_ob, int):
            ts_ob = ts_ob / 1000
        new_ob['timestamp'] = ts_ob
        for new_bid in data['data']['bids']:
            if float(new_bid[0]) >= new_ob['top_bid'][0]:
                new_ob['top_bid'] = [float(new_bid[0]), float(new_bid[1])]
                new_ob['top_bid_timestamp'] = data['data']['timestamp']
                flag = True
                side = 'sell'
            if new_ob['bids'].get(new_bid[0]) and new_bid[1] == '0':
                del new_ob['bids'][new_bid[0]]
                if float(new_bid[0]) == new_ob['top_bid'][0] and len(new_ob['bids']):
                    top = sorted(new_ob['bids'])[-1]
                    new_ob['top_bid'] = [float(top), float(new_ob['bids'][top])]
                    new_ob['top_bid_timestamp'] = data['data']['timestamp']
            elif new_bid[1] != '0':
                new_ob['bids'][new_bid[0]] = new_bid[1]
        for new_ask in data['data']['asks']:
            if float(new_ask[0]) <= new_ob['top_ask'][0]:
                new_ob['top_ask'] = [float(new_ask[0]), float(new_ask[1])]
                new_ob['top_ask_timestamp'] = data['data']['timestamp']
                flag = True
                side = 'buy'
            if new_ob['asks'].get(new_ask[0]) and new_ask[1] == '0':
                del new_ob['asks'][new_ask[0]]
                if float(new_ask[0]) == new_ob['top_ask'][0] and len(new_ob['asks']):
                    top = sorted(new_ob['asks'])[0]
                    new_ob['top_ask'] = [float(top), float(new_ob['asks'][top])]
                    new_ob['top_ask_timestamp'] = data['data']['timestamp']
            elif new_ask[1] != '0':
                new_ob['asks'][new_ask[0]] = new_ask[1]
        self.orderbook[symbol] = new_ob
        if flag and ts_ms - ts_ob < 0.035:
            coin = symbol.split('PFC')[0]
            if self.state == 'Bot':
                await self.finder.count_one_coin(coin, self.EXCHANGE_NAME, side, self.multibot.run_arbitrage)
            else:
                await self.finder.count_one_coin(coin, self.EXCHANGE_NAME, side)
        # elif ts_ms - self.last_keep_alive > 15:
        #     self.last_keep_alive = ts_ms
        #     self.amount = self.instruments[symbol]['min_size']
        #     tick = self.instruments[symbol]['tick_size']
        #     self.fit_sizes(new_ob['top_bid'][0] - (50 * tick), symbol)
        #     order = await self.create_fast_order(symbol, 'buy')
        #     print(f"Create {self.EXCHANGE_NAME} keep-alive order time: {order['timestamp'] - ts_ms}")
        #     self.LAST_ORDER_ID = 'default'
        #     await self.cancel_order(symbol, order['exchange_order_id'], self.async_session)

    @try_exc_async
    async def update_orderbook_snapshot(self, data):
        symbol = data['data']['symbol']
        self.orderbook[symbol] = {'asks': {x[0]: x[1] for x in data['data']['asks']},
                                  'bids': {x[0]: x[1] for x in data['data']['bids']},
                                  'timestamp': data['data']['timestamp'],
                                  'top_ask': [float(data['data']['asks'][0][0]), float(data['data']['asks'][0][1])],
                                  'top_bid': [float(data['data']['bids'][0][0]), float(data['data']['bids'][0][1])],
                                  'top_ask_timestamp': data['data']['timestamp'],
                                  'top_bid_timestamp': data['data']['timestamp'],
                                  'ts_ms': time.time()}

    @try_exc_regular
    def get_last_price(self, side: str) -> float:
        return self.last_price[side]

    @try_exc_regular
    def get_orderbook(self, symbol) -> dict:
        if not self.orderbook.get(symbol):
            return {}
        snap = self.orderbook[symbol].copy()
        if isinstance(snap['asks'], list):
            return snap
        if snap['top_ask'][0] <= snap['top_bid'][0]:
            print(f"ALARM! ORDERBOOK ERROR {self.EXCHANGE_NAME}: {snap}")
            return {}
        c_v = self.instruments[symbol]['contract_value']
        ob = {'timestamp': self.orderbook[symbol]['timestamp'],
              'asks': [[float(x), float(snap['asks'][x]) * c_v] for x in sorted(snap['asks'])[:self.ob_len]],
              'bids': [[float(x), float(snap['bids'][x]) * c_v] for x in sorted(snap['bids'])[::-1][:self.ob_len]],
              'top_ask_timestamp': self.orderbook[symbol]['top_ask_timestamp'],
              'top_bid_timestamp': self.orderbook[symbol]['top_bid_timestamp'],
              'ts_ms': self.orderbook[symbol]['ts_ms']}
        return ob

    @try_exc_async
    async def get_orderbook_by_symbol(self, symbol):
        async with aiohttp.ClientSession() as session:
            path = f'/api/v2.1/orderbook'
            params = {'symbol': symbol, 'depth': 10}
            post_string = '?' + "&".join([f"{key}={params[key]}" for key in sorted(params)])
            # headers = self.get_private_headers(path, {})  # Assuming authentication is required
            async with session.get(url=self.BASE_URL + path + post_string, headers=self.headers, data=params) as resp:
                ob = await resp.json()
                contract_value = self.instruments[symbol]['contract_value']
                if 'buyQuote' in ob and 'sellQuote' in ob:
                    orderbook = {
                        'timestamp': ob['timestamp'],
                        'asks': [[float(ask['price']), float(ask['size']) * contract_value] for ask in ob['sellQuote']],
                        'bids': [[float(bid['price']), float(bid['size']) * contract_value] for bid in ob['buyQuote']]
                    }
                    return orderbook

    @try_exc_regular
    def run_updater(self):
        self.wst_public.daemon = True
        self.wst_public.start()
        while set(self.orderbook) < set([self.markets[x] for x in self.markets_list if self.markets.get(x)]):
            # print(f'{self.EXCHANGE_NAME} waiting for full ob')
            time.sleep(0.1)
        if self.state == 'Bot':
            self.orders_thread.daemon = True
            self.orders_thread.start()
        # self._wst_orderbooks.daemon = True
        # self._wst_orderbooks.start()
        # if self.state == 'Bot':
        #     self.wst_private.daemon = True
        #     self.wst_private.start()

    @try_exc_regular
    def get_fills(self, symbol: str, order_id: str):
        path = '/api/v2.1/user/trade_history'
        params = {'orderID': order_id,
                  'symbol': symbol}
        final_path = path + f"?orderID={order_id}&symbol={symbol}"
        self.get_private_headers(path, params)
        response = self.session.get(url=self.BASE_URL + final_path, json=params)
        if response.status_code in ['200', 200, '201', 201]:
            data = response.json()
            print(data)
        else:
            print(f"ERROR IN GET_FILLS RESPONSE BTSE: {response.text=}")

    @try_exc_async
    async def create_aiohttp_order(self, symbol, side, session):
        path = '/api/v2.1/order'
        contract_value = self.instruments[symbol]['contract_value']
        body = {"symbol": symbol,
                "side": side.upper(),
                "price": self.price,
                "type": "LIMIT",
                'size': int(self.amount / contract_value)}
        self.get_private_headers(path, body)
        time_start = time.time()
        async with session.post(url=self.BASE_URL + path, headers=self.session.headers, json=body) as resp:
            response = await resp.json()
            # print(f"{self.EXCHANGE_NAME} ORDER CREATE RESPONSE AIOHTTP: {response}")
            # print(f"ORDER PLACING TIME AIOHTTP: {time.time() - time_start}")
            # print()
            self.aver_time_aiohttp.append(time.time() - time_start)
            self.aver_time_aiohttp_response.append(response[0]['timestamp'] / 1000 - time_start)
            return {'exchange_name': self.EXCHANGE_NAME,
                    'exchange_order_id': response[0].get('orderID', 'default')}

    @try_exc_async
    async def create_httpx_order(self, symbol, side, session):
        path = '/api/v2.1/order'
        contract_value = self.instruments[symbol]['contract_value']
        body = {"symbol": symbol,
                "side": side.upper(),
                "price": self.price,
                "type": "LIMIT",
                'size': int(self.amount / contract_value)}
        self.get_private_headers(path, body)
        time_start = time.time()
        response = await session.post(url=self.BASE_URL + path, headers=self.session.headers, json=body)
        response = response.json()
        # print(f"{self.EXCHANGE_NAME} ORDER CREATE RESPONSE HTTPX: {response}")
        # print(f"ORDER PLACING TIME HTTPX: {time.time() - time_start}")
        # print()
        self.aver_time_httpx.append(time.time() - time_start)
        self.aver_time_httpx_response.append(response[0]['timestamp'] / 1000 - time_start)
        return {'exchange_name': self.EXCHANGE_NAME,
                'exchange_order_id': response[0].get('orderID', 'default')}

    @try_exc_regular
    def create_requests_order(self, symbol, side):
        path = '/api/v2.1/order'
        contract_value = self.instruments[symbol]['contract_value']
        body = {"symbol": symbol,
                "side": side.upper(),
                "price": self.price,
                "type": "LIMIT",
                'size': int(self.amount / contract_value)}
        self.get_private_headers(path, body)
        time_start = time.time()
        response = self.session.post(url=self.BASE_URL + path, headers=self.session.headers, json=body).json()
        # print(f"{self.EXCHANGE_NAME} ORDER CREATE RESPONSE REQUESTS: {response}")
        # print(f"ORDER PLACING TIME HTTPX: {time.time() - time_start}")
        # print()
        self.aver_time_requests.append(time.time() - time_start)
        self.aver_time_requests_response.append(response[0]['timestamp'] / 1000 - time_start)
        return {'exchange_name': self.EXCHANGE_NAME,
                'exchange_order_id': response[0].get('orderID', 'default')}

    @try_exc_async
    async def cancel_order_httpx(self, symbol: str, order_id: int, session):
        path = '/api/v2.1/order'
        params = {'symbol': symbol,
                  'orderID': order_id}
        self.get_private_headers(path, params)
        path += '?' + "&".join([f"{key}={params[key]}" for key in sorted(params)])
        resp = await session.post(url=self.BASE_URL + path, headers=self.session.headers, json=params)
        resp = resp.json()
        return resp

    @try_exc_async
    async def cancel_order_aiohttp(self, symbol: str, order_id: int, session):
        path = '/api/v2.1/order'
        params = {'symbol': symbol,
                  'orderID': order_id}
        self.get_private_headers(path, params)
        path += '?' + "&".join([f"{key}={params[key]}" for key in sorted(params)])
        async with session.post(url=self.BASE_URL + path, headers=self.session.headers, json=params) as resp:
            resp = await resp.json()
            return resp

    @try_exc_regular
    def cancel_order_requests(self, symbol: str, order_id: int):
        path = '/api/v2.1/order'
        params = {'symbol': symbol,
                  'orderID': order_id}
        self.get_private_headers(path, params)
        path += '?' + "&".join([f"{key}={params[key]}" for key in sorted(params)])
        resp = self.session.post(url=self.BASE_URL + path, headers=self.session.headers, json=params).json()
        return resp


if __name__ == '__main__':
    import configparser

    config = configparser.ConfigParser()
    config.read('config.ini', "utf-8")
    client = BtseClient(keys=config['BTSE'],
                        leverage=float(config['SETTINGS']['LEVERAGE']),
                        max_pos_part=int(config['SETTINGS']['PERCENT_PER_MARKET']),
                        markets_list=['MANA'])

    import aiohttp
    import asyncio

    import httpx


    async def httpx_test_order():
        async with httpx.AsyncClient(http2=True) as session:
            ob = client.get_orderbook('MANAPFC')
            client.amount = client.instruments['MANAPFC']['min_size']
            client.price = ob['bids'][3][0] - (client.instruments['MANAPFC']['tick_size'] * 100)
            response = await client.create_httpx_order('MANAPFC', 'buy', session)
            await client.cancel_order_httpx('MANAPFC', response['exchange_order_id'], session)

    def requests_test_order():
        ob = client.get_orderbook('MANAPFC')
        client.amount = client.instruments['MANAPFC']['min_size']
        client.price = ob['bids'][3][0] - (client.instruments['MANAPFC']['tick_size'] * 100)
        response = client.create_requests_order('MANAPFC', 'buy')
        client.cancel_order_requests('MANAPFC', response['exchange_order_id'])

    async def aiohttp_test_order():
        async with aiohttp.ClientSession() as session:
            ob = client.get_orderbook('MANAPFC')
            client.amount = client.instruments['MANAPFC']['min_size']
            client.price = ob['bids'][3][0] - (client.instruments['MANAPFC']['tick_size'] * 100)
            response = await client.create_aiohttp_order('MANAPFC', 'buy', session)
            await client.cancel_order_httpx('MANAPFC', response['exchange_order_id'], session)

    client.markets_list = list(client.markets.keys())
    client.run_updater()

    client.aver_time_httpx = []
    client.aver_time_httpx_response = []
    client.aver_time_requests = []
    client.aver_time_requests_response = []
    client.aver_time_aiohttp = []
    client.aver_time_aiohttp_response = []
    while True:
        time.sleep(1)
        requests_test_order()
        asyncio.run(aiohttp_test_order())
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
        # print(f"Repeats (1 sec each): {len(client.aver_time_httpx)}")
        # print('OWN HTTPX')
        # print(f"Min order create time: {min(client.aver_time_httpx)} sec")
        # print(f"Max order create time: {max(client.aver_time_httpx)} sec")
        # print(f"Aver. order create time: {sum(client.aver_time_httpx) / len(client.aver_time_httpx)} sec")
        # print('RESPONSE HTTPX')
        # print(f"Min order create time: {min(client.aver_time_httpx_response)} sec")
        # print(f"Max order create time: {max(client.aver_time_httpx_response)} sec")
        # print(f"Aver. order create time: {sum(client.aver_time_httpx_response) / len(client.aver_time_httpx_response)} sec")
        # print()

