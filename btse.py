import random
import time
import traceback
import aiohttp
import json
import requests
from datetime import datetime
import threading
import hmac
import hashlib
from clients.core.enums import ResponseStatus, OrderStatus
from core.wrappers import try_exc_regular, try_exc_async
from clients.core.base_client import BaseClient
import asyncio
import string


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
                 max_pos_part=20, finder=None, ob_len=4, market_finder=None):
        super().__init__()
        self.market_finder = market_finder
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
        self._loop_private = asyncio.new_event_loop()
        self.wst_public = threading.Thread(target=self._run_ws_forever, args=['public', self._loop])
        self.wst_private = threading.Thread(target=self._run_ws_forever, args=['private', self._loop_private])
        self.order_loop = asyncio.new_event_loop()
        self.orders_thread = threading.Thread(target=self.deals_thread_func, args=[self.order_loop])
        self.orderbook = {}
        self.orders = {}
        self.rate_limit_orders = 75
        self.taker_fee = 0.0005 * 0.75
        self.maker_fee = 0.0001 * 0.75
        self.orig_sizes = {}
        self.LAST_ORDER_ID = 'default'
        self.last_keep_alive = 0
        self.last_websocket_ping = 0
        self.async_tasks = []
        self.responses = {}
        self.cancel_responses = {}
        self.deleted_orders = []
        self.top_ws_ping = 0.012
        if multibot:
            self.cancel_all_orders()

    @try_exc_regular
    def deals_thread_func(self, loop):
        while True:
            loop.run_until_complete(self._run_order_loop(loop))

    @try_exc_async
    async def _run_order_loop(self, loop):
        last_request = time.time()
        request_pause = 1.02 / self.rate_limit_orders
        async with aiohttp.ClientSession() as self.async_session:
            self.async_session.headers.update(self.headers)
            while True:
                ts_ms = time.time()
                for task in self.async_tasks:
                    if ts_ms - last_request < request_pause:
                        break
                    elif task[0] == 'create_order':
                        last_request = ts_ms
                        price = task[1]['price']
                        size = task[1]['size']
                        side = task[1]['side']
                        market = task[1]['market']
                        client_id = task[1].get('client_id')
                        loop.create_task(self.create_fast_order(price, size, side, market, client_id))
                    elif task[0] == 'cancel_order':
                        if task[1]['order_id'] not in self.deleted_orders:
                            last_request = ts_ms
                            if len(self.deleted_orders) > 1000:
                                self.deleted_orders = []
                            self.deleted_orders.append(task[1]['order_id'])
                            loop.create_task(self.cancel_order(task[1]['market'], task[1]['order_id']))
                    elif task[0] == 'amend_order':
                        if task[1]['order_id'] not in self.deleted_orders:
                            last_request = ts_ms
                            price = task[1]['price']
                            size = task[1]['size']
                            order_id = task[1]['order_id']
                            market = task[1]['market']
                            old_order_size = task[1]['old_order_size']
                            loop.create_task(self.amend_order(price, size, order_id, market, old_order_size))
                    self.async_tasks.remove(task)
                if ts_ms - self.last_keep_alive > 3:
                    self.last_keep_alive = ts_ms
                    loop.create_task(self.keep_alive_order())
                    loop.create_task(self.get_balance_async())
                    if self.market_finder:
                        loop.create_task(self.check_extra_orders())
                await asyncio.sleep(0.0001)

    @staticmethod
    @try_exc_regular
    def id_generator(size=6, chars=string.ascii_letters):
        return ''.join(random.choice(chars) for _ in range(size))

    @try_exc_async
    async def keep_alive_order(self):
        market = self.markets[self.markets_list[random.randint(0, len(self.markets_list) - 1)]]
        price = self.get_orderbook(market)['bids'][0][0] * 0.95
        price, size = self.fit_sizes(price, self.instruments[market]['min_size'], market)
        rand_id = self.id_generator()
        await self.create_fast_order(price, size, 'buy', market, 'keepxxxalivexxx' + rand_id)
        resp = self.responses.get('keepxxxalivexxx' + rand_id)
        ex_order_id = resp['exchange_order_id']
        await self.cancel_order(market, ex_order_id)

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
    def _run_ws_forever(self, ws_type, loop):
        while True:
            loop.run_until_complete(self._run_ws_loop(ws_type, loop))

    @try_exc_regular
    def generate_signature(self, path, nonce, data=''):
        language = "latin-1"
        message = path + nonce + data
        signature = hmac.new(bytes(self.api_secret, language),
                             msg=bytes(message, language),
                             digestmod=hashlib.sha384).hexdigest()
        return signature

    @try_exc_regular
    def get_private_headers(self, path, data=dict()):
        json_data = json.dumps(data) if data else ''
        nonce = str(int(time.time() * 1000) + random.randint(-100, 100))
        signature = self.generate_signature(path, nonce, json_data)
        self.session.headers.update({"request-api": self.api_key,
                                     "request-nonce": nonce,
                                     "request-sign": signature})

    @try_exc_async
    async def get_balance_async(self):
        self.get_real_balance()

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
            self.positions = {}
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
                                                       'amount_usd': size_usd,
                                                       }})

    @try_exc_regular
    def get_position(self):
        path = "/api/v2.1/user/positions"
        self.get_private_headers(path)
        response = self.session.get(self.BASE_URL + path)
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
                                                       'amount_usd': size_usd,
                                                       'unrealised_pnl': pos['unrealizedProfitLoss']}})
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

    @try_exc_async
    async def amend_order(self, price, sz, order_id, market, old_order_size):
        time_start = time.time()
        path = '/api/v2.1/order'
        body = {"symbol": market,
                "orderID": order_id,
                "type": 'PRICE' if old_order_size == sz else 'ALL'}
        if body['type'] == 'ALL':
            contract_value = self.instruments[market]['contract_value']
            body.update({"orderSize": int(sz / contract_value),
                         "orderPrice": price})
        else:
            body.update({"value": price})
        self.get_private_headers(path, body)
        async with self.async_session.put(url=self.BASE_URL + path, headers=self.session.headers, json=body) as resp:
            try:
                response = await resp.json()
            except:
                return
            if isinstance(response, dict):
                return
            # print(f"{self.EXCHANGE_NAME} ORDER AMEND PING: {response[0]['timestamp'] / 1000 - time_start}")
            # print(f"ORDER AMEND: {response}")
            status = self.get_order_response_status(response)
            self.LAST_ORDER_ID = response[0].get('orderID', 'default')
            self.orig_sizes.update({self.LAST_ORDER_ID: response[0].get('originalSize')})
            order_res = {'exchange_name': self.EXCHANGE_NAME,
                         'exchange_order_id': self.LAST_ORDER_ID,
                         'timestamp': response[0]['timestamp'] / 1000 if response[0].get('timestamp') else time.time(),
                         'status': status,
                         'api_response': response,
                         'size': response[0]['fillSize'],
                         'price': response[0]['avgFillPrice'],
                         'create_order_time': response[0]['timestamp'] / 1000 - time_start}
            if response[0].get("clOrderID"):
                self.responses.update({response[0]["clOrderID"]: order_res})
            else:
                self.responses.update({response[0]['orderId']: order_res})
            # self.last_keep_alive = order_res['timestamp']
            # res_example = [{'status': 2, 'symbol': 'BTCPFC', 'orderType': 76, 'price': 43490, 'side': 'BUY', 'size': 1,
            #             'orderID': '13a82711-f6e2-4228-bf9f-3755cd8d7885', 'timestamp': 1703535543583,
            #             'triggerPrice': 0, 'trigger': False, 'deviation': 100, 'stealth': 100, 'message': '',
            #             'avgFillPrice': 0, 'fillSize': 0, 'clOrderID': '', 'originalSize': 1, 'postOnly': False,
            #             'remainingSize': 1, 'orderDetailType': None, 'positionMode': 'ONE_WAY',
            #             'positionDirection': None, 'positionId': 'BTCPFC-USD', 'time_in_force': 'GTC'}]

    @try_exc_async
    async def create_fast_order(self, price, sz, side, market, client_id=None):
        time_start = time.time()
        path = '/api/v2.1/order'
        contract_value = self.instruments[market]['contract_value']
        sz = int(sz / contract_value)
        body = {"symbol": market,
                "side": side.upper(),
                'size': sz if sz else 1}
        if 'taker' in client_id:
            body.update({"type": "MARKET"})
        else:
            body.update({"price": price,
                         'type': 'LIMIT'})
        # if client_id:
        #     body.update({'clOrderID': client_id})
        # print(f"{self.EXCHANGE_NAME} SENDING ORDER: {body}")
        self.get_private_headers(path, body)
        async with self.async_session.post(url=self.BASE_URL + path, headers=self.session.headers, json=body) as resp:
            try:
                response = await resp.json()
                # if 'keep' not in response[0].get("clOrderID", ''):
                print(body)
                print(f"{self.EXCHANGE_NAME} ORDER CREATE RESPONSE: {response}")
                print(f"{self.EXCHANGE_NAME} ORDER CREATE PING: {response[0]['timestamp'] / 1000 - time_start}")
                print()
            except Exception:
                # if self.EXCHANGE_NAME != self.multibot.mm_exchange:
                print(body)
                traceback.print_exc()
                print(resp, '\n\n')
            if isinstance(response, list):
                status = self.get_order_response_status(response)
                self.orig_sizes.update({self.LAST_ORDER_ID: response[0].get('originalSize')})
                order_res = {'exchange_name': self.EXCHANGE_NAME,
                             'exchange_order_id': response[0].get('orderID', 'default'),
                             'timestamp': response[0]['timestamp'] / 1000 if response[0].get(
                                 'timestamp') else time.time(),
                             'status': status,
                             'api_response': response[0],
                             'size': response[0]['fillSize'] * self.instruments[market]['contract_value'],
                             'price': response[0]['avgFillPrice'],
                             'time_order_sent': time_start,
                             'create_order_time': response[0]['timestamp'] / 1000 - time_start}
                if client_id:
                    self.responses.update({client_id: order_res})
                    self.LAST_ORDER_ID = response[0].get('orderID', 'default')
                else:
                    self.responses.update({response[0]['orderID']: order_res})
            else:
                print(response)
            # res_example = [{'status': 2, 'symbol': 'BTCPFC', 'orderType': 76, 'price': 43490, 'side': 'BUY', 'size': 1,
            #             'orderID': '13a82711-f6e2-4228-bf9f-3755cd8d7885', 'timestamp': 1703535543583,
            #             'triggerPrice': 0, 'trigger': False, 'deviation': 100, 'stealth': 100, 'message': '',
            #             'avgFillPrice': 0, 'fillSize': 0, 'clOrderID': '', 'originalSize': 1, 'postOnly': False,
            #             'remainingSize': 1, 'orderDetailType': None, 'positionMode': 'ONE_WAY',
            #             'positionDirection': None, 'positionId': 'BTCPFC-USD', 'time_in_force': 'GTC'}]

    @try_exc_async
    async def create_order(self, symbol, side, price, size, session, expire=10000, client_id=None, expiration=None):
        path = '/api/v2.1/order'
        contract_value = self.instruments[symbol]['contract_value']
        body = {"symbol": symbol,
                "side": side.upper(),
                "price": price,
                "type": "LIMIT",
                'size': int(size / contract_value)}
        self.get_private_headers(path, body)
        async with session.post(url=self.BASE_URL + path, headers=self.session.headers, json=body) as resp:
            res = await resp.json()
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
        else:
            print(f"ERROR IN GET_ORDER_BY_ID RESPONSE BTSE: {response.text=}")
            order_data = {}
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
        tot_unrealised_pnl = sum([x['unrealised_pnl'] for x in self.positions.values()])
        return self.balance['total'] + tot_unrealised_pnl

    @try_exc_regular
    def get_available_balance(self):
        return super().get_available_balance(
            leverage=self.leverage,
            max_pos_part=self.max_pos_part,
            positions=self.positions,
            balance=self.balance)

    @try_exc_async
    async def _run_ws_loop(self, ws_type, loop):
        async with aiohttp.ClientSession() as s:
            if ws_type == 'private':
                endpoint = self.PRIVATE_WS_ENDPOINT
            else:
                endpoint = self.PUBLIC_WS_ENDPOINT
            async with s.ws_connect(endpoint) as ws:
                print(f"BTSE: connected {ws_type}")
                self._connected.set()
                if ws_type == 'private':
                    self._ws_private = ws
                    await loop.create_task(self.subscribe_privates())
                else:
                    self._ws_public = ws
                    await loop.create_task(self.subscribe_orderbooks())
                loop.create_task(self._ping(ws))
                async for msg in ws:
                    data = json.loads(msg.data)
                    if data.get('topic') == 'fills':
                        own_ts = time.time()
                        await self.upd_fills(data, own_ts)
                    loop.create_task(self.process_ws_msg(data))
            await ws.close()

    @try_exc_async
    async def process_ws_msg(self, data):
        if 'update' in data.get('topic', ''):
            if data.get('data') and data['data']['type'] == 'delta':
                await self.upd_ob(data)
            elif data.get('data') and data['data']['type'] == 'snapshot':
                await self.upd_ob_snapshot(data)
        elif data.get('topic') == 'allPosition':
            await self.upd_positions(data)
        else:
            print(data)

    @try_exc_async
    async def _ping(self, ws):
        while True:
            await asyncio.sleep(25)  # Adjust the ping interval as needed
            await ws.ping()
        # print(f'PING SENT: {datetime.utcnow()}')

    @try_exc_async
    async def check_extra_orders(self):
        orders = self.get_all_orders()
        # all_legit_orders = [x[0] for x in self.multibot.open_orders.values()]
        all_markets = {}
        for order in orders:
            if saved := all_markets.get(order['symbol']):
                ord = order if order['timestamp'] < saved['timestamp'] else saved
                keep = order if order != ord else saved
                self.async_tasks.append(['cancel_order', {'market': order['symbol'], 'order_id': ord['orderID']}])
                print(f"ALERT: NON-LEGIT ORDER: {order}")
                all_markets.update({order['symbol']: keep})
            else:
                all_markets.update({order['symbol']: order})

    @try_exc_regular
    def get_all_orders(self):
        path = '/api/v2.1/user/open_orders'
        self.get_private_headers(path, {})
        return self.session.get(url=self.BASE_URL + path).json()
        # example = [{'vendorName': None, 'botID': None, 'orderType': 76, 'price': 0.02562, 'size': 11900, 'side': 'BUY',
        #             'filledSize': 0, 'orderValue': 304.878, 'pegPriceMin ': 0, 'pegPriceMax': 0, 'pegPriceDeviation': 1,
        #             'cancelDuration': 0, 'timestamp': 1706981603805,
        #             'orderID': '0cc00ce1-eeab-4251-b859-4f4eefb0d04d', 'stealth': 1, 'triggerOrder': False,
        #             'triggered': False, 'triggerPrice': 0, 'triggerOriginalPrice': 0, 'triggerOrderType': 0,
        #             'triggerTrailingStopDeviation': 0, 'triggerStopPrice': 0, 'symbol': 'PEOPLEPFC',
        #             'trailValue': 0, 'remainingSize': 11900, 'clOrderID': 'makerxxxBTSExxxPEOPLExxxNKTWVI',
        #             'reduceOnly': False, 'orderState': 'STATUS_ACTIVE', 'triggerUseLastPrice': False,
        #             'avgFilledPrice': 0, 'timeInForce': 'GTC', 'orderDetailType': None, 'takeProfitOrder': None,
        #             'stopLossOrder': None, 'closeOrder': False, 'positionMode': 'ONE_WAY', 'positionDirection': None,
        #             'positionId': 'PEOPLEPFC-USD'},
        #            {'vendorNa    me': None, 'botID': None, 'orderType': 76, 'price': 113.73, 'size': 26, 'side': 'BUY',
        #             'filledSize': 0, 'orderValue': 295.698, 'pegPriceMin': 0, 'pegPriceMax': 0, 'pegPriceDeviation': 1,
        #             'cancelDuration': 0, 'timestamp': 1706981594980,
        #             'orderID': 'f0a6a3ea-13b0-4442-9e2c-d1bc73c9884b', 'stealth': 1, 'triggerOrder': False,
        #             'triggered': False, 'triggerPrice': 0, 'triggerOriginalPrice': 0, 'triggerOrderType': 0,
        #             'triggerTrailingStopDeviation': 0, 'triggerStopPrice': 0, 'symbol': 'TRBPFC', 'trailValue': 0,
        #             'remainingSize': 26, 'clOrderID': 'makerxxxBTSExxxTRBxxxWcjnVB', 'reduceOnly': False,
        #             'orderState': 'STATUS_ACTIVE', 'triggerUseLastPrice': False, 'avgFilledPrice': 0,
        #             'timeInForce': 'GTC', 'orderDetailType': None, 'takeProfitOrder': None, 'stopLossOrder': None,
        #             'closeOrder': False, 'positionMode': 'ONE_WAY', 'positionDirection': None,
        #             'positionId': 'TRBPFC-USD'}]

    @try_exc_async
    async def cancel_order(self, symbol: str, order_id: str):
        path = '/api/v2.1/order'
        data = {'symbol': symbol,
                'orderID': order_id}
        self.get_private_headers(path, data)
        path += '?' + "&".join([f"{key}={data[key]}" for key in sorted(data)])
        async with self.async_session.delete(url=self.BASE_URL + path, headers=self.session.headers, json=data) as resp:
            try:
                response = await resp.json()
                # if isinstance(response, list):
                # if 'maker' in response[0].get('clOrderID', '') and self.EXCHANGE_NAME == self.multibot.mm_exchange:
                #     coin = symbol.split('PFC')[0]
                #     ord_id = coin + '-' + self.EXCHANGE_NAME
                #     self.multibot.open_orders.pop(ord_id)
                self.cancel_responses.update({order_id: response[0]})
                        # if self.multibot.open_orders.get(ord_id, [''])[0] == response[0]['orderID']:
                        #     self.multibot.dump_orders.update({ord_id: self.multibot.open_orders.pop(ord_id)})
            except:
                print(f'ORDER CANCEL ERROR', resp)
            # else:
            #     print(f'ORDER WAS CANCELED BEFORE {self.EXCHANGE_NAME}', response)

    # example = [{'status': 6, 'symbol': 'TRBPFC', 'orderType': 76, 'price': 118.35, 'side': 'BUY', 'size': 2,
    #       'orderID': 'cfcbcd08-bda4-487a-a261-192e24c31db4', 'timestamp': 1705925467489, 'triggerPrice': 0,
    #       'trigger': False, 'deviation': 100, 'stealth': 100, 'message': '', 'avgFillPrice': 0, 'fillSize': 0,
    #       'clOrderID': 'maker-BTSE-TRB-7089166', 'originalSize': 2, 'postOnly': False, 'remainingSize': 2,
    #       'orderDetailType': None, 'positionMode': 'ONE_WAY', 'positionDirection': None, 'positionId': 'TRBPFC-USD',
    #       'time_in_force': 'GTC'}]

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
    async def upd_fills(self, data, own_ts):
        # print(f"GOT FILL {datetime.utcnow()}")
        loop = asyncio.get_event_loop()
        for fill in data['data']:
            order_id = fill['orderId']
            size = float(fill['size']) * self.instruments[fill['symbol']]['contract_value']
            if 'maker' in fill.get('clOrderId', '') and self.multibot.mm_exchange == self.EXCHANGE_NAME:
                deal = {'side': fill['side'].lower(),
                        'size': size,
                        'coin': fill['symbol'].split('PFC')[0],
                        'price': float(fill['price']),
                        'timestamp': fill['timestamp'] / 1000,
                        'ts_ms': own_ts,
                        'order_id': order_id,
                        'type': 'maker' if fill['maker'] else 'taker'}
                await loop.create_task(self.multibot.hedge_maker_position(deal))
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
        loop.create_task(self.multibot.update_all_av_balances())

        # fills_example = {'topic': 'fills', 'id': '', 'data': [
        #     {'orderId': '04e64f39-b715-44e5-a2b8-e60b3379c2f3', 'serialId': 4260246, 'clOrderId': '', 'type': '76',
        #      'symbol': 'ETHPFC', 'side': 'BUY', 'price': '2238.25', 'size': '1.0', 'feeAmount': '0.01119125',
        #      'feeCurrency': 'USDT', 'base': 'ETHPFC', 'quote': 'USD', 'maker': False, 'timestamp': 1703580743141,
        #      'tradeId': 'd6201931-446d-4a1c-ab83-e3ebdcf2f077'}]}

    @try_exc_async
    async def upd_positions(self, data):
        for pos in data['data']:
            market = pos['marketName'].split('-')[0]
            contract_value = self.instruments[market]['contract_value']
            size = pos['totalContracts'] * contract_value
            size = -size if pos['totalValue'] < 0 else size
            self.positions.update({market: {'timestamp': int(datetime.utcnow().timestamp()),
                                            'entry_price': pos['entryPrice'],
                                            'amount': size,
                                            'amount_usd': pos['totalValue'],
                                            'unrealised_pnl': pos['unrealizedProfitLoss']}})
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
    async def upd_ob(self, data):
        ts_ms = time.time()
        ts_ob = data['data']['timestamp']
        if isinstance(ts_ob, int):
            ts_ob = ts_ob / 1000
        # print(f"OB UPD PING: {ts_ms - ts_ob}")
        flag = False
        flag_market = False
        symbol = data['data']['symbol']
        new_ob = self.orderbook[symbol].copy()
        new_ob['ts_ms'] = ts_ms

        new_ob['timestamp'] = ts_ob
        for new_bid in data['data']['bids']:
            if float(new_bid[0]) >= new_ob['top_bid'][0]:
                new_ob['top_bid'] = [float(new_bid[0]), float(new_bid[1])]
                new_ob['top_bid_timestamp'] = data['data']['timestamp']
                flag = True
                flag_market = True
                side = 'sell'
            if new_ob['bids'].get(new_bid[0]) and new_bid[1] == '0':
                del new_ob['bids'][new_bid[0]]
                if float(new_bid[0]) == new_ob['top_bid'][0] and len(new_ob['bids']):
                    top = sorted(new_ob['bids'])[-1]
                    new_ob['top_bid'] = [float(top), float(new_ob['bids'][top])]
                    new_ob['top_bid_timestamp'] = data['data']['timestamp']
                    flag_market = True
                    side = 'sell'
            elif new_bid[1] != '0':
                new_ob['bids'][new_bid[0]] = new_bid[1]
        for new_ask in data['data']['asks']:
            if float(new_ask[0]) <= new_ob['top_ask'][0]:
                new_ob['top_ask'] = [float(new_ask[0]), float(new_ask[1])]
                new_ob['top_ask_timestamp'] = data['data']['timestamp']
                flag_market = True
                flag = True
                side = 'buy'
            if new_ob['asks'].get(new_ask[0]) and new_ask[1] == '0':
                del new_ob['asks'][new_ask[0]]
                if float(new_ask[0]) == new_ob['top_ask'][0] and len(new_ob['asks']):
                    top = sorted(new_ob['asks'])[0]
                    new_ob['top_ask'] = [float(top), float(new_ob['asks'][top])]
                    new_ob['top_ask_timestamp'] = data['data']['timestamp']
                    flag_market = True
                    side = 'buy'
            elif new_ask[1] != '0':
                new_ob['asks'][new_ask[0]] = new_ask[1]
        self.orderbook[symbol] = new_ob
        if self.market_finder and flag_market:
            await self.market_finder.count_one_coin(symbol.split('PFC')[0], self.EXCHANGE_NAME)
        if flag and ts_ms - ts_ob < self.top_ws_ping and self.finder:
            coin = symbol.split('PFC')[0]
            if self.state == 'Bot':
                await self.finder.count_one_coin(coin, self.EXCHANGE_NAME, side, self.multibot.run_arbitrage)
            else:
                await self.finder.count_one_coin(coin, self.EXCHANGE_NAME, side)

    @try_exc_async
    async def upd_ob_snapshot(self, data):
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
        ob = {'timestamp': snap['timestamp'],
              'asks': sorted([[float(x), y] for x, y in snap['asks'].copy().items()])[:self.ob_len],
              'bids': sorted([[float(x), y] for x, y in snap['bids'].copy().items()])[::-1][:self.ob_len],
              'top_ask_timestamp': snap['top_ask_timestamp'],
              'top_bid_timestamp': snap['top_bid_timestamp'],
              'ts_ms': snap['ts_ms']}
        ob['asks'] = [[x, float(y) * c_v] for x, y in ob['asks']]
        ob['bids'] = [[x, float(y) * c_v] for x, y in ob['bids']]
        return ob

    @try_exc_async
    async def get_orderbook_by_symbol(self, symbol):
        async with aiohttp.ClientSession() as session:
            path = f'/api/v2.1/orderbook'
            params = {'symbol': symbol, 'depth': 10}
            post_string = '?' + "&".join([f"{key}={params[key]}" for key in sorted(params)])
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
        while True:
            if set(self.orderbook) == set([y for x, y in self.markets.items() if x in self.markets_list]):
                print(f"{self.EXCHANGE_NAME} ALL MARKETS FETCHED")
                break
            time.sleep(0.1)
        if self.state == 'Bot':
            self.orders_thread.daemon = True
            self.orders_thread.start()
            self.wst_private.daemon = True
            self.wst_private.start()

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


if __name__ == '__main__':
    import configparser

    config = configparser.ConfigParser()
    config.read('config.ini', "utf-8")
    client = BtseClient(keys=config['BTSE'], state='Bot')
    client.markets_list = list(client.markets.keys())
    client.run_updater()
    time.sleep(3)
    ob = client.get_orderbook('MANAPFC')
    amount = client.instruments['MANAPFC']['min_size']
    price, amount = client.fit_sizes(ob['bids'][0][0] * 0.95, amount, 'MANAPFC')
    while True:
        time.sleep(5)
        client.order_loop.create_task(client.create_fast_order(price, amount, 'buy', 'MANAPFC'))
        client.cancel_all_orders()
        # for symbol in client.markets.values():
        #     print(client.get_orderbook(symbol))

    # print()
