import asyncio
from datetime import datetime
import json
import threading
import time
import urllib.parse
import aiohttp
from bravado.client import SwaggerClient
from bravado.requests_client import RequestsClient

# from config import Config
from clients.core.base_client import BaseClient
from clients.core.enums import ResponseStatus, OrderStatus
from clients.core.APIKeyAuthenticator import APIKeyAuthenticator as Auth
from core.wrappers import try_exc_regular, try_exc_async
import uvloop
import gc
import socket
import aiodns
from aiohttp.resolver import AsyncResolver

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


# Naive implementation of connecting to BitMEX websocket for streaming realtime data.
# The Marketmaker still interacts with this as if it were a REST Endpoint, but now it can get
# much more realtime data without polling the hell out of the API.
#
# The Websocket offers a bunch of data as raw properties right on the object.
# On connect, it synchronously asks for a push of all this data then returns.
# Right after, the MM can start using its data. It will be updated in realtime, so the MM can
# poll really often if it wants.

class BitmexClient(BaseClient):
    BASE_WS = 'wss://ws.bitmex.com/realtime'
    BASE_URL = 'https://www.bitmex.com'
    EXCHANGE_NAME = 'BITMEX'
    MAX_TABLE_LEN = 200
    headers = {"Content-Type": "application/json",
               'Connection': 'keep-alive'}
    LAST_ORDER_ID = 'default'

    def __init__(self, multibot=None, keys=None, leverage=None, state='Bot', markets_list=[],
                 max_pos_part=20, finder=None, ob_len=4, market_finder=None):
        super().__init__()
        self.multibot = multibot
        self.state = state
        self.finder = finder
        self.ob_len = ob_len
        self.market_finder = market_finder
        self.max_pos_part = max_pos_part
        self.markets_list = markets_list
        self.leverage = leverage
        self.api_key = keys['API_KEY']
        self.api_secret = keys['API_SECRET']
        self.subscriptions = ['margin', 'position', 'execution']
        self.orderbook_type = 'orderBook10'  # 'orderBookL2_25'  #

        self.auth = Auth(self.BASE_URL, self.api_key, self.api_secret)
        self.amount = 0
        self.amount_contracts = 0
        self.taker_fee = 0.0005
        self.maker_fee = -0.0001
        self.requestLimit = 1200
        self.price = 0
        self.orders = {}
        self.positions = {}
        self.orderbook = {}
        self.balance = {}
        self.keys = {}
        self.error_info = None
        self.swagger_client = self.swagger_client_init()
        self.commission = self.swagger_client.User.User_getCommission().result()[0]
        self.instruments = self.get_all_instruments()
        self.markets = self.get_markets()
        self.get_real_balance()
        self.time_sent = datetime.utcnow().timestamp()
        self.responses = {}
        self.async_tasks = []
        self.orig_sizes = {}
        self.clients_ids = {}
        self.top_ws_ping = 0.01
        if multibot:
            self.cancel_all_orders()

    @try_exc_async
    async def _run_order_loop(self, loop):
        # last_request = time.time()
        # request_pause = 1.02 / self.rate_limit_orders
        # connector = aiohttp.TCPConnector(family=socket.AF_INET6)
        resolver = AsyncResolver()
        connector = aiohttp.TCPConnector(resolver=resolver, family=socket.AF_INET)
        async with aiohttp.ClientSession(connector=connector) as self.async_session:
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
                    elif task[0] == 'cancel_order':
                        if task[1]['order_id'] not in self.deleted_orders:
                            if len(self.deleted_orders) > 1000:
                                self.deleted_orders = []
                            self.deleted_orders.append(task[1]['order_id'])
                            self.cancel_all_orders()
                    # elif task[0] == 'amend_order':
                    #     if task[1]['order_id'] not in self.deleted_orders:
                    #         price = task[1]['price']
                    #         size = task[1]['size']
                    #         order_id = task[1]['order_id']
                    #         market = task[1]['market']
                    #         old_order_size = task[1]['old_order_size']
                    #         loop.create_task(self.amend_order(price, size, order_id, market, old_order_size))
                    self.async_tasks.remove(task)
                await asyncio.sleep(0.00001)

    @try_exc_async
    async def keep_alive_order(self):
        while True:
            market = self.markets[self.markets_list[0]]
            price = self.get_orderbook(market)['asks'][0][0] * 0.95
            price, size = self.fit_sizes(price, self.instruments[market]['min_size'], market)
            await self.create_fast_order(price, size, 'buy', market, 'keep-alive')
            order_data = self.responses.get('keep-alive')
            if order_data:
                await self.cancel_order(order_data['exchange_order_id'])
                self.responses.__delitem__('keep-alive')
            else:
                self.cancel_all_orders()
            await asyncio.sleep(15)

    @try_exc_regular
    def get_markets(self) -> dict:
        markets = {}
        for symbol, market in self.instruments.items():
            markets.update({market['coin']: symbol})
        return markets

    @try_exc_regular
    def run_updater(self) -> None:
        wst = threading.Thread(target=self._run_ws_forever, args=[asyncio.new_event_loop(), self._run_ws_loop])
        wst.daemon = True
        wst.start()
        time.sleep(2)
        if self.state == 'Bot':
            wst = threading.Thread(target=self._run_ws_forever, args=[asyncio.new_event_loop(), self._run_order_loop])
            wst.daemon = True
            wst.start()
        # self.__wait_for_account()
        # self.get_contract_value()

    @try_exc_regular
    def get_fees(self, symbol: str) -> (float, float):
        taker_fee = self.commission[symbol]['takerFee']
        maker_fee = self.commission[symbol]['makerFee']
        return taker_fee, maker_fee

    @staticmethod
    @try_exc_regular
    def get_quantity_precision(step_size: float) -> int:
        if '.' in str(step_size):
            quantity_precision = len(str(step_size).split('.')[1])
        elif '-' in str(step_size):
            quantity_precision = int(str(step_size).split('-')[1])
        else:
            quantity_precision = 1
        return quantity_precision

    @try_exc_regular
    def get_all_instruments(self) -> dict:
        refactored_instruments = {}
        instruments = self.swagger_client.Instrument.Instrument_get(
            filter=json.dumps({'quoteCurrency': 'USDT', 'state': 'Open'})).result()
        for instr in instruments[0]:
            if '2' in instr['symbol'] or '_' in instr['symbol']:
                continue
            contract_value = instr['underlyingToPositionMultiplier']
            price_precision = self.get_price_precision(instr['tickSize'])
            step_size = instr['lotSize'] / contract_value
            quantity_precision = self.get_quantity_precision(step_size)
            refactored_instruments.update({instr['symbol']: {'tick_size': instr['tickSize'],
                                                             'price_precision': price_precision,
                                                             'step_size': instr['lotSize'] / contract_value,
                                                             'min_size': instr['lotSize'] / contract_value,
                                                             'quantity_precision': quantity_precision,
                                                             'contract_value': contract_value,
                                                             'coin': instr['rootSymbol']
                                                             }})
        return refactored_instruments

    @staticmethod
    @try_exc_regular
    def get_price_precision(tick_size: float) -> int:
        if '.' in str(tick_size):
            price_precision = len(str(tick_size).split('.')[1])
        elif '-' in str(tick_size):
            price_precision = int(str(tick_size).split('-')[1])
        else:
            price_precision = 0
        return price_precision

    @try_exc_regular
    def _run_ws_forever(self, loop: asyncio.new_event_loop, call: callable) -> None:
        while True:
            try:
                loop.run_until_complete(call(loop))
            finally:
                print("WS loop completed. Restarting")

    @try_exc_async
    async def _run_ws_loop(self, loop) -> None:
        async with aiohttp.ClientSession(headers=self.__get_auth('GET', '/realtime')) as s:
            async with s.ws_connect(self.__get_url()) as ws:
                loop.create_task(self._ping(ws))
                print("Bitmex: connected")
                async for msg in ws:
                    loop.create_task(self._process_msg(msg))
                await ws.close()

    @try_exc_async
    async def _ping(self, ws):
        while True:
            await asyncio.sleep(25)  # Adjust the ping interval as needed
            await ws.ping()

    @staticmethod
    @try_exc_regular
    def get_order_status(order: dict) -> str:
        if order['ordStatus'] == 'New':
            status = OrderStatus.PROCESSING
        elif order['ordStatus'] == 'Filled':
            status = OrderStatus.FULLY_EXECUTED
        elif order['ordStatus'] == 'Canceled' and order['cumQty']:
            status = OrderStatus.PARTIALLY_EXECUTED
        elif order['ordStatus'] == 'Canceled' and not order['cumQty']:
            status = OrderStatus.NOT_EXECUTED
        else:
            status = OrderStatus.PARTIALLY_EXECUTED
        return status

    @try_exc_regular
    def get_all_tops(self) -> dict:
        # NECESSARY
        tops = {}
        for symbol, orderbook in self.orderbook.items():
            coin = symbol.upper().split('USD')[0]
            if len(orderbook['bids']) & len(orderbook['asks']):
                tops.update({self.EXCHANGE_NAME + '__' + coin: {
                    'top_bid': orderbook['bids'][0][0], 'top_ask': orderbook['asks'][0][0],
                    'bid_vol': orderbook['bids'][0][1], 'ask_vol': orderbook['asks'][0][1],
                    'ts_exchange': orderbook['timestamp']
                }})
        return tops

    @try_exc_async
    async def get_all_orders(self, symbol: str, session: aiohttp.ClientSession) -> list:
        res = self.swagger_client.Order.Order_getOrders(filter=json.dumps({'symbol': symbol})).result()[0]
        contract_value = self.get_contract_value(symbol)
        orders = []
        for order in res:
            if res.get('ordStatus') == 'Filled':
                status = OrderStatus.FULLY_EXECUTED
            elif res['orderQty'] > res['cumQty']:
                status = OrderStatus.PARTIALLY_EXECUTED
            else:
                status = OrderStatus.NOT_EXECUTED
            real_size = res['cumQty'] / contract_value
            expect_size = res['orderQty'] / contract_value
            real_price = res.get('avgPx') if isinstance(res.get('avgPx'), float) else 0
            expect_price = res.get('price', 0)
            orders.append(
                {
                    'id': uuid.uuid4(),
                    'datetime': datetime.strptime(order['transactTime'], '%Y-%m-%dT%H:%M:%SZ'),
                    'ts': int(datetime.utcnow().timestamp()),
                    'context': 'web-interface' if 'api_' not in order['clOrdID'] else order['clOrdID'].split('_')[1],
                    'parent_id': uuid.uuid4(),
                    'exchange_order_id': order['orderID'],
                    'type': order['timeInForce'],
                    'status': status,
                    'exchange_name': self.EXCHANGE_NAME,
                    'side': order['side'].lower(),
                    'symbol': symbol,
                    'expect_price': expect_price,
                    'expect_amount_coin': expect_size,
                    'expect_amount_usd': expect_price * expect_size,
                    'expect_fee': self.taker_fee,
                    'factual_price': real_price,
                    'factual_amount_coin': real_size,
                    'factual_amount_usd': real_size * real_price,
                    'factual_fee': self.taker_fee,
                    'order_place_time': 0,
                    'env': '-',
                    'datetime_update': datetime.utcnow(),
                    'ts_update': int(datetime.utcnow().timestamp()),
                    'client_id': order['clientId']
                }
            )
        return orders

    @try_exc_regular
    def get_order_by_id(self, symbol: str, order_id: str) -> dict:
        res = self.swagger_client.Order.Order_getOrders(filter=json.dumps({'orderID': order_id}))
        if len(res.result()[0]):
            response = res.result()[0][0]
            contract_value = self.get_contract_value(symbol)
            real_size = response['cumQty'] / contract_value
            real_price = response.get('avgPx') if isinstance(response.get('avgPx'), float) else 0
            return {
                'exchange_order_id': order_id,
                'exchange_name': self.EXCHANGE_NAME,
                'status': OrderStatus.FULLY_EXECUTED if response.get('ordStatus') == 'Filled' else OrderStatus.NOT_EXECUTED,
                'factual_price': real_price,
                'factual_amount_coin': real_size,
                'factual_amount_usd': real_price * real_size,
                'datetime_update': datetime.utcnow(),
                'ts_update': int(datetime.utcnow().timestamp() * 1000)
            }
        else:
            return {
                'exchange_order_id': order_id,
                'exchange_name': self.EXCHANGE_NAME,
                'status': OrderStatus.NOT_EXECUTED,
                'factual_price': 0,
                'factual_amount_coin': 0,
                'factual_amount_usd': 0,
                'datetime_update': datetime.utcnow(),
                'ts_update': int(datetime.utcnow().timestamp() * 1000)
            }

    @try_exc_regular
    def get_order_result(self, order: dict) -> dict:
        factual_price = order.get('avgPx') if isinstance(order.get('avgPx'), float) else 0
        contract_value = self.get_contract_value(order['symbol'])
        factual_size_coin = order['cumQty'] / contract_value
        factual_size_usd = factual_size_coin * factual_price
        if factual_size_coin and self.multibot:
            loop = asyncio.get_event_loop()
            loop.create_task(self.multibot.update_all_av_balances())
        status = self.get_order_status(order)
        timestamp = self.timestamp_from_date(order['transactTime'])
        result = {
            'exchange_order_id': order['orderID'],
            'exchange_name': self.EXCHANGE_NAME,
            'status': status,
            'factual_price': factual_price,
            'price': factual_price,
            'limit_price': order['price'],
            'factual_amount_coin': factual_size_coin,
            'size': factual_size_coin,
            'factual_amount_usd': factual_size_usd,
            'datetime_update': datetime.utcnow(),
            'ts_update': int(round(datetime.utcnow().timestamp() * 1000)),
            'api_response': order,
            'timestamp': timestamp,
            'time_order_sent': self.time_sent,
            'create_order_time': timestamp - self.time_sent}
        return result

    @try_exc_async
    async def _process_msg(self, msg: aiohttp.WSMessage) -> None:
        if msg.type == aiohttp.WSMsgType.TEXT:
            message = json.loads(msg.data)
            if message.get('subscribe'):
                print(message)
            if message.get("action"):
                if message['table'] == 'execution':
                    await self.update_fills(message['data'])
                elif message['table'] == self.orderbook_type:
                    await self.update_orderbook(message['data'])
                elif message['table'] == 'position':
                    await self.update_positions(message['data'])
                elif message['table'] == 'margin':
                    await self.update_balance(message['data'])

    @try_exc_async
    async def update_positions(self, data: dict) -> None:
        for position in data:
            if position.get('foreignNotional'):
                side = 'SHORT' if position['foreignNotional'] > 0 else 'LONG'
                amount = -position['currentQty'] if side == 'SHORT' else position['currentQty']
                price = position['avgEntryPrice'] if position.get('avgEntryPrice') else 0
                self.positions.update({position['symbol']: {'side': side,
                                                            'amount_usd': -position['foreignNotional'],
                                                            'amount': amount / (10 ** 6),
                                                            'entry_price': price,
                                                            'unrealized_pnl_usd': 0,
                                                            'realized_pnl_usd': 0,
                                                            'lever': self.leverage}})

    @try_exc_async
    async def update_orderbook(self, data: dict) -> None:
        for ob in data:
            if market := ob.get('symbol'):
                side = None
                if self.instruments.get(market) and market.split('USD')[0] in self.markets_list:
                    ts_ob = self.timestamp_from_date(ob['timestamp'])
                    ts_ms = time.time()
                    # print(ts_ms - ts_ob)
                    # return
                    ob.update({'timestamp': ts_ob, 'ts_ms': ts_ms})
                    contract_value = self.get_contract_value(market)
                    ob['bids'] = [[x[0], x[1] / contract_value] for x in ob['bids']]
                    ob['asks'] = [[x[0], x[1] / contract_value] for x in ob['asks']]
                    if self.finder and self.orderbook.get(market):
                        last_ob = self.get_orderbook(market).copy()
                        if ob['asks'][0][0] > last_ob['asks'][0][0]:
                            side = 'buy'
                        elif ob['bids'][0][0] < last_ob['bids'][0][0]:
                            side = 'sell'
                    self.orderbook.update({market: ob})
                    if side:
                        coin = market.split('USDT')[0]
                        if self.state == 'Bot' and ts_ms - ts_ob < self.top_ws_ping:
                            await self.finder.count_one_coin(coin, self.EXCHANGE_NAME, side, 'ob')
                    # #     else:

    @try_exc_async
    async def update_fills(self, data: dict) -> None:
        for order in data:
            if order.get('cumQty'):
                print(f"{self.EXCHANGE_NAME} FILL: {data}")
                result = self.get_order_result(order)
                self.orders.update({order['orderID']: result})
                if client_id := self.clients_ids.get(order['orderID']):
                    if self.responses.get(client_id):
                        self.responses[client_id].update(result)
                    else:
                        self.responses.update({client_id: result})
        # example_blank = [{'execID': '00000000-006d-1000-0000-000509ada326', 'orderID': 'ef99ee87-f7c1-405e-873c-cf1513595c5a',
        #             'account': 2127720, 'symbol': 'ETHUSDT', 'side': 'Buy', 'orderQty': 1000, 'price': 3171.73,
        #             'currency': 'USDT', 'settlCurrency': 'USDt', 'execType': 'New', 'ordType': 'Limit',
        #             'timeInForce': 'GoodTillCancel', 'ordStatus': 'New', 'workingIndicator': True, 'leavesQty': 1000,
        #             'cumQty': 0, 'text': 'Submitted via API.', 'transactTime': '2024-02-28T13:09:40.041Z',
        #             'timestamp': '2024-02-28T13:09:40.041Z'}]

    @try_exc_async
    async def update_balance(self, data: dict) -> None:
        for balance in data:
            if balance['currency'] == 'USDt' and balance.get('marginBalance'):
                self.balance = {'free': balance.get('availableMargin', 0) / (10 ** 6),
                                'total': balance['marginBalance'] / (10 ** 6),
                                'timestamp': datetime.utcnow().timestamp()}

    @staticmethod
    @try_exc_regular
    def timestamp_from_date(date: str) -> float:
        # date = '2023-02-15T02:55:27.640Z'
        ms = int(date.split(".")[1].split('Z')[0]) / 1000
        return time.mktime(datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ").timetuple()) + ms

    @staticmethod
    @try_exc_regular
    def get_pos_power(symbol: str) -> (int, str):
        pos_power = 6 if 'USDT' in symbol else 8
        currency = 'USDt' if 'USDT' in symbol else 'XBt'
        return pos_power, currency

    @try_exc_regular
    def swagger_client_init(self, swagger_config: dict = None) -> SwaggerClient:
        if swagger_config is None:
            # See full config options at http://bravado.readthedocs.io/en/latest/configuration.html
            swagger_config = {
                # Don't use models (Python classes) instead of dicts for #/definitions/{models}
                'use_models': False,
                # bravado has some issues with nullable fields
                'validate_responses': False,
                # Returns response in 2-tuple of (body, response); if False, will only return body
                'also_return_response': True,
            }
        spec_uri = self.BASE_URL + '/api/explorer/swagger.json'
        request_client = RequestsClient()
        request_client.authenticator = self.auth
        return SwaggerClient.from_url(spec_uri, config=swagger_config, http_client=request_client)

    @try_exc_regular
    def get_balance(self) -> float:
        """Get your margin details."""
        return self.balance['total']

    @try_exc_regular
    def fit_sizes(self, price: float, amount: float, market: str) -> (float, float):
        # NECESSARY
        instr = self.instruments[market]
        tick_size = instr['tick_size']
        quantity_precision = instr['quantity_precision']
        price_precision = instr['price_precision']
        amount = round(amount, quantity_precision)
        rounded_price = round(price / tick_size) * tick_size
        price = round(rounded_price, price_precision)
        return price, amount

    @try_exc_async
    async def create_fast_order(self, price: float, sz: float, side: str, market: str, client_id: str = None):
        self.time_sent = time.time()
        path = "/api/v1/order"
        contract_value = self.get_contract_value(market)
        body = {"symbol": market,
                "ordType": "Limit",
                "price": price,
                "orderQty": int(sz * contract_value),
                "side": side.capitalize()}
        headers_body = '&'.join([x + '=' + str(y) for x, y in body.items()])
        headers = self.__get_auth("POST", path, headers_body)
        self.async_session.headers.update(headers)
        async with self.async_session.post(url=self.BASE_URL + path, headers=headers, data=headers_body) as resp:
            response = await resp.json()
            exchange_order_id = response.get('orderID', 'default')
            if client_id:
                self.clients_ids.update({exchange_order_id: client_id})
            if not client_id or 'taker' in client_id:
                print(f"{self.EXCHANGE_NAME} RES: {response}")
                timestamp = self.timestamp_from_date(response['transactTime'])
                print(f'{self.EXCHANGE_NAME} ORDER PLACE TIME: {timestamp - self.time_sent} sec')
            order_res = self.construct_order_res(response, market, exchange_order_id)
            if client_id:
                self.responses.update({client_id: order_res})
                self.LAST_ORDER_ID = exchange_order_id
            else:
                self.responses.update({exchange_order_id: order_res})

    @try_exc_regular
    def construct_order_res(self, response: dict, market: str, exchange_order_id: str) -> dict:
        self.orig_sizes.update({exchange_order_id: response.get('homeNotional')})
        status = self.get_order_response_status(response)
        timestamp = self.timestamp_from_date(response['transactTime'])
        return {'exchange_name': self.EXCHANGE_NAME,
                'exchange_order_id': exchange_order_id,
                'timestamp': timestamp,
                'status': status,
                'api_response': response,
                'size': response['cumQty'] / self.instruments[market]['contract_value'],
                'price': response.get('avgPx') if isinstance(response.get('avgPx'), float) else 0,
                'time_order_sent': self.time_sent,
                'create_order_time': timestamp - self.time_sent}

    @try_exc_regular
    def get_order_response_status(self, response: dict) -> str:
        if response.get('errors'):
            status = ResponseStatus.ERROR
            self.error_info = response.get('errors')
        elif response.get('ordStatus'):
            status = ResponseStatus.SUCCESS
        else:
            status = ResponseStatus.NO_CONNECTION
            self.error_info = response
        return status

    # response_example = {'orderID': 'ca60fd7c-71ad-4fe9-95b3-1358a923f36d', 'clOrdID': '', 'clOrdLinkID': '',
    #                     'account': 2127720, 'symbol': 'ETHUSDT', 'side': 'Buy', 'orderQty': 1000, 'price': 3151.08,
    #                     'displayQty': None, 'stopPx': None, 'pegOffsetValue': None, 'pegPriceType': '',
    #                     'currency': 'USDT', 'settlCurrency': 'USDt', 'ordType': 'Limit',
    #                     'timeInForce': 'GoodTillCancel', 'execInst': '', 'contingencyType': '', 'ordStatus': 'New',
    #                     'triggered': '', 'workingIndicator': True, 'ordRejReason': '', 'leavesQty': 1000, 'cumQty': 0,
    #                     'avgPx': None, 'text': 'Submitted via API.', 'transactTime': '2024-02-28T12:55:19.104Z',
    #                     'timestamp': '2024-02-28T12:55:19.104Z'}

    @try_exc_regular
    def change_order(self, amount: float, price: float, order_id: str):
        if amount:
            self.swagger_client.Order.Order_amend(orderID=order_id, orderQty=amount, price=price).result()
        else:
            self.swagger_client.Order.Order_amend(orderID=order_id, price=price).result()

    @try_exc_regular
    def cancel_all_orders(self):
        result = self.swagger_client.Order.Order_cancelAll().result()
        return result

    @try_exc_async
    async def cancel_order(self, order_id: str):
        result = self.swagger_client.Order.Order_cancel(orderID=order_id).result()
        # print(result)
        return result

    @try_exc_regular
    def __get_auth(self, method: str, uri: str, body: str = '') -> dict:
        """
        Return auth headers. Will use API Keys if present in settings.
        """
        # To auth to the WS using an API key, we generate a signature of a nonce &
        # the WS API endpoint.
        expires = str(int(round(time.time()) + 100))
        return {
            "api-expires": expires,
            "api-signature": self.auth.generate_signature(self.api_secret, method, uri, expires, body),
            "api-key": self.api_key,
            "Content-Type": "application/x-www-form-urlencoded",
            "Content-Length": str(len(body.encode('utf-8')))
        }

    @try_exc_regular
    def __get_url(self):
        """
        Generate a connection URL. We can define subscriptions right in the querystring.
        Most subscription topics are scoped by the symbol we're listening to.
        """
        # Some subscriptions need to have the symbol appended.
        url_parts = list(urllib.parse.urlparse(self.BASE_WS))
        url_parts[2] += "?subscribe={}".format(','.join(self.subscriptions)) + ',' + self.orderbook_type
        # actual_markets = [self.orderbook_type + ':' + y for x, y in self.markets.items() if x in self.markets_list]
        # url_parts[2] += ',' + ','.join(actual_markets)
        final_url = urllib.parse.urlunparse(url_parts)
        return final_url

    @try_exc_regular
    def get_orders(self) -> dict:
        # NECESSARY
        return self.orders

    @try_exc_regular
    def get_real_balance(self) -> None:
        transes = None
        while not transes:
            try:
                transes = self.swagger_client.User.User_getWalletHistory(currency='USDt').result()
            except:
                pass
        real = transes[0][0]['walletBalance'] if not transes[0][0].get('marginBalance') else transes[0][0][
            'marginBalance']
        self.balance['total'] = (real / 10 ** 6)
        self.balance['timestamp'] = datetime.utcnow().timestamp()

    @try_exc_regular
    def get_positions(self) -> dict:
        return self.positions

    @try_exc_async
    async def get_orderbook_by_symbol(self, symbol: str) -> dict:
        res = self.swagger_client.OrderBook.OrderBook_getL2(symbol=symbol).result()[0]
        contract_value = self.get_contract_value(symbol)
        orderbook = dict()
        orderbook['bids'] = [[x['price'], x['size'] / contract_value] for x in res if x['side'] == 'Buy']
        orderbook['asks'] = [[x['price'], x['size'] / contract_value] for x in res if x['side'] == 'Sell']
        orderbook['timestamp'] = int(datetime.utcnow().timestamp() * 1000)
        return orderbook

    @try_exc_regular
    def get_available_balance(self) -> dict:
        return super().get_available_balance(
            leverage=self.leverage,
            max_pos_part=self.max_pos_part,
            positions=self.positions,
            balance=self.balance)

    @try_exc_regular
    def get_position(self) -> None:
        poses = self.swagger_client.Position.Position_get().result()[0]
        pos_bitmex = {x['symbol']: x for x in poses}
        all_poses = {}
        for symbol, position in pos_bitmex.items():
            all_poses[symbol] = {
                'amount': float(position['homeNotional']),
                'entry_price': float(position['avgEntryPrice']),
                'unrealized_pnl_usd': 0,
                'side': 'LONG',
                'amount_usd': -float(position['foreignNotional']),
                'realized_pnl_usd': 0,
                'lever': float(position['leverage']),
            }
        self.positions = all_poses

    @try_exc_regular
    def get_orderbook(self, market: str):
        ob = self.orderbook.get(market)
        if ob and ob['asks'][0][0] <= ob['bids'][0][0]:
            print(f"\n\nALERT {self.EXCHANGE_NAME} ORDERBOOK IS BROKEN\n\n{ob}")
            return None
        return ob

    def get_contract_value(self, symbol):
        return self.instruments[symbol]['contract_value']

    @try_exc_async
    async def create_order(self, symbol, side, price, size, session, expire=10000, client_id=None, expiration=None):
        contract_value = self.get_contract_value(symbol)
        body = {
            "symbol": symbol,
            "ordType": "Limit",
            "price": price,
            "orderQty": int(size * contract_value),
            "side": side.capitalize()
        }
        print(f'BITMEX BODY: {body}')
        if client_id is not None:
            body["clOrdID"] = client_id

        res = await self._post("/api/v1/order", body, session)
        print(f"BITMEX RES: {res}")
        timestamp = 0000000000000
        exchange_order_id = None
        if res.get('errors'):
            status = ResponseStatus.ERROR
            self.error_info = res.get('errors')
        elif res.get('ordStatus'):
            timestamp = int(datetime.timestamp(datetime.strptime(res['transactTime'], '%Y-%m-%dT%H:%M:%S.%fZ')) * 1000)
            status = ResponseStatus.SUCCESS
            self.LAST_ORDER_ID = res['orderID']
            exchange_order_id = res['orderID']
        else:
            status = ResponseStatus.NO_CONNECTION
            self.error_info = res
        return {
            'exchange_name': self.EXCHANGE_NAME,
            'exchange_order_id': exchange_order_id,
            'timestamp': timestamp,
            'status': status
        }

    @try_exc_async
    async def _post(self, path, data, session):
        headers_body = f"symbol={data['symbol']}&side={data['side']}&ordType=Limit&orderQty={data['orderQty']}&price={data['price']}"
        headers = self.__get_auth("POST", path, headers_body)
        headers.update(
            {
                "Content-Length": str(len(headers_body.encode('utf-8'))),
                "Content-Type": "application/x-www-form-urlencoded"}
        )
        async with session.post(url=self.BASE_URL + path, headers=headers, data=headers_body) as resp:
            return await resp.json()


if __name__ == '__main__':
    import configparser
    import sys
    import uuid

    config = configparser.ConfigParser()
    config.read(sys.argv[1], "utf-8")
    client = BitmexClient(keys=config['BITMEX'],
                          leverage=float(config['SETTINGS']['LEVERAGE']),
                          markets_list=['ETH', 'LINK', 'LTC', 'BCH', 'SOL', 'MINA', 'XRP', 'PEPE', 'CFX', 'FIL'],
                          state='Bot')
    client.run_updater()


    async def test_order():
        ob = client.get_orderbook('ETHUSDT')
        # print(ob)
        price = ob['bids'][5][0]
        # # client.get_markets()
        price, amount = client.fit_sizes(price, client.instruments['ETHUSDT']['min_size'], 'ETHUSDT')
        data = await client.create_fast_order(price, amount, 'buy', 'ETHUSDT')
        print(data)
        data_cancel = client.cancel_all_orders()
        print(type(data_cancel))


    time.sleep(3)

    # print(client.markets)
    #
    # client.get_real_balance()
    # asyncio.run(test_order())
    # client.get_position()
    # print(client.positions)
    # print(client.get_balance())
    # print(client.orders)
    while True:
        # print(client.funds())
        # print(client.orderbook)
        # print('CYCLE DONE')
        # print(f"{client.get_available_balance('sell')=}")
        # print(f"{client.get_available_balance('buy')=}")
        # print("\n")
        time.sleep(5)
