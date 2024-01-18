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
from clients.core.APIKeyAuthenticator import APIKeyAuthenticator as auth
from core.wrappers import try_exc_regular, try_exc_async


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

    def __init__(self, keys, leverage, markets_list=[], max_pos_part=20):
        super().__init__()
        self.max_pos_part = max_pos_part
        self.markets_list = markets_list
        self._loop = asyncio.new_event_loop()
        self._connected = asyncio.Event()
        self.leverage = leverage
        self.api_key = keys['API_KEY']
        self.api_secret = keys['API_SECRET']
        self.subscriptions = ['margin', 'position', 'orderBook10', 'execution']

        self.auth = auth(self.BASE_URL, self.api_key, self.api_secret)
        self.amount = 0
        self.amount_contracts = 0
        self.taker_fee = 0.0005
        self.requestLimit = 1200
        self.price = 0
        self.data = {}
        self.orders = {}
        self.positions = {}
        self.orderbook = {}
        self.balance = {}
        self.keys = {}
        self.exited = False
        self.error_info = None
        self.swagger_client = self.swagger_client_init()
        self.commission = self.swagger_client.User.User_getCommission().result()[0]
        self.instruments = self.get_all_instruments()
        self.markets = self.get_markets()
        self.get_real_balance()
        self.wst = threading.Thread(target=self._run_ws_forever, daemon=True)
        self.time_sent = datetime.utcnow().timestamp()

    @try_exc_regular
    def get_markets(self):
        markets = {}
        for symbol, market in self.instruments.items():
            markets.update({market['coin']: symbol})
        return markets

    @try_exc_regular
    def run_updater(self):
        self.wst.start()
        # self.__wait_for_account()
        # self.get_contract_value()

    @try_exc_regular
    def get_fees(self, symbol):
        taker_fee = self.commission[symbol]['takerFee']
        maker_fee = self.commission[symbol]['makerFee']
        return taker_fee, maker_fee

    @staticmethod
    @try_exc_regular
    def get_quantity_precision(step_size):
        if '.' in str(step_size):
            quantity_precision = len(str(step_size).split('.')[1])
        elif '-' in str(step_size):
            quantity_precision = int(str(step_size).split('-')[1])
        else:
            quantity_precision = 1
        return quantity_precision

    @try_exc_regular
    def get_all_instruments(self):
        instr_list = {}
        instruments = self.swagger_client.Instrument.Instrument_get(
            filter=json.dumps({'quoteCurrency': 'USDT', 'state': 'Open'})).result()
        for instr in instruments[0]:
            if '2' in instr['symbol'] or '_' in instr['symbol']:
                continue
            contract_value = instr['underlyingToPositionMultiplier']
            price_precision = self.get_price_precision(instr['tickSize'])
            step_size = instr['lotSize'] / contract_value
            quantity_precision = self.get_quantity_precision(step_size)
            instr_list.update({instr['symbol']: {'tick_size': instr['tickSize'],
                                                 'price_precision': price_precision,
                                                 'step_size': instr['lotSize'] / contract_value,
                                                 'min_size': instr['lotSize'] / contract_value,
                                                 'quantity_precision': quantity_precision,
                                                 'contract_value': contract_value,
                                                 'coin': instr['rootSymbol']
                                                 }})
        return instr_list

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
    def _run_ws_forever(self):
        while True:
            try:
                self._loop.run_until_complete(self._run_ws_loop())
            finally:
                print("WS loop completed. Restarting")

    @try_exc_regular
    def __wait_for_account(self):
        '''On subscribe, this data will come down. Wait for it.'''
        # Wait for the keys to show up from the ws
        while not set(self.subscriptions) <= set(self.data):
            time.sleep(0.1)

    @try_exc_async
    async def _run_ws_loop(self):
        async with aiohttp.ClientSession(headers=self.__get_auth('GET', '/realtime')) as s:
            async with s.ws_connect(self.__get_url()) as ws:
                print("Bitmex: connected")
                self._connected.set()
                self._ws = ws
                async for msg in ws:
                    self._process_msg(msg)
                self._connected.clear()

    @staticmethod
    @try_exc_regular
    def get_order_status(order):
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
    def get_all_tops(self):
        # NECESSARY
        tops = {}
        for symbol, orderbook in self.orderbook.items():
            coin = symbol.upper().split('USD')[0]
            if len(orderbook['bids']) and len(orderbook['asks']):
                tops.update({self.EXCHANGE_NAME + '__' + coin:
                               {'top_bid': orderbook['bids'][0][0], 'top_ask': orderbook['asks'][0][0],
                                'bid_vol': orderbook['bids'][0][1], 'ask_vol': orderbook['asks'][0][1],
                                'ts_exchange': orderbook['timestamp']}})
        return tops

    @try_exc_async
    async def get_all_orders(self, symbol: str, session: aiohttp.ClientSession):
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
            real_price = res.get('avgPx', 0)
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
                    'exchange': self.EXCHANGE_NAME,
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
    def get_order_by_id(self, symbol, order_id):
        res = self.swagger_client.Order.Order_getOrders(filter=json.dumps({'orderID': order_id})).result()[0][0]
        contract_value = self.get_contract_value(symbol)
        real_size = res['cumQty'] / contract_value
        real_price = res.get('avgPx', 0)
        return {
            'exchange_order_id': order_id,
            'exchange': self.EXCHANGE_NAME,
            'status': OrderStatus.FULLY_EXECUTED if res.get('ordStatus') == 'Filled' else OrderStatus.NOT_EXECUTED,
            'factual_price': real_price,
            'factual_amount_coin': real_size,
            'factual_amount_usd': real_price * real_size,
            'datetime_update': datetime.utcnow(),
            'ts_update': int(datetime.utcnow().timestamp() * 1000)
        }

    @try_exc_regular
    def get_order_result(self, order):
        factual_price = order['avgPx'] if order.get('avgPx') else 0
        factual_size_coin = abs(order['homeNotional']) if order.get('homeNotional') else 0
        factual_size_usd = abs(order['foreignNotional']) if order.get('foreignNotional') else 0
        status = self.get_order_status(order)
        result = {
            'exchange_order_id': order['orderID'],
            'exchange': self.EXCHANGE_NAME,
            'status': status,
            'factual_price': factual_price,
            'factual_amount_coin': factual_size_coin,
            'factual_amount_usd': factual_size_usd,
            'datetime_update': datetime.utcnow(),
            'ts_update': int(round(datetime.utcnow().timestamp() * 1000))
        }
        return result

    @try_exc_regular
    def _process_msg(self, msg: aiohttp.WSMessage):
        if msg.type == aiohttp.WSMsgType.TEXT:
            message = json.loads(msg.data)
            if message.get('subscribe'):
                print(message)
            if message.get("action"):
                if message['table'] == 'execution':
                    self.update_fills(message['data'])
                elif message['table'] == 'orderBook10':
                    self.update_orderbook(message['data'])
                elif message['table'] == 'position':
                    self.update_positions(message['data'])
                elif message['table'] == 'margin':
                    self.update_balance(message['data'])

    @try_exc_regular
    def update_positions(self, data):
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

    @try_exc_regular
    def update_orderbook(self, data):
        for ob in data:
            if ob.get('symbol') and self.instruments.get(ob['symbol']):
                ob.update({'timestamp': int(datetime.utcnow().timestamp() * 1000)})
                contract_value = self.get_contract_value(ob['symbol'])
                ob['bids'] = [[x[0], x[1] / contract_value] for x in ob['bids']]
                ob['asks'] = [[x[0], x[1] / contract_value] for x in ob['asks']]
                self.orderbook.update({ob['symbol']: ob})

    @try_exc_regular
    def update_fills(self, data):
        for order in data:
            if order['ordStatus'] == 'New':
                timestamp = self.timestamp_from_date(order['transactTime'])
                print(f'BITMEX ORDER PLACE TIME: {timestamp - self.time_sent} sec')
            result = self.get_order_result(order)
            self.orders.update({order['orderID']: result})

    @try_exc_regular
    def update_balance(self, data):
        for balance in data:
            if balance['currency'] == 'USDt' and balance.get('marginBalance'):
                self.balance = {'free': balance.get('availableMargin', 0) / (10 ** 6),
                                'total': balance['marginBalance'] / (10 ** 6),
                                'timestamp': datetime.utcnow().timestamp()}

    @staticmethod
    @try_exc_regular
    def timestamp_from_date(date: str):
        # date = '2023-02-15T02:55:27.640Z'
        ms = int(date.split(".")[1].split('Z')[0]) / 1000
        return time.mktime(datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ").timetuple()) + ms

    @staticmethod
    @try_exc_regular
    def get_pos_power(self, symbol):
        pos_power = 6 if 'USDT' in symbol else 8
        currency = 'USDt' if 'USDT' in symbol else 'XBt'
        return pos_power, currency

    @try_exc_regular
    def swagger_client_init(self, config=None):
        if config is None:
            # See full config options at http://bravado.readthedocs.io/en/latest/configuration.html
            config = {
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
        return SwaggerClient.from_url(spec_uri, config=config, http_client=request_client)

    @try_exc_regular
    def exit(self):
        '''Call this to exit - will close websocket.'''
        self.exited = True
        self.ws.close()

    @try_exc_regular
    def get_balance(self):
        """Get your margin details."""
        return self.balance['total']

    @try_exc_regular
    def open_orders(self):
        """Get all your open orders."""
        return self.data['order']

    @try_exc_regular
    def fit_sizes(self, price, symbol):
        instr = self.instruments[symbol]
        tick_size = instr['tick_size']
        contract_value = instr['contract_value']
        quantity_precision = instr['quantity_precision']
        self.amount = round(self.amount, quantity_precision)
        self.amount_contracts = round(self.amount * contract_value)
        rounded_price = round(price / tick_size) * tick_size
        self.price = round(rounded_price, instr['price_precision'])

    @try_exc_async
    async def create_order(self, symbol, side, session, expire=100, client_id=None):
        self.time_sent = datetime.utcnow().timestamp()
        body = {
            "symbol": symbol,
            "ordType": "Limit",
            "price": self.price,
            "orderQty": self.amount_contracts,
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

    @try_exc_regular
    def change_order(self, amount, price, id):
        if amount:
            self.swagger_client.Order.Order_amend(orderID=id, orderQty=amount, price=price).result()
        else:
            self.swagger_client.Order.Order_amend(orderID=id, price=price).result()

    @try_exc_regular
    def cancel_all_orders(self):
        result = self.swagger_client.Order.Order_cancelAll().result()
        return result
        # print('order', order)
        # if not order['ordStatus'] in ['Canceled', 'Filled']:
        #
        #     print(self.swagger_client.Order.Order_cancel(orderID=order['orderID']).result())
        #
        #     print('\n\n\n\n\n')

    @try_exc_regular
    def __get_auth(self, method, uri, body=''):
        """
        Return auth headers. Will use API Keys if present in settings.
        """
        # To auth to the WS using an API key, we generate a signature of a nonce and
        # the WS API endpoint.
        expires = str(int(round(time.time()) + 100))
        return {
            "api-expires": expires,
            "api-signature": self.auth.generate_signature(self.api_secret, method, uri, expires, body),
            "api-key": self.api_key,
        }

    @try_exc_regular
    def __get_url(self):
        """
        Generate a connection URL. We can define subscriptions right in the querystring.
        Most subscription topics are scoped by the symbol we're listening to.
        """
        # Some subscriptions need to xhave the symbol appended.
        url_parts = list(urllib.parse.urlparse(self.BASE_WS))
        url_parts[2] += "?subscribe={}".format(','.join(self.subscriptions))
        return urllib.parse.urlunparse(url_parts)

    @try_exc_regular
    def get_orders(self):
        # NECESSARY
        return self.orders

    # def get_pnl(self):
    #     positions = self.positions()
    #     pnl = [x for x in positions if x['symbol'] == self.symbol]
    #     pnl = None if not len(pnl) else pnl[0]
    #     if not pnl:
    #         return [0, 0, 0]
    #     multiplier_power = 6 if pnl['currency'] == 'USDt' else 8
    #     change = 1 if pnl['currency'] == 'USDt' else self.get_orderbook()['XBTUSD']['bids'][0][0]
    #     realized_pnl = pnl['realisedPnl'] / 10 ** multiplier_power * change
    #     unrealized_pnl = pnl['unrealisedPnl'] / 10 ** multiplier_power * change
    #     return [realized_pnl + unrealized_pnl, pnl, realized_pnl]

    @try_exc_regular
    def get_last_price(self, side, symbol):
        side = side.capitalize()
        # last_trades = self.recent_trades()
        last_trades = self.data['execution']
        last_price = 0
        for trade in last_trades:
            if trade['side'] == side and trade['symbol'] == symbol and trade.get('avgPx'):
                last_price = trade['avgPx']
        return last_price

    @try_exc_regular
    def get_real_balance(self):
        transes = None
        while not transes:
            try:
                transes = self.swagger_client.User.User_getWalletHistory(currency='USDt').result()
            except:
                pass
        real = transes[0][0]['marginBalance'] if transes[0][0]['marginBalance'] else transes[0][0]['walletBalance']
        self.balance['total'] = (real / 10 ** 6)
        self.balance['timestamp'] = datetime.utcnow().timestamp()

    @try_exc_regular
    def get_positions(self) -> dict:
        return self.positions

    @try_exc_async
    async def get_orderbook_by_symbol(self, symbol):
        res = self.swagger_client.OrderBook.OrderBook_getL2(symbol=symbol).result()[0]
        contract_value = self.get_contract_value(symbol)
        orderbook = dict()
        orderbook['bids'] = [[x['price'], x['size'] / contract_value] for x in res if x['side'] == 'Buy']
        orderbook['asks'] = [[x['price'], x['size'] / contract_value] for x in res if x['side'] == 'Sell']
        orderbook['timestamp'] = int(datetime.utcnow().timestamp() * 1000)
        return orderbook

    @try_exc_regular
    def get_available_balance(self):
        return super().get_available_balance(
            leverage=self.leverage,
            max_pos_part=self.max_pos_part,
            positions=self.positions,
            balance=self.balance)

    @try_exc_regular
    def get_position(self):
        '''Get your positions.'''
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
    def get_orderbook(self, symbol):
        return self.orderbook[symbol]

    def get_contract_value(self, symbol):
        return self.instruments[symbol]['contract_value']


if __name__ == '__main__':
    import configparser
    import sys
    import uuid

    config = configparser.ConfigParser()
    config.read(sys.argv[1], "utf-8")
    client = BitmexClient(keys=config['BITMEX'],
                          leverage=float(config['SETTINGS']['LEVERAGE']),
                          markets_list=['ETH', 'LINK', 'LTC', 'BCH', 'SOL', 'MINA', 'XRP', 'PEPE', 'CFX', 'FIL'])
    client.run_updater()


    async def test_order():
        async with aiohttp.ClientSession() as session:
            ob = client.get_orderbook('ETHUSDT')
            # print(ob)
            price = ob['bids'][5][0]
            # # client.get_markets()
            client.fit_sizes(0.0135235, price, 'ETHUSDT')
            data = await client.create_order('ETHUSDT',
                                             'buy',
                                             session=session)
            print(data)
            data_cancel = client.cancel_all_orders()
            print(data_cancel)


    time.sleep(3)

    # print(client.markets)
    #
    # client.get_real_balance()
    asyncio.run(test_order())
    # client.get_position()
    # print(client.positions)
    # print(client.get_balance())
    # print(client.orders)
    while True:
        # print(client.funds())
        # print(client.get_orderbook())
        # print('CYCLE DONE')
        # print(f"{client.get_available_balance('sell')=}")
        # print(f"{client.get_available_balance('buy')=}")
        # print("\n")
        time.sleep(1)

