import asyncio
import base64
import hashlib
import hmac

import threading
import time
import uuid
from datetime import datetime

from urllib.parse import urlencode

import aiohttp
import orjson
import requests

from clients.core.base_client import BaseClient
from clients.core.enums import ConnectMethodEnum, ResponseStatus, OrderStatus, PositionSideEnum
from core.wrappers import try_exc_regular, try_exc_async


class KrakenClient(BaseClient):
    BASE_WS = 'wss://futures.kraken.com/ws/v1'
    BASE_URL = 'https://futures.kraken.com'
    EXCHANGE_NAME = 'KRAKEN'
    LAST_ORDER_ID = None
    urlOrderbooks = "https://futures.kraken.com/derivatives/api/v3/orderbook?symbol="
    urlMarkets = "https://futures.kraken.com/derivatives/api/v3/tickers"

    def __init__(self, keys, leverage, markets_list=[], max_pos_part=20):
        super().__init__()
        self.markets_list = markets_list
        self.max_pos_part = max_pos_part
        self.amount = None
        self.taker_fee = 0.0005
        self.requestLimit = 1200
        self.headers = {"Content-Type": "application/json"}
        self.tickers = {}
        self.markets = {}
        self.precises = self.get_precises()
        self.instruments = self.get_instruments()
        self.get_markets()
        self.leverage = leverage
        self.__api_key = keys['API_KEY']
        self.__secret_key = keys['API_SECRET']
        self.__last_challenge = None
        self.orders = {}
        self.count_flag = False
        self.error_info = None
        self.balance = {
            'total': 0.0,
            'free': 0,
            'timestamp': 0
        }
        self.last_price = {
            'sell': 0,
            'buy': 0
        }
        self.max_bid = 0
        self.min_ask = 10000000
        self.positions = {}
        self.get_position()
        self.get_real_balance()
        self.orderbook = {}
        self.pings = []
        self.price = 0
        self.amount = 0

    @try_exc_regular
    def get_instruments(self):
        markets = requests.get(url=self.urlMarkets, headers=self.headers).json()
        instruments = {}
        for market in markets['tickers']:
            symbol = market['symbol']
            if 'PF_' not in symbol.upper():
                continue
            self.tickers.update({symbol: market})
            coin = market['pair'].split(':')[0]
            step_size = self.get_stepsize(market)
            quantity_precision = self.get_quantity_precision(step_size)
            tick_size = self.precises[symbol]['tickSize']
            price_precision = self.get_price_precision(tick_size)
            instruments.update({symbol: {'coin': coin,
                                         'tick_size': tick_size,
                                         'quantity_precision': quantity_precision,
                                         'step_size': step_size,
                                         'min_size': step_size,
                                         'price_precision': price_precision}})
        return instruments

    @staticmethod
    @try_exc_regular
    def get_price_precision(tick_size):
        if '.' in str(tick_size):
            price_precision = len(str(tick_size).split('.')[1])
        elif 'e' in str(tick_size):
            price_precision = int((str(tick_size).split('-')[1])) - 1
        else:
            price_precision = 0
        return price_precision

    @staticmethod
    @try_exc_regular
    def get_quantity_precision(step_size):
        if '.' in str(step_size):
            quantity_precision = len(str(step_size).split('.')[1])
        else:
            quantity_precision = 0
        return quantity_precision

    @staticmethod
    @try_exc_regular
    def get_stepsize(market):
        values = [market.get('askSize', None),
                  market.get('vol24h', None),
                  market.get('lastSize', None),
                  market.get('bidSize', None)]
        max_value = 0
        for value in values:
            if not value:
                continue
            splt = str(value).split('.')[1] if '.' in str(value) else ''
            if len(splt) >= max_value:
                max_value = max(len(splt), max_value)
        if max_value:
            step_size = float('0.' + '0' * (max_value - 1) + str(1))
        else:
            step_size = 1
        return step_size

    @try_exc_regular
    def get_precises(self):
        url_path = "/derivatives/api/v3/instruments"
        res = requests.get(url=self.BASE_URL + url_path).json()
        precises = {}
        for instrument in res['instruments']:
            precises.update({instrument['symbol']: instrument})
        return precises

    @try_exc_regular
    def get_available_balance(self):
        return super().get_available_balance(
            leverage=self.leverage,
            max_pos_part=self.max_pos_part,
            positions=self.positions,
            balance=self.balance)

    @try_exc_regular
    def cancel_all_orders(self, orderID=None) -> dict:
        return self.__cancel_open_orders()

    @try_exc_regular
    def get_positions(self) -> dict:
        return self.positions

    @try_exc_regular
    def get_markets(self):
        for market in self.tickers.values():
            if market.get('tag') is not None:
                if (market['tag'] == 'perpetual') & (market['pair'].split(":")[1] == 'USD') & (not market['postOnly']):
                    coin = market['pair'].split(":")[0]
                    self.markets.update({coin: market['symbol']})
        return self.markets

    @try_exc_regular
    def get_balance(self) -> float:
        if round(datetime.utcnow().timestamp()) - self.balance['timestamp'] > 60:
            self.get_real_balance()
        return self.balance['total']

    @try_exc_regular
    def get_orderbook(self, symbol) -> dict:
        snap = self.orderbook[symbol.upper()]
        if snap.get('asks'):
            return snap
        try:
            orderbook = {'timestamp': self.orderbook[symbol.upper()]['timestamp'],
                         'asks': [[x, snap['sell'][x]] for x in sorted(snap['sell']) if snap['sell'].get(x)],
                         'bids': [[x, snap['buy'][x]] for x in sorted(snap['buy']) if snap['buy'].get(x)][::-1]}
        except:
            return self.get_orderbook(symbol)
        return orderbook

    @try_exc_regular
    def get_all_tops(self):
        orderbooks = dict()
        [orderbooks.update({self.markets[x]: self.get_orderbook(self.markets[x])}) for x in self.markets_list if
         self.markets.get(x)]
        tops = {}
        for symbol, orderbook in orderbooks.items():
            coin = symbol.upper().split('_')[1].split('USD')[0]
            if len(orderbook['bids']) and len(orderbook['asks']):
                tops.update({self.EXCHANGE_NAME + '__' + coin:
                                 {'top_bid': orderbook['bids'][0][0], 'top_ask': orderbook['asks'][0][0],
                                  'bid_vol': orderbook['bids'][0][1], 'ask_vol': orderbook['asks'][0][1],
                                  'ts_exchange': orderbook['timestamp']}})
        return tops

    @try_exc_async
    async def get_multi_orderbook(self, symbol):
        async with aiohttp.ClientSession() as session:
            async with session.get(url=self.urlOrderbooks + symbol) as response:
                resp = await response.json()
                ob = resp['orderBook']
                ts_exchange = int(datetime.strptime(resp['serverTime'], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() * 1000)
                return {'top_bid': ob['bids'][0][0], 'top_ask': ob['asks'][0][0],
                        'bid_vol': ob['bids'][0][1], 'ask_vol': ob['asks'][0][1],
                        'ts_exchange': ts_exchange}

    @try_exc_regular
    def get_last_price(self, side: str) -> float:
        return self.last_price[side.lower()]

    @try_exc_regular
    def run_updater(self) -> None:
        _loop_public = asyncio.new_event_loop()
        _loop_private = asyncio.new_event_loop()
        wsd_public = threading.Thread(target=self._run_forever, args=[ConnectMethodEnum.PUBLIC, _loop_public])
        bal_check = threading.Thread(target=self._run_forever, args=[ConnectMethodEnum.PRIVATE, _loop_private])
        wsd_public.start()
        bal_check.start()
        time.sleep(5)

    @try_exc_regular
    def _run_forever(self, type, loop) -> None:
        loop.run_until_complete(self._run_loop(type))

    @try_exc_async
    async def _run_loop(self, type) -> None:
        async with aiohttp.ClientSession() as session:
            if type == ConnectMethodEnum.PUBLIC:
                await self._symbol_data_getter(session)
            elif type == ConnectMethodEnum.PRIVATE:
                await self._user_balance_getter(session)

    # PUBLIC -----------------------------------------------------------------------------------------------------------
    @try_exc_async
    async def get_orderbook_by_symbol(self, symbol):
        async with aiohttp.ClientSession() as session:
            url_path = "/derivatives/api/v3/orderbook"
            async with session.get(url=self.BASE_URL + url_path + f'?symbol={symbol}') as resp:
                res = await resp.json()
                orderbook = {}
                orderbook.update({'bids': res['orderBook']['bids']})
                orderbook.update({'asks': res['orderBook']['asks']})
                orderbook.update({'timestamp': int(time.time() * 1000)})
            await session.close()
        return orderbook

    @try_exc_regular
    def get_orderbook_by_symbol_non_async(self, symbol):
        url_path = "/derivatives/api/v3/orderbook"
        resp = requests.get(url=self.BASE_URL + url_path + f'?symbol={symbol}').json()
        orderbook = {}
        orderbook.update({'bids': resp['orderBook']['bids']})
        orderbook.update({'asks': resp['orderBook']['asks']})
        orderbook.update({'timestamp': int(time.time() * 1000)})
        return orderbook

    @try_exc_async
    async def _symbol_data_getter(self, session: aiohttp.ClientSession) -> None:
        async with session.ws_connect(self.BASE_WS) as ws:
            # self.markets_list = list(self.markets.keys())[:10]
            for symbol in list(self.markets.keys()):
                if market := self.markets.get(symbol):
                    await ws.send_str(orjson.dumps({"event": "subscribe",
                                                    "feed": "book",
                                                    'snapshot': False,
                                                    "product_ids": [market.upper()]}).decode('utf-8'))
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    payload = orjson.loads(msg.data)
                    if not payload.get('event'):
                        if payload.get('feed') == 'book_snapshot':
                            self.update_orderbook_snapshot(payload)
                        elif payload.get('feed') == 'book':
                            self.update_orderbook(payload)

    @try_exc_regular
    def update_orderbook(self, payload):
        symbol = payload['product_id']
        res = self.orderbook[symbol][payload['side']]
        if res.get(payload['price']) and not payload['qty']:
            del res[payload['price']]
        else:
            self.orderbook[symbol][payload['side']][payload['price']] = payload['qty']
            self.orderbook[symbol]['timestamp'] = payload['timestamp']
        if payload['side'] == 'sell':
            if payload['price'] < self.min_ask:
                self.min_ask = payload['price']
                self.count_flag = True
        else:
            if payload['price'] > self.max_bid:
                self.max_bid = payload['price']
                self.count_flag = True

    @try_exc_regular
    def update_orderbook_snapshot(self, payload):
        symbol = payload['product_id']
        self.orderbook[symbol] = {'sell': {x['price']: x['qty'] for x in payload['asks']},
                                  'buy': {x['price']: x['qty'] for x in payload['bids']},
                                  'timestamp': payload['timestamp']}
        self.max_bid = max(self.orderbook[symbol]['buy'].keys())
        self.min_ask = min(self.orderbook[symbol]['sell'].keys())
        self.count_flag = True

    # PRIVATE ----------------------------------------------------------------------------------------------------------

    @try_exc_regular
    def get_fills(self) -> dict:
        url_path = '/derivatives/api/v3/fills'
        nonce = str(int(time.time() * 1000))
        params = {
            "lastFillTime": str(round((time.time() - 600) * 1000)),
        }

        headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
            "Nonce": nonce,
            "APIKey": self.__api_key,
            "Authent": self.get_kraken_futures_signature(
                url_path, '', nonce
            ).decode('utf-8')
        }
        resp = requests.get(headers=headers, url=self.BASE_URL + url_path, data=params)
        return resp.json()

    @try_exc_async
    async def get_all_orders(self, symbol, session) -> list:
        all_orders = self.__get_all_orders()
        res_orders = {}
        for elements in all_orders['elements']:
            for event in ['OrderPlaced', 'OrderCancelled']:
                # print(elements)
                if order := elements['event'].get(event, {}).get('order'):
                    if order['clientId'] == "":
                        if int(order['lastUpdateTimestamp']) > int(time.time() * 1000) - 86400000:
                            amount = float(order['quantity']) if order['filled'] == '0' else float(
                                order['filled']) - float(order['quantity'])
                            status = OrderStatus.NOT_EXECUTED if not float(
                                order['quantity']) else OrderStatus.FULLY_EXECUTED
                            res_orders.update({order['uid']: {
                                'id': uuid.uuid4(),
                                'datetime': datetime.fromtimestamp(int(order['lastUpdateTimestamp'] / 1000)),
                                'ts': int(order['lastUpdateTimestamp']),
                                'context': 'web-interface',
                                'parent_id': uuid.uuid4(),
                                'exchange_order_id': order['uid'],
                                'type': 'GTC',  # TODO check it
                                'status': status,
                                'exchange': self.EXCHANGE_NAME,
                                'side': order['direction'].lower(),
                                'symbol': order['tradeable'],
                                'expect_price': float(order['limitPrice']),
                                'expect_amount_coin': amount,
                                'expect_amount_usd': float(order['limitPrice']) * amount,
                                'expect_fee': self.taker_fee,
                                'factual_price': float(order['limitPrice']),
                                'factual_amount_coin': 0 if order['filled'] == '0' else float(order['filled']) - float(
                                    order['quantity']),
                                'factual_amount_usd': float(order['limitPrice']) * amount,
                                'order_place_time': 0,
                                'factual_fee': self.taker_fee,
                                'env': '-',
                                'datetime_update': datetime.utcnow(),
                                'ts_update': int(time.time() * 1000),
                                'client_id': order['clientId']
                            }})
        return [x for x in res_orders.values()]

    @try_exc_regular
    def __get_all_orders(self) -> dict:
        url_path = '/api/history/v3/orders'
        nonce = str(int(time.time() * 1000))
        headers = {"Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
                   "Nonce": nonce,
                   "APIKey": self.__api_key,
                   "Authent": self.get_kraken_futures_signature(url_path, '', nonce).decode('utf-8')}
        resp = requests.get(headers=headers, url=self.BASE_URL + url_path)
        return resp.json()

    @try_exc_regular
    def get_order_status(self, order_id):
        orders = self.__get_all_orders()
        status = OrderStatus.NOT_EXECUTED
        for order in orders['elements']:
            if order_id in str(order) and 'OrderUpdated' in str(order):
                if order['event']['OrderUpdated']['newOrder']['quantity'] == '0.0':
                    status = OrderStatus.FULLY_EXECUTED
                    break
                elif order['event']['OrderUpdated']['newOrder']['filled'] != '0.0':
                    status = OrderStatus.PARTIALLY_EXECUTED
                    break
        return status

    @try_exc_regular
    def get_order_by_id(self, symbol, order_id: str) -> dict:
        fills_orders = self.get_fills()
        status = self.get_order_status(order_id)
        av_price = 0
        real_size_coin = 0
        real_size_usd = 0
        for fill in fills_orders.get('fills', []):
            if fill['order_id'] == order_id:
                if av_price:
                    real_size_usd = av_price * real_size_coin + float(fill['size']) * float(fill['price'])
                    av_price = real_size_usd / (real_size_coin + float(fill['size']))
                else:
                    real_size_usd = float(fill['size']) * float(fill['price'])
                    av_price = float(fill['price'])
                real_size_coin += float(fill['size'])
        return {'exchange_order_id': order_id,
                'exchange': self.EXCHANGE_NAME,
                'status': status,
                'factual_price': av_price,
                'factual_amount_coin': real_size_coin,
                'factual_amount_usd': real_size_usd,
                'datetime_update': datetime.utcnow(),
                'ts_update': int(datetime.utcnow().timestamp() * 1000)}

    # @try_exc_regular
    # def __get_order_by_all_orders(self, order_id):
    #     all_orders = self.__get_all_orders()
    #     if type(all_orders) == dict and all_orders.get('elements'):
    #         for order in all_orders['elements']:
    #             stts = OrderStatus.PROCESSING
    #             price = 0
    #             amount = 0
    #             if last_update := order['event'].get('OrderUpdated'):
    #                 if last_update['newOrder']['uid'] == order_id:
    #                     # print(f"KRAKEN {order=}")
    #                     rsn = last_update['reason']
    #                     if rsn in ['full_fill', 'partial_fill']:
    #                         stts = OrderStatus.FULLY_EXECUTED if rsn == 'full_fill' else OrderStatus.PARTIALLY_EXECUTED
    #                         price = float(last_update['newOrder']['limitPrice'])
    #                         amount = float(last_update['newOrder']['filled'])
    #                     elif rsn in ['cancelled_by_user', 'cancelled_by_admin', 'not_enough_margin']:
    #                         stts = OrderStatus.NOT_EXECUTED
    #                     return {'exchange_order_id': last_update['oldOrder']['uid'],
    #                             'exchange': self.EXCHANGE_NAME,
    #                             'status': stts,
    #                             'factual_price': price,
    #                             'factual_amount_coin': amount,
    #                             'factual_amount_usd': amount * price,
    #                             'datetime_update': datetime.utcnow(),
    #                             'ts_update': int(time.time() * 1000)}
    #         return {'exchange_order_id': order_id,
    #                 'exchange': self.EXCHANGE_NAME,
    #                 'status': OrderStatus.NOT_EXECUTED,
    #                 'factual_price': 0,
    #                 'factual_amount_coin': 0,
    #                 'factual_amount_usd': 0,
    #                 'datetime_update': datetime.utcnow(),
    #                 'ts_update': int(time.time() * 1000)}

    @try_exc_regular
    def _sign_message(self, api_path: str, data: dict) -> str:
        plain_data = []
        for key, value in data.items():
            if isinstance(value, (list, tuple)):
                plain_data.extend([(key, str(item)) for item in value])
            else:
                plain_data.append((key, str(value)))

        post_data = urlencode(plain_data)
        encoded = (str(data["nonce"]) + post_data).encode()
        message = api_path.encode() + hashlib.sha256(encoded).digest()
        signature = hmac.new(base64.b64decode(self.__secret_key), message, hashlib.sha512)
        sig_digest = base64.b64encode(signature.digest())

        return sig_digest.decode()

    @try_exc_regular
    def get_kraken_futures_signature(self, endpoint: str, data: str, nonce: str):
        if endpoint.startswith("/derivatives"):
            endpoint = endpoint[len("/derivatives"):]

        sha256_hash = hashlib.sha256()
        sha256_hash.update((data + nonce + endpoint).encode("utf8"))

        return base64.b64encode(
            hmac.new(base64.b64decode(self.__secret_key), sha256_hash.digest(), hashlib.sha512).digest())

    @try_exc_regular
    def __cancel_open_orders(self):
        url_path = "/derivatives/api/v3/cancelallorders"
        nonce = str(int(time.time() * 1000))
        headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
            "Nonce": nonce,
            "APIKey": self.__api_key,
            "Authent": self.get_kraken_futures_signature(
                url_path, '', nonce),
        }
        return requests.post(headers=headers, url=self.BASE_URL + url_path).json()

    @try_exc_regular
    def get_real_balance(self):
        url_path = "/derivatives/api/v3/accounts"
        nonce = str(int(time.time() * 1000))

        headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
            "Nonce": nonce,
            "APIKey": self.__api_key,
            "Authent": self.get_kraken_futures_signature(
                url_path, '', nonce
            ),
        }
        res = requests.get(headers=headers, url=self.BASE_URL + url_path).json()
        self.balance['total'] = res['accounts']['flex']['portfolioValue']
        self.balance['free'] = res['accounts']['flex']['availableMargin']
        self.balance['timestamp'] = round(datetime.utcnow().timestamp())

    @try_exc_regular
    def fit_sizes(self, price, symbol):
        instr = self.instruments[symbol.upper()]
        tick_size = instr['tick_size']
        price_precision = instr['price_precision']
        quantity_precision = instr['quantity_precision']
        self.amount = round(self.amount, quantity_precision)
        rounded_price = round(price / tick_size) * tick_size
        self.price = round(rounded_price, price_precision)

    @try_exc_async
    async def create_order(self, symbol, side, session, expire=5000, client_id=None):
        time_sent = datetime.utcnow().timestamp()
        nonce = str(int(time.time() * 1000))
        url_path = "/derivatives/api/v3/sendorder"
        params = {
            "orderType": "lmt",  # post
            "limitPrice": self.price,
            "side": side.lower(),
            "size": self.amount,
            "symbol": symbol,
            "cliOrdId": client_id
        }
        post_string = "&".join([f"{key}={params[key]}" for key in sorted(params)])
        print(f"KRAKEN BODY: {params}")
        headers = {
            "Content-Type": "application/json",
            "Nonce": nonce,
            "APIKey": self.__api_key,
            "Authent": self.get_kraken_futures_signature(url_path, post_string, nonce).decode('utf-8'),
        }
        url = self.BASE_URL + url_path + '?' + post_string
        try:
            async with session.post(url=url, headers=headers, data=post_string) as resp:
                response = await resp.json()
                print(f'KRAKEN RESPONSE: {response}')
                self.LAST_ORDER_ID = response.get('sendStatus', {}).get('order_id', 'default')
                exchange_order_id = response.get('sendStatus', {}).get('order_id', 'default')
                if response['sendStatus'].get('receivedTime'):
                    timestamp = response['sendStatus']['receivedTime']
                    timestamp = int(datetime.timestamp(datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')) * 1000)
                    status = ResponseStatus.SUCCESS
                else:
                    timestamp = 0000000000000
                    status = ResponseStatus.ERROR
                    self.error_info = response
                ping = int(round(timestamp - (time_sent * 1000), 0))
                self.pings.append(ping)
                with open(f'{self.EXCHANGE_NAME}_pings.txt', 'a') as file:
                    file.write(str(datetime.utcnow()) + ' ' + str(ping) + '\n')
                avr = int(round((sum(self.pings) / len(self.pings)), 0))
                print(f"{self.EXCHANGE_NAME}: ping {ping}|avr: {avr}|max: {max(self.pings)}|min: {min(self.pings)}")
                return {
                    'exchange_name': self.EXCHANGE_NAME,
                    'exchange_order_id': exchange_order_id,
                    'timestamp': timestamp,
                    'status': status
                }
        except Exception as e:
            self.error_info = e
            return {
                'exchange_name': self.EXCHANGE_NAME,
                'exchange_order_id': None,
                'timestamp': int(round(datetime.utcnow().timestamp() * 1000)),
                'status': ResponseStatus.ERROR
            }

    # NEW FUNCTIONS
    @try_exc_regular
    def get_position(self):
        self.positions = {}
        nonce = str(int(time.time() * 1000))
        url_path = '/derivatives/api/v3/openpositions'
        headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
            "Nonce": nonce,
            "APIKey": self.__api_key,
            "Authent": self.get_kraken_futures_signature(url_path, '', nonce).decode('utf-8'),
        }
        res = requests.get((self.BASE_URL + url_path), headers=headers).json()
        if isinstance(res, dict):
            for payload in res.get('openPositions', []):
                if float(payload['size']):
                    side = PositionSideEnum.LONG if payload['side'] == 'long' else PositionSideEnum.SHORT
                    amount_usd = payload['size'] * payload['price']
                    self.positions.update({payload['symbol'].upper(): {
                        'side': side,
                        'amount_usd': amount_usd if side == PositionSideEnum.LONG else -amount_usd,
                        'amount': payload['size'] if side == PositionSideEnum.LONG else -payload['size'],
                        'entry_price': payload['price'],
                        'unrealized_pnl_usd': 0,
                        'realized_pnl_usd': 0,
                        'lever': self.leverage
                    }})

    @try_exc_regular
    def _get_sign_challenge(self, challenge: str) -> str:
        sha256_hash = hashlib.sha256()
        sha256_hash.update(challenge.encode("utf-8"))
        return base64.b64encode(
            hmac.new(
                base64.b64decode(self.__secret_key), sha256_hash.digest(), hashlib.sha512
            ).digest()
        ).decode("utf-8")

    @try_exc_async
    async def _user_balance_getter(self, session: aiohttp.ClientSession) -> None:
        async with session.ws_connect(self.BASE_WS) as ws:
            if not self.__last_challenge:
                await ws.send_str(orjson.dumps({"event": "challenge", "api_key": self.__api_key}).decode('utf-8'))

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        msg_data = orjson.loads(msg.data)
                        if msg_data.get('event') == 'challenge':
                            self.__last_challenge = msg_data['message']
                            await ws.send_str(orjson.dumps({
                                "event": "subscribe",
                                "feed": "balances",
                                "api_key": self.__api_key,
                                'original_challenge': self.__last_challenge,
                                "signed_challenge": self._get_sign_challenge(self.__last_challenge)
                            }).decode('utf-8'))

                            await ws.send_str(orjson.dumps({
                                "event": "subscribe",
                                "feed": "fills",
                                "api_key": self.__api_key,
                                'original_challenge': self.__last_challenge,
                                "signed_challenge": self._get_sign_challenge(self.__last_challenge)
                            }).decode('utf-8'))

                            await ws.send_str(orjson.dumps({
                                "event": "subscribe",
                                "feed": "open_positions",
                                "api_key": self.__api_key,
                                'original_challenge': self.__last_challenge,
                                "signed_challenge": self._get_sign_challenge(self.__last_challenge)
                            }).decode('utf-8'))

                        elif msg_data.get('feed') == 'balances':
                            if msg_data.get('balance_value'):
                                self.balance['timestamp'] = round(datetime.utcnow().timestamp())
                                self.balance['total'] = msg_data['portfolio_value']
                                self.balance['free'] = msg_data['available_margin']

                        elif msg_data.get('feed') == 'open_positions':
                            for position in msg_data.get('positions', []):
                                side = PositionSideEnum.LONG if position['balance'] >= 0 else PositionSideEnum.SHORT
                                amount_usd = position['balance'] * position['mark_price']
                                self.positions.update({position['instrument'].upper(): {
                                    'side': side,
                                    'amount_usd': amount_usd,
                                    'amount': position['balance'],
                                    'entry_price': position['entry_price'],
                                    'unrealized_pnl_usd': 0,
                                    'realized_pnl_usd': position['pnl'],
                                    'lever': self.leverage
                                }})
                        elif msg_data.get('fills') and msg_data['feed'] != 'fills_snapshot':
                            for fill in msg_data['fills']:
                                # ex = [{'instrument': 'PF_DASHUSD', 'time': 1699002267263, 'price': 28.438, 'seq': 100, 'buy': True, 'qty': 5.5, 'remaining_order_qty': 24.5, 'order_id': 'ee1e5189-422f-4a5a-9584-acbc2017ef2c', 'cli_ord_id': 'api_deal_59cf2b524ac74fa0b414', 'fill_id': '6dce06eb-2012-4aaf-b52c-8b189bc9fbd0', 'fill_type': 'taker', 'fee_paid': 0.0782045, 'fee_currency': 'USD', 'taker_order_type': 'lmt', 'order_type': 'lmt'}, {'instrument': 'PF_DASHUSD', 'time': 1699002267392, 'price': 28.438, 'seq': 101, 'buy': True, 'qty': 23.7, 'remaining_order_qty': 0.8000000000000007, 'order_id': 'ee1e5189-422f-4a5a-9584-acbc2017ef2c', 'cli_ord_id': 'api_deal_59cf2b524ac74fa0b414', 'fill_id': '937458d8-5e96-4f4c-850d-eaa2af5fbe34', 'fill_type': 'maker', 'fee_paid': 0.13479612, 'fee_currency': 'USD', 'taker_order_type': 'ioc', 'order_type': 'lmt'}, {'instrument': 'PF_DASHUSD', 'time': 1699002267585, 'price': 28.438, 'seq': 102, 'buy': True, 'qty': 0.8, 'remaining_order_qty': 0.0, 'order_id': 'ee1e5189-422f-4a5a-9584-acbc2017ef2c', 'cli_ord_id': 'api_deal_59cf2b524ac74fa0b414', 'fill_id': '4bc83d88-948e-4287-af42-53e93c200d1d', 'fill_type': 'maker','fee_paid': 0.00455008, 'fee_currency': 'USD', 'taker_order_type': 'ioc','order_type': 'lmt'}]
                                status = OrderStatus.PARTIALLY_EXECUTED
                                if fill['remaining_order_qty'] == 0:
                                    status = OrderStatus.FULLY_EXECUTED
                                if order := self.orders.get(fill['order_id']):
                                    qty_coin = order['factual_amount_coin'] + fill['qty']
                                    qty_usd = order['factual_amount_usd'] + (fill['qty'] * fill['price'])
                                    factual_price = qty_usd / qty_coin
                                    self.last_price['buy' if fill['buy'] else 'sell'] = factual_price
                                    self.orders.update({fill['order_id']: {
                                        'factual_price': factual_price,
                                        'exchange_order_id': fill['order_id'],
                                        'exchange': self.EXCHANGE_NAME,
                                        'status': status,
                                        'factual_amount_coin': qty_coin,
                                        'factual_amount_usd': qty_usd,
                                        'datetime_update': datetime.utcnow(),
                                        'ts_update': int(time.time() * 1000)
                                    }})
                                    # print(self.orders)
                                else:
                                    self.last_price['buy' if fill['buy'] else 'sell'] = fill['price']
                                    self.orders.update({fill['order_id']: {
                                        'factual_price': fill['price'],
                                        'exchange_order_id': fill['order_id'],
                                        'exchange': self.EXCHANGE_NAME,
                                        'status': status,
                                        'factual_amount_coin': fill['qty'],
                                        'factual_amount_usd': fill['qty'] * fill['price'],
                                        'datetime_update': datetime.utcnow(),
                                        'ts_update': int(time.time() * 1000)
                                    }})
                                    # print(self.orders)

                                # self.last_price['sell' if order['direction'] else 'buy'] = float(order['limit_price'])
                                # status = OrderStatus.PROCESSING
                                #
                                # if msg_data['reason'] == 'full_fill':
                                #     status = OrderStatus.FULLY_EXECUTED
                                # elif msg_data['reason'] == 'partial_fill':
                                #     status = OrderStatus.PARTIALLY_EXECUTED
                                # elif msg_data['reason'] in ['cancelled_by_user', 'cancelled_by_admin', 'not_enough_margin']:
                                #     status = OrderStatus.NOT_EXECUTED
                                #
                                # if status:
                                #     price = 0 if status in [OrderStatus.PROCESSING, OrderStatus.NOT_EXECUTED] \
                                #         else order['limit_price']
                                #     amount = 0 if status in [OrderStatus.PROCESSING, OrderStatus.NOT_EXECUTED] \
                                #         else order['filled']
                                #     result = {
                                #         'exchange_order_id': order['order_id'],
                                #         'exchange': self.EXCHANGE_NAME,
                                #         'status': status,
                                #         'factual_price': price,
                                #         'factual_amount_coin': amount,
                                #         'factual_amount_usd': 0 if status in [OrderStatus.PROCESSING,
                                #                                               OrderStatus.NOT_EXECUTED] \
                                #             else amount * price,
                                #         'datetime_update': datetime.utcnow(),
                                #         'ts_update': time.time() * 1000
                                #     }
                                #
                                #     if self.symbol.upper() == order['instrument'].upper() \
                                #             and result['status'] != OrderStatus.PROCESSING:
                                #         self.orders.update({order['order_id']: result})


if __name__ == '__main__':
    import configparser
    import sys

    config = configparser.ConfigParser()
    config.read(sys.argv[1], "utf-8")
    client = KrakenClient(keys=config['KRAKEN'],
                          leverage=float(config['SETTINGS']['LEVERAGE']),
                          max_pos_part=int(config['SETTINGS']['PERCENT_PER_MARKET']))
    # client.markets_list = ['ETH', 'RUNE', 'SNX', 'ENJ', 'DOT', 'LINK', 'ETC', 'DASH', 'XLM', 'WAVES']
    client.run_updater()
    time.sleep(3)


    # print(client.get_balance())
    # print()
    # print(client.positions)
    # print()
    # print(client.new_get_available_balance())
    # print(client.get_markets())
    # time.sleep(5)

    async def test_order():
        async with aiohttp.ClientSession() as session:
            # print(client.orderbook)
            ob = client.get_orderbook('pf_dashusd')
            price = ob['bids'][5][0]
            client.price = price
            client.amount = 1
            # client.fit_sizes(1, price, 'pf_dashusd')
            data = await client.create_order('pf_dashusd',
                                             'buy',
                                             session,
                                             client_id=f"api_deal_{str(uuid.uuid4()).replace('-', '')[:20]}")
            # data = client.get_balance()
            # print(client.get_order_by_id(data['exchange_order_id']))

            # client.cancel_all_orders()


    # #
    # #
    # time.sleep(5)
    # print(client.tickers)
    # print('\n\n\n\n')
    # print(client.instruments)
    # asyncio.run(test_order())

    while True:
        time.sleep(5)

    # time.sleep(5)

        print(client.get_all_tops())
