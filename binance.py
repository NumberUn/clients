import asyncio
from datetime import datetime
import hashlib
import hmac
import threading
import time
import traceback
import uuid
from urllib.parse import urlencode

import aiohttp

import requests

from clients.core.base_client import BaseClient
from clients.core.enums import ConnectMethodEnum, EventTypeEnum, PositionSideEnum, ResponseStatus, OrderStatus, \
    ClientsOrderStatuses
from core.wrappers import try_exc_regular, try_exc_async


import orjson


class BinanceClient(BaseClient):
    BASE_WS = 'wss://fstream.binance.com/ws/'
    BASE_URL = 'https://fapi.binance.com'
    EXCHANGE_NAME = 'BINANCE'
    urlMarkets = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    urlOrderbooks = "https://fapi.binance.com/fapi/v1/depth?limit=5&symbol="

    def __init__(self, keys, leverage, markets_list=[], max_pos_part=20):
        super().__init__()
        self.markets_list = markets_list
        self.max_pos_part = max_pos_part
        self.taker_fee = 0.00036
        self.leverage = leverage
        self.__api_key = keys['API_KEY']
        self.__secret_key = keys['API_SECRET']
        self.headers = {"Content-Type": "application/json", 'X-MBX-APIKEY': self.__api_key}
        self.symbol_is_active = False
        self.count_flag = False
        self.error_info = None
        self.markets = {}
        self.message_to_rabbit_list = []
        self.requestLimit = 1200
        self.balance = {
            'total': 0.0,
            'avl_balance': 0.0,
            'timestamp': round(datetime.utcnow().timestamp())
        }
        self.last_price = {
            'sell': 0,
            'buy': 0
        }
        self.orders = {}
        self.positions = {}
        self.orderbook = {}
        self._check_symbol_value()
        self.amount = 0
        self.price = 0
        self.get_markets()
        self._loop_public = asyncio.new_event_loop()
        self._loop_private = asyncio.new_event_loop()
        self._loop_message = asyncio.new_event_loop()
        self.wsd_public = threading.Thread(target=self._run_forever,
                                           args=[ConnectMethodEnum.PUBLIC, self._loop_public],
                                           daemon=True)
        self.wsu_private = threading.Thread(target=self._run_forever,
                                            args=[ConnectMethodEnum.PRIVATE, self._loop_private],
                                            daemon=True)
        self.bal_check = threading.Thread(target=self._balance)
        self.lk_check = threading.Thread(target=self._ping_listen_key)
        self.get_real_balance()
        self._get_listen_key()
        self.get_position()
        self.pings = []

    @try_exc_regular
    def cancel_all_orders(self, orderID=None) -> dict:
        return self.__cancel_open_orders()

    @try_exc_regular
    def get_positions(self) -> dict:
        return self.positions

    @try_exc_regular
    def get_balance(self) -> float:
        if time.time() - self.balance['timestamp'] > 60:
            self.get_real_balance()
        return self.balance['total']

    @try_exc_regular
    def get_orderbook(self, symbol) -> dict:
        while not self.orderbook.get(symbol):
            print(f"{self.EXCHANGE_NAME}: CAN'T GET OB {symbol}")
            time.sleep(0.01)
        return self.orderbook[symbol]

    @try_exc_regular
    def get_all_tops(self):
        time_start = time.time()
        tops = {}
        for symbol, orderbook in self.orderbook.items():
            print(symbol, orderbook, '\n')
            if len(orderbook['bids']) < 10 or len(orderbook['asks']) < 10:
                asyncio.run(self.get_multi_orderbook(symbol))
            coin = symbol.upper().split('USD')[0]
            tops.update({self.EXCHANGE_NAME + '__' + coin:
                             {'top_bid': orderbook['bids'][0][0], 'top_ask': orderbook['asks'][0][0],
                              'bid_vol': orderbook['bids'][0][1], 'ask_vol': orderbook['asks'][0][1],
                              'ts_exchange': orderbook['timestamp']}})
        return tops

    @try_exc_async
    async def get_multi_orderbook(self, symbol):
        async with aiohttp.ClientSession() as session:
            async with session.get(url=self.urlOrderbooks + symbol) as response:
                ob = await response.json()
                print(ob)
                try:
                    return {'top_bid': ob['bids'][0][0], 'top_ask': ob['asks'][0][0],
                            'bid_vol': ob['bids'][0][1], 'ask_vol': ob['asks'][0][1],
                            'ts_exchange': ob['E']}
                except Exception as error:
                    print('Error from Client. Binance Module:', symbol, error)

    @try_exc_regular
    def get_last_price(self, side: str) -> float:
        return self.last_price[side.lower()]

    @try_exc_regular
    def get_markets(self):
        markets = requests.get(url=self.urlMarkets, headers=self.headers).json()
        for market in markets['symbols']:
            if market['marginAsset'] == 'USDT' and market['contractType'] == 'PERPETUAL' and market[
                'underlyingType'] == 'COIN':
                if market['status'] == 'TRADING':
                    coin = market['baseAsset']
                    self.markets.update({coin: market['symbol']})
        return self.markets

    @try_exc_regular
    def run_updater(self) -> None:
        self.lk_check.start()
        self.bal_check.start()
        self.wsd_public.start()
        self.wsu_private.start()

    @try_exc_regular
    def _run_forever(self, type, loop) -> None:
        while True:
            try:
                loop.run_until_complete(self._run_loop(type))
            except Exception:  # noqa
                traceback.print_exc()

    @try_exc_async
    async def _run_loop(self, type) -> None:
        async with aiohttp.ClientSession() as session:
            if type == ConnectMethodEnum.PUBLIC:
                await self._symbol_data_getter(session)
            elif type == ConnectMethodEnum.PRIVATE and self.listen_key:
                await self._user_data_getter(session)

    # PUBLIC -----------------------------------------------------------------------------------------------------------
    @try_exc_regular
    def _check_symbol_value(self) -> None:
        url_path = '/fapi/v1/exchangeInfo'
        response = requests.get(self.BASE_URL + url_path).json()
        self.exchange_info = response

    @try_exc_regular
    def get_sizes_for_symbol(self, symbol):
        for key, value in self.exchange_info.items():
            # print(key)
            if 'symbol' in key:
                for data in value:
                    if data['symbol'] == symbol.upper():
                        if data['status'] == 'TRADING' and data['contractType'] == 'PERPETUAL':
                            quantity_precision = data['quantityPrecision']
                            price_precision = data['pricePrecision']
                            for fltr in data['filters']:
                                if fltr['filterType'] == 'PRICE_FILTER':
                                    tick_size = float(fltr['tickSize'])
                                elif fltr['filterType'] == 'LOT_SIZE':
                                    step_size = float(fltr['stepSize'])
                            return tick_size, step_size, quantity_precision, price_precision

    @try_exc_regular
    def __check_ob(self, ob: dict, side: str, symbol) -> None:
        reformat_ob = [[float(x[0]), float(x[1])] for x in ob[side]]
        # if not len(self.orderbook[self.symbol][side]):
        self.orderbook[symbol][side] = []
        for new_order in reformat_ob:
            if len(self.orderbook[symbol][side]) > 10:
                return
            if new_order[1] > 0:
                self.orderbook[symbol][side].append(new_order)

    @try_exc_async
    async def __orderbook_update(self, ob: dict) -> None:
        symbol = ob['s']
        if not self.orderbook.get(symbol):
            self.orderbook.update({symbol: {'asks': [], 'bids': []}})
        ask = self.orderbook[symbol]['asks'][0][0] if len(self.orderbook[symbol]['asks']) else 0
        bid = self.orderbook[symbol]['bids'][0][0] if len(self.orderbook[symbol]['bids']) else 0
        if ob.get('asks'):
            self.__check_ob(ob, 'asks', symbol)
        if ob.get('bids'):
            self.__check_ob(ob, 'bids', symbol)
        self.orderbook[symbol]['timestamp'] = ob['E']
        if len(self.orderbook[symbol]['asks']) and len(self.orderbook[symbol]['bids']):
            if ask != self.orderbook[symbol]['asks'][0][0] or bid != self.orderbook[symbol]['bids'][0][0]:
                self.count_flag = True

    @try_exc_async
    async def _symbol_data_getter(self, session: aiohttp.ClientSession) -> None:
        async with session.ws_connect(self.BASE_WS + list(self.markets.keys())[0].lower()) as ws:
            # self.markets_list = list(self.markets.keys())[:10]
            for symbol in self.markets_list:
                if market := self.markets.get(symbol):
                    await ws.send_str(orjson.dumps({
                        'id': 1,
                        'method': 'SUBSCRIBE',
                        'params': [f"{market.lower()}@depth@100ms"]
                    }).decode('utf-8'))

            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    payload = orjson.loads(msg.data)
                    if payload.get('e') == 'depthUpdate':
                        payload['asks'] = [x for x in payload.get('a', [])]
                        payload['bids'] = [x for x in payload.get('b', [])][::-1]
                        await self.__orderbook_update(payload)

    # PRIVATE ----------------------------------------------------------------------------------------------------------
    @staticmethod
    @try_exc_regular
    def _prepare_query(params: dict) -> str:
        return urlencode(params)

    @try_exc_regular
    def get_available_balance(self):
        return super().get_available_balance(
            leverage=self.leverage,
            max_pos_part=self.max_pos_part,
            positions=self.positions,
            balance=self.balance)

    # def get_available_balance(self, side):
    #     position_value = 0
    #     position_value_abs = 0
    #     for symbol, position in self.positions.items():
    #         if position.get('amount_usd'):
    #             position_value += position['amount_usd']
    #             position_value_abs += abs(position['amount_usd'])
    #
    #     available_margin = self.balance['total'] * self.leverage
    #     if position_value_abs > available_margin:
    #         if position_value > 0:
    #             if side == 'buy':
    #                 return available_margin - position_value
    #             elif side == 'sell':
    #                 return available_margin + position_value
    #         else:
    #             if side == 'buy':
    #                 return available_margin + abs(position_value)
    #             elif side == 'sell':
    #                 return available_margin - abs(position_value)
    #     if side == 'buy':
    #         return available_margin - position_value
    #     elif side == 'sell':
    #         return available_margin + position_value

    @try_exc_regular
    def _create_signature(self, query: str) -> str:
        if self.__secret_key is None or self.__secret_key == "":
            raise Exception("Secret key are required")

        return hmac.new(self.__secret_key.encode(), msg=query.encode(), digestmod=hashlib.sha256).hexdigest()

    @try_exc_regular
    def _balance(self) -> None:
        while True:
            self.get_real_balance()
            time.sleep(59)

    @try_exc_regular
    def get_position(self):
        self.positions = {}
        url_path = "/fapi/v2/account"
        payload = {"timestamp": int(time.time() * 1000)}

        query_string = self._prepare_query(payload)
        payload["signature"] = self._create_signature(query_string)
        query_string = self._prepare_query(payload)

        res = requests.get(url=self.BASE_URL + url_path + '?' + query_string, headers=self.headers).json()
        if isinstance(res, dict):
            for s in res.get('positions', []):
                if float(s['positionAmt']):
                    self.positions.update({s['symbol']: {
                        'side': PositionSideEnum.LONG if float(s['positionAmt']) > 0 else PositionSideEnum.SHORT,
                        'amount_usd': float(s['positionAmt']) * float(s['entryPrice']),
                        'amount': float(s['positionAmt']),
                        'entry_price': float(s['entryPrice']),
                        'unrealized_pnl_usd': float(s['unrealizedProfit']),
                        'realized_pnl_usd': 0,
                        'lever': self.leverage
                    }})
        else:
            time.sleep(5)
            self.get_position()

    @try_exc_regular
    def get_real_balance(self) -> [float, float]:
        url_path = "/fapi/v2/balance"
        payload = {
            "timestamp": int(time.time() * 1000),
            "recvWindow": int((time.time() + 2) * 1000)
        }

        query_string = self._prepare_query(payload)
        payload["signature"] = self._create_signature(query_string)
        query_string = self._prepare_query(payload)
        res = requests.get(url=self.BASE_URL + url_path + '?' + query_string, headers=self.headers).json()
        if isinstance(res, list):
            for s in res:
                if s['asset'] == 'USDT':
                    self.balance['timestamp'] = round(datetime.utcnow().timestamp())
                    self.balance['total'] = float(s['balance']) + float(s['crossUnPnl'])
                    self.balance['free'] = float(s['availableBalance'])
                    return float(s['balance']) + float(s['crossUnPnl']), float(s['availableBalance'])
        else:
            # print(res)
            time.sleep(120)
            return self.get_real_balance()

    @try_exc_async
    async def get_historical_price(self, session, symbol, time_):
        url_path = "/fapi/v1/markPriceKlines"
        payload = {
            'symbol': symbol,
            "interval": '1m',
            "startTime": time_ - 3,
            'endTime': time_ + 3
        }
        query_string = self._prepare_query(payload)
        payload["signature"] = self._create_signature(query_string)
        query_string = self._prepare_query(payload)

        async with session.get(url=self.BASE_URL + url_path + '?' + query_string, headers=self.headers) as resp:
            price = await resp.json()

            if price:
                price = (float(price[0][1]) + float(price[0][4])) / 2
                return price
            # else:
            #     time_ = int(time_)
            #     return await self.get_historical_price(session, symbol, time_)

    @try_exc_async
    async def get_funding_history(self, session, symbol):
        url_path = "/fapi/v1/fundingRate"
        payload = {
            "timestamp": int(time.time() * 1000),
            'symbol': symbol,
            'limit': 100
        }

        query_string = self._prepare_query(payload)
        payload["signature"] = self._create_signature(query_string)
        query_string = self._prepare_query(payload)

        async with session.get(url=self.BASE_URL + url_path + '?' + query_string, headers=self.headers) as resp:
            return await resp.json()

    @try_exc_async
    async def get_funding_payments(self, session):
        url_path = "/fapi/v1/income"
        payload = {
            'incomeType': 'FUNDING_FEE',
            "timestamp": int(time.time() * 1000),
            "recvWindow": int((time.time() + 2) * 1000),
            'limit': 100
        }
        query_string = self._prepare_query(payload)
        payload["signature"] = self._create_signature(query_string)
        query_string = self._prepare_query(payload)
        symbols = {}

        async with session.get(url=self.BASE_URL + url_path + '?' + query_string, headers=self.headers) as resp:
            funding_payments = await resp.json()

            for fund in funding_payments:
                time.sleep(15)
                symbol = fund['symbol']

                if not symbols.get(symbol):
                    symbols.update({symbol: await self.get_funding_history(session, symbol)})

                rate = [x['fundingRate'] for x in symbols[symbol] if abs(x['fundingTime'] - fund['time']) < 3000]

                if rate:
                    price = await self.get_historical_price(session, symbol, fund['time'])
                    if price:
                        fund.update({'rate': rate[0],
                                     'price': price,
                                     'positionSize': (float(fund['income']) / -float(rate[0])) / price,
                                     'market': symbol,
                                     'payment': fund['income'],
                                     'datetime': datetime.fromtimestamp(fund['time'] / 1000),
                                     'asset': 'USDT'})

        return funding_payments

    @try_exc_regular
    def fit_sizes(self, amount, price, symbol):
        tick_size, step_size, quantity_precision, price_precision = self.get_sizes_for_symbol(symbol)
        rounded_amount = round(amount / step_size) * step_size
        self.amount = round(rounded_amount, quantity_precision)
        rounded_price = round(price / tick_size) * tick_size
        self.price = round(rounded_price, price_precision)
        return self.price, self.amount

    @try_exc_async
    async def create_order(self, symbol, side, session, expire=5000, client_id=None) -> dict:
        side = side.upper()
        time_sent = datetime.utcnow().timestamp()
        url_path = '/fapi/v1/order?'
        query_string = f"timestamp={int(time.time() * 1000)}&symbol={symbol}&side={side}&type=LIMIT&" \
                       f"price={self.price}&quantity={self.amount}&timeInForce=GTC&" \
                       f"recvWindow={time.time() * 1000 + expire}&newClientOrderId={client_id}"
        query_string += f'&signature={self._create_signature(query_string)}'
        print(f"{self.EXCHANGE_NAME} BODY: {query_string}")
        try:
            async with session.post(url=self.BASE_URL + url_path + query_string, headers=self.headers) as resp:
                res = await resp.json()
                print(f'{self.EXCHANGE_NAME} RESPONSE: {res}')
                self.LAST_ORDER_ID = res.get('orderId', 'default')
                exchange_order_id = res.get('orderId', 'default')
                timestamp = 0000000000000
                if res.get('code'):
                    status = ResponseStatus.ERROR
                    self.error_info = res
                elif res.get('status'):
                    status = ResponseStatus.SUCCESS
                    timestamp = res['updateTime']
                    utc_diff = round((datetime.utcnow().timestamp() - time.time()) * 1000)
                    timestamp += utc_diff
                else:
                    status = ResponseStatus.NO_CONNECTION
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

    @try_exc_regular
    def __cancel_open_orders(self) -> dict:
        url_path = "/fapi/v1/allOpenOrders"
        reses = []
        for symbol in self.markets.values():
            payload = {
                "timestamp": int(time.time() * 1000),
                'symbol': symbol
            }
            query_string = self._prepare_query(payload)
            payload["signature"] = self._create_signature(query_string)
            query_string = self._prepare_query(payload)
            res = requests.delete(url=self.BASE_URL + url_path + '?' + query_string, headers=self.headers).json()
            reses.append(res)
        return reses

    @try_exc_async
    async def get_orderbook_by_symbol(self, symbol):
        async with aiohttp.ClientSession() as session:
            url_path = "/fapi/v1/depth"
            payload = {"symbol": symbol}
            query_string = self._prepare_query(payload)
            async with session.get(url=self.BASE_URL + url_path + "?" + query_string, headers=self.headers) as resp:
                res = await resp.json()
                orderbook = {
                    'asks': [[float(x[0]), float(x[1])] for x in res['asks']],
                    'bids': [[float(x[0]), float(x[1])] for x in res['bids']],
                    'timestamp': int(time.time() * 1000)
                }
                return orderbook

    @try_exc_async
    async def get_all_orders(self, symbol: str, session: aiohttp.ClientSession) -> list:
        url_path = "/fapi/v1/allOrders"
        payload = {
            "timestamp": int(time.time() * 1000),
            "symbol": symbol,
            "startTime": int((time.time() - 86400) * 1000),
            "recvWindow": int((time.time() + 2) * 1000)
        }
        query_string = self._prepare_query(payload)
        payload["signature"] = self._create_signature(query_string)
        query_string = self._prepare_query(payload)
        orders = []
        async with session.get(url=self.BASE_URL + url_path + "?" + query_string, headers=self.headers) as resp:
            res = await resp.json()
            try:
                for order in res:
                    if order.get('status') == ClientsOrderStatuses.FILLED and float(order['origQty']) > float(
                            order['executedQty']):
                        status = OrderStatus.PARTIALLY_EXECUTED
                    elif order.get('status') == ClientsOrderStatuses.FILLED:
                        status = OrderStatus.FULLY_EXECUTED
                    else:
                        status = OrderStatus.NOT_EXECUTED

                    orders.append(
                        {
                            'id': uuid.uuid4(),
                            'datetime': datetime.fromtimestamp(int(order['time'] / 1000)),
                            'ts': int(order['time']),
                            'context': 'web-interface' if not 'api_' in order['clientOrderId'] else
                            order['clientOrderId'].split('_')[1],
                            'parent_id': uuid.uuid4(),
                            'exchange_order_id': order['orderId'],
                            'type': order['timeInForce'],
                            'status': status,
                            'exchange': self.EXCHANGE_NAME,
                            'side': order['side'].lower(),
                            'symbol': symbol,
                            'expect_price': float(order['price']),
                            'expect_amount_coin': float(order['origQty']),
                            'expect_amount_usd': float(order['price']) * float(order['origQty']),
                            'expect_fee': self.taker_fee,
                            'factual_price': float(order['avgPrice']),
                            'factual_amount_coin': float(order['executedQty']),
                            'factual_amount_usd': float(order['price']) * float(order['origQty']),
                            'order_place_time': 0,
                            'factual_fee': self.taker_fee,
                            'env': '-',
                            'datetime_update': datetime.utcnow(),
                            'ts_update': int(time.time() * 1000),
                            'client_id': order['clientOrderId']
                        }
                    )
            except:
                traceback.print_exc()

        return orders

    @try_exc_async
    async def get_order_by_id(self, symbol, order_id: str, session: aiohttp.ClientSession) -> dict:
        url_path = "/fapi/v1/order"
        payload = {
            "timestamp": int(time.time() * 1000),
            "symbol": symbol,
            "orderId": int(order_id),
            "recvWindow": int((time.time() + 2) * 1000)
        }

        query_string = self._prepare_query(payload)
        payload["signature"] = self._create_signature(query_string)
        query_string = self._prepare_query(payload)

        async with session.get(url=self.BASE_URL + url_path + "?" + query_string, headers=self.headers) as resp:
            res = await resp.json()
            if res.get('msg') == 'Order does not exist.':
                return {
                    'exchange_order_id': order_id,
                    'exchange': self.EXCHANGE_NAME,
                    'status': 'Order does not exist.',
                    'factual_price': 0,
                    'factual_amount_coin': 0,
                    'factual_amount_usd': 0,
                    'datetime_update': datetime.utcnow(),
                    'ts_update': int((time.time() - 3600) * 1000)}
            if res.get('status') == ClientsOrderStatuses.FILLED and res.get('time', 0) == res.get('updateTime'):
                status = OrderStatus.FULLY_EXECUTED
            elif res.get('status') == ClientsOrderStatuses.FILLED and float(res['origQty']) > float(
                    res['executedQty']):
                status = OrderStatus.PARTIALLY_EXECUTED
            else:
                status = OrderStatus.NOT_EXECUTED

            return {
                'exchange_order_id': order_id,
                'exchange': self.EXCHANGE_NAME,
                'status': status,
                'factual_price': float(res['avgPrice']),
                'factual_amount_coin': float(res['executedQty']),
                'factual_amount_usd': float(res['executedQty']) * float(res['avgPrice']),
                'datetime_update': datetime.utcnow(),
                'ts_update': int((time.time() - 3600) * 1000)
            }

    @try_exc_regular
    def _get_listen_key(self) -> None:
        response = requests.post(
            self.BASE_URL + '/fapi/v1/listenKey', headers=self.headers).json()

        if response.get('code'):
            raise Exception(f'ListenKey Error: {response}')
        self.listen_key = response.get('listenKey')

    @try_exc_regular
    def _ping_listen_key(self) -> None:
        while True:
            time.sleep(59 * 60)
            requests.put(self.BASE_URL + '/fapi/v1/listenKey', headers={'X-MBX-APIKEY': self.__api_key})

    @try_exc_async
    async def _user_data_getter(self, session: aiohttp.ClientSession) -> None:
        async with session.ws_connect(self.BASE_WS + self.listen_key) as ws:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = orjson.loads(msg.data)
                    if data['e'] == EventTypeEnum.ACCOUNT_UPDATE and data['a']['P']:
                        for p in data['a']['P']:
                            if p['ps'] in PositionSideEnum.all_position_sides() and float(p['pa']):
                                self.positions.update({p['s'].upper(): {
                                    'side': PositionSideEnum.LONG if float(p['pa']) > 0 else PositionSideEnum.SHORT,
                                    'amount_usd': float(p['pa']) * float(p['ep']),
                                    'amount': float(p['pa']),
                                    'entry_price': float(p['ep']),
                                    'unrealized_pnl_usd': float(p['up']),
                                    'realized_pnl_usd': 0,
                                    'lever': self.leverage
                                }})

                    elif data['e'] == EventTypeEnum.ORDER_TRADE_UPDATE:
                        # print(f'{data=}')
                        self.last_price[data['o']['S'].lower()] = float(data['o']['ap'])
                        status = None
                        if data['o']['X'] == ClientsOrderStatuses.NEW:
                            status = OrderStatus.PROCESSING
                        elif data['o']['X'] == ClientsOrderStatuses.FILLED and data['o']['m'] is False:
                            status = OrderStatus.FULLY_EXECUTED
                        elif data['o']['X'] == ClientsOrderStatuses.FILLED and data['o']['m'] is True:
                            status = OrderStatus.FULLY_EXECUTED
                        elif data['o']['X'] == ClientsOrderStatuses.PARTIALLY_FILLED:
                            status = OrderStatus.PARTIALLY_EXECUTED
                        elif data['o']['X'] == ClientsOrderStatuses.CANCELED and data['o']['z'] == '0':
                            status = OrderStatus.NOT_EXECUTED

                        if status:
                            result = {
                                'exchange_order_id': data['o']['i'],
                                'exchange': self.EXCHANGE_NAME,
                                'status': status,
                                'factual_price': 0 if status in [OrderStatus.PROCESSING,
                                                                 OrderStatus.NOT_EXECUTED] else float(data['o']['ap']),
                                'factual_amount_coin': 0 if status in [OrderStatus.PROCESSING,
                                                                       OrderStatus.NOT_EXECUTED] else float(
                                    data['o']['z']),
                                'factual_amount_usd': 0 if status in [OrderStatus.PROCESSING,
                                                                      OrderStatus.NOT_EXECUTED] else float(
                                    data['o']['z']) * float(data['o']['ap']),
                                'datetime_update': datetime.utcnow(),
                                'ts_update': int((time.time() - 3600) * 1000)
                            }

                            if result['status'] != OrderStatus.PROCESSING:
                                self.orders.update({data['o']['i']: result})


if __name__ == '__main__':
    import configparser
    import sys

    config = configparser.ConfigParser()
    config.read(sys.argv[1], "utf-8")
    client = BinanceClient(keys=config['BINANCE'],
                           leverage=float(config['SETTINGS']['LEVERAGE']),
                           max_pos_part=int(config['SETTINGS']['PERCENT_PER_MARKET']),
                           markets_list=['ETH', 'BTC', 'LTC', 'BCH', 'SOL', 'MINA', 'XRP', 'PEPE', 'CFX', 'FIL'])
    client.run_updater()
    # time.sleep(3)
    # print(client.get_balance())
    # print()
    # print(client.positions)
    # print()
    # print(client.new_get_available_balance())
    # client.get_markets()
    # time.sleep(5)

    # async def test_order():
    #     async with aiohttp.ClientSession() as session:
    #         client.fit_amount(-0.017)
    #         price = client.get_orderbook()[client.symbol]['bids'][10][0]
    #         data = await client.create_order(price,
    #                                          'buy',
    #                                          session,
    #                                          client_id=f"api_deal_{str(uuid.uuid4()).replace('-', '')[:20]}")
    #         # data = await client.get_orderbook_by_symbol()
    #         print(data)
    #         client.cancel_all_orders()
    #
    #
    # asyncio.run(test_order())

    while True:
        time.sleep(5)
        for ob in client.orderbook.values():
            print(ob)

# {'symbol': 'ETHUSDT', 'pair': 'ETHUSDT', 'contractType': 'PERPETUAL', 'deliveryDate': 4133404800000,
#  'onboardDate': 1630911600000, 'status': 'TRADING', 'maintMarginPercent': '2.5000', 'requiredMarginPercent': '5.0000',
#  'baseAsset': 'ETH', 'quoteAsset': 'USDT', 'marginAsset': 'USDT', 'pricePrecision': 2, 'quantityPrecision': 3,
#  'baseAssetPrecision': 8, 'quotePrecision': 8, 'underlyingType': 'COIN', 'underlyingSubType': [], 'settlePlan': 0,
#  'triggerProtect': '0.0500', 'liquidationFee': '0.015000', 'marketTakeBound': '0.05',
#  'filters': [{'minPrice': '39.90', 'maxPrice': '1000000', 'filterType': 'PRICE_FILTER', 'tickSize': '0.10'},
#              {'stepSize': '0.001', 'filterType': 'LOT_SIZE', 'maxQty': '10000', 'minQty': '0.001'},
#              {'stepSize': '0.001', 'filterType': 'MARKET_LOT_SIZE', 'maxQty': '50', 'minQty': '0.001'},
#              {'limit': 200, 'filterType': 'MAX_NUM_ORDERS'}, {'limit': 10, 'filterType': 'MAX_NUM_ALGO_ORDERS'},
#              {'notional': '5', 'filterType': 'MIN_NOTIONAL'},
#              {'multiplierDown': '0.9500', 'multiplierUp': '1.0500', 'multiplierDecimal': '4',
#               'filterType': 'PERCENT_PRICE'}],
#  'orderTypes': ['LIMIT', 'MARKET', 'STOP', 'STOP_MARKET', 'TAKE_PROFIT', 'TAKE_PROFIT_MARKET', 'TRAILING_STOP_MARKET'],
#  'timeInForce': ['GTC', 'IOC', 'FOK', 'GTX']}
