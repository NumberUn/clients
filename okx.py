import traceback
import aiohttp
import asyncio
import time
import hmac
import base64
import json
import threading
import string
from datetime import datetime
import requests
import random
import uuid

from clients.core.base_client import BaseClient
from clients.core.enums import ResponseStatus, OrderStatus
from core.wrappers import try_exc_regular, try_exc_async


class OkxClient(BaseClient):
    WS_PBL = "wss://wsaws.okx.com:8443/ws/v5/public"
    WS_PRV = "wss://wsaws.okx.com:8443/ws/v5/private"
    BASE_URL = 'https://www.okx.com'
    headers = {'Content-Type': 'application/json'}
    EXCHANGE_NAME = 'OKX'
    LAST_ORDER_ID = 'default'

    def __init__(self, multibot=None, keys=None, leverage=None, state='Bot', markets_list=[],
                 max_pos_part=20, finder=None, ob_len=4, market_finder=None):
        super().__init__()
        self.multibot = multibot
        self.finder = finder
        self.market_finder = market_finder
        self.max_pos_part = max_pos_part
        self.markets_list = markets_list
        self.leverage = leverage
        self.state = state
        self.ob_len = ob_len
        self.taker_fee = 0.0005
        self.maker_fee = 0.0002
        if keys:
            self.public_key = keys['API_KEY']
            self.secret_key = keys['API_SECRET']
            self.passphrase = keys['PASSPHRASE']
        self.get_position()
        self.instruments = self.get_instruments()
        self.markets = self.get_markets()
        self.balance = {}
        self.get_real_balance()
        self.error_info = None
        self.orderbook = {}
        self.orders = {}
        self.async_tasks = []
        self.deleted_orders = []
        self.responses = {}
        self.time_sent = 0
        self.orders_timestamps = {}
        if multibot:
            self.cancel_all_orders()

    def change_leverage(self):
        for symbol in self.markets_list:
            self.set_leverage(self.markets[symbol])
            time.sleep(0.1)

    @staticmethod
    @try_exc_regular
    def id_generator(size=6, chars=string.ascii_letters):
        return ''.join(random.choice(chars) for _ in range(size))

    @try_exc_regular
    def run_updater(self):
        wst_public = threading.Thread(target=self._run_ws, args=['public', asyncio.new_event_loop(), self.WS_PBL])
        wst_public.daemon = True
        wst_public.start()
        if self.state == 'Bot':
            wst_private = threading.Thread(target=self._run_ws, args=['private', asyncio.new_event_loop(), self.WS_PRV])
            wst_private.daemon = True
            wst_private.start()
            wst_orders = threading.Thread(target=self._run_order_loop, args=[asyncio.new_event_loop()])
            wst_orders.daemon = True
            wst_orders.start()
        self.change_leverage()
        while True:
            if set(self.orderbook) == set([y for x, y in self.markets.items() if x in self.markets_list]):
                print(f"{self.EXCHANGE_NAME} ALL MARKETS FETCHED")
                break
            time.sleep(0.1)

    @try_exc_regular
    def _run_order_loop(self, loop):
        while True:
            loop.run_until_complete(self.orders_processing_loop(loop))

    @try_exc_async
    async def orders_processing_loop(self, loop):
        async with aiohttp.ClientSession() as s:
            async with s.ws_connect(self.WS_PRV) as ws:
                await loop.create_task(self.login_ws_message(loop, ws, 'orders_processing'))
                loop.create_task(self._ping(ws))
                while True:
                    for task in self.async_tasks:
                        if task[0] == 'create_order':
                            price = task[1]['price']
                            size = task[1]['size']
                            side = task[1]['side']
                            market = task[1]['market']
                            client_id = task[1].get('client_id')
                            loop.create_task(self._send_order(market, size, price, side, ws, client_id))
                        elif task[0] == 'cancel_order':
                            loop.create_task(self.cancel_order(task[1]['market'], task[1]['order_id'], ws))
                        elif task[0] == 'amend_order':
                            price = task[1]['price']
                            size = task[1]['size']
                            order_id = task[1]['order_id']
                            market = task[1]['market']
                            loop.create_task(self.amend_order(price, size, order_id, market))
                        self.async_tasks.remove(task)
                    await asyncio.sleep(0.0001)

    @try_exc_async
    async def cancel_order(self, market, order_id, ws):
        self.multibot.deleted_orders.append(order_id)
        data = {"id": self.id_generator(),
                "op": "cancel-order",
                "args": [
                    {"instId": market,
                     "ordId": order_id}]}
        await ws.send_json(data)
        response = await ws.receive()
        response = json.loads(response.data)
        if order_id in self.multibot.deleted_orders:
            self.multibot.deleted_orders.remove(order_id)
        # print(f'ORDER CANCELED {self.EXCHANGE_NAME}', response)
        if 'maker' in response['data'][0].get('clOrdId', '') and self.EXCHANGE_NAME == self.multibot.mm_exchange:
            coin = market.split('-')[0]
            if self.multibot.open_orders.get(coin + '-' + self.EXCHANGE_NAME, [''])[0] == response[0]['orderID']:
                self.multibot.open_orders.pop(coin + '-' + self.EXCHANGE_NAME)
    # error_example = {'id': 'RrkaMm', 'op': 'cancel-order', 'code': '1', 'msg': '', 'data': [
    #     {'ordId': 'makerxxxOKXxxxXoSXQmxxxETH', 'clOrdId': '', 'sCode': '51000', 'sMsg': 'Parameter ordId  error'}],
    #                  'inTime': '1706890754843769', 'outTime': '1706890754843819'}
    # success_example = {'id': 'rfkWOW', 'op': 'cancel-order', 'code': '0', 'msg': '', 'data': [
    #     {'ordId': '673684370330640402', 'clOrdId': 'makerxxxOKXxxxCgSkIRxxxETH', 'sCode': '0', 'sMsg': ''}],
    #                    'inTime': '1706890870438465', 'outTime': '1706890870439349'}

    @try_exc_async
    async def amend_order(self, price, size, order_id, market):
        pass

    @try_exc_async
    async def _send_order(self, symbol, amount, price, side, ws, client_id, expire=100):
        self.time_sent = time.time()
        contract_value = self.instruments[symbol]['contract_value']
        msg = {"id": self.id_generator(),
               "op": "order",
               "args": [{"side": side,
                         "instId": symbol,
                         "tdMode": "cross",
                         "ordType": 'limit',
                         "sz": int(amount * contract_value),
                         "px": price}]}
        if client_id:
            msg['args'][0].update({'clOrdId': client_id})
        await ws.send_json(msg)
        response = await ws.receive()
        response = json.loads(response.data)
        order_id = response['data'][0]['ordId'] if response['code'] == '0' else 'default'
        self.LAST_ORDER_ID = order_id
        if not order_id:
            self.error_info = response
        else:
            self.orders_timestamps.update({order_id: float(response['inTime']) / 1000000})
        print(f'{self.EXCHANGE_NAME} CREATE ORDER RESPONSE', response)
        print(f"{self.EXCHANGE_NAME} CREATE ORDER  TIME {float(response['inTime']) / 1000000 - self.time_sent} sec")
        # error_parameter = {"id": "587912334468", "op": "order", "code": "1", "msg": "",
        #          "data": [{"tag": "", "ordId": "", "clOrdId": "", "sCode": "51000", "sMsg": "Parameter sz  error"}],
        #          "inTime": "1706881893459102", "outTime": "1706881893459133"}
        # error_margin = {"id": "309558547378", "op": "order", "code": "1", "msg": "", "data": [
        #     {"tag": "", "ordId": "", "clOrdId": "", "sCode": "51008",
        #      "sMsg": "Order failed. Insufficient USDT margin in account"}], "inTime": "1706882063195045",
        #                 "outTime": "1706882063196212"}
        # success = {"id": "LOpfkn", "op": "order", "code": "0", "msg": "", "data": [
        #     {"tag": "", "ordId": "673649813187354653", "clOrdId": "makerxxxOKXxxxWPZhyuETH", "sCode": "0",
        #      "sMsg": "Order successfully placed."}], "inTime": "1706882631273262", "outTime": "1706882631274259"}

    @try_exc_async
    async def _login(self, ws):
        request_path = '/users/self/verify'
        timestamp = str(int(round(time.time())))
        signature = self.signature(timestamp, 'GET', request_path, None)
        msg = {"op": "login",
               "args": [{
                   "apiKey": self.public_key,
                   "passphrase": self.passphrase,
                   "timestamp": timestamp,
                   "sign": signature
               }]}
        await ws.send_json(msg)

    @try_exc_regular
    def signature(self, timestamp, method, request_path, body):
        if str(body) == '{}' or str(body) == 'None':
            body = ''
        message = str(timestamp) + str.upper(method) + request_path + str(body)
        mac = hmac.new(bytes(self.secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
        signature = mac.digest()
        return base64.b64encode(signature).decode('UTF-8')

    @try_exc_async
    async def _subscribe_orderbooks(self, ws):
        for symbol in self.markets_list:
            if market := self.markets.get(symbol):
                msg = {"op": "subscribe",
                       "args": [{
                           "channel": "bbo-tbt",  # "bbo-tbt",  # 0-l2-tbt",
                           "instId": market}]}
                await ws.send_json(msg)

    @try_exc_async
    async def _subscribe_account(self, ws):
        msg = {"op": "subscribe",
               "args": [{"channel": "account"}]}
        await ws.send_json(msg)

    @try_exc_async
    async def _subscribe_positions(self, ws):
        for symbol in self.markets_list:
            if market := self.markets.get(symbol):
                msg = {"op": "subscribe",
                       "args": [{"channel": "positions",
                                 "instType": "SWAP",
                                 "instFamily": market.split('-SWAP')[0],
                                 "instId": market}]}
                await ws.send_json(msg)

    @try_exc_async
    async def _subscribe_orders(self, ws):
        for symbol in self.markets_list:
            if market := self.markets.get(symbol):
                msg = {"op": "subscribe",
                       "args": [{"channel": "orders",
                                 "instType": "SWAP",
                                 "instFamily": market.split('-SWAP')[0],
                                 "instId": market}]}
                await ws.send_json(msg)

    @try_exc_regular
    def _run_ws(self, connection_type, loop, endpoint):
        while True:
            loop.run_until_complete(self._run_ws_loop(connection_type, loop, endpoint))

    @try_exc_async
    async def _run_ws_loop(self, connection_type, loop, endpoint):
        async with aiohttp.ClientSession() as s:
            async with s.ws_connect(endpoint) as ws:
                await loop.create_task(self.login_ws_message(loop, ws, connection_type))
                if connection_type == 'private':
                    self._ws_private = ws
                    await loop.create_task(self._subscribe_account(ws))
                    await loop.create_task(self._subscribe_positions(ws))
                    await loop.create_task(self._subscribe_orders(ws))
                else:
                    await loop.create_task(self._subscribe_orderbooks(ws))
                loop.create_task(self._ping(ws))
                async for msg in ws:
                    loop.create_task(self._process_msg(msg))

    @try_exc_async
    async def login_ws_message(self, loop, ws, connection_type):
        await loop.create_task(self._login(ws))
        print(self.EXCHANGE_NAME, connection_type, await ws.receive())

    @try_exc_async
    async def _ping(self, ws):
        while True:
            await asyncio.sleep(25)  # Adjust the ping interval as needed
            await ws.ping()

    @try_exc_regular
    def get_balance(self):
        if not self.balance.get('total'):
            self.get_real_balance()
        return self.balance['total']

    @try_exc_regular
    def get_position(self):
        self.positions = {}
        way = '/api/v5/account/positions'
        headers = self.get_private_headers('GET', way)
        resp = requests.get(url=self.BASE_URL + way, headers=headers).json()
        for pos in resp['data']:
            side = 'LONG' if float(pos['pos']) > 0 else 'SHORT'
            amount_usd = float(pos['notionalUsd'])
            if side == 'SHORT':
                amount_usd = -float(pos['notionalUsd'])
            amount = amount_usd / float(pos['markPx'])
            self.positions.update({pos['instId']: {'side': side,
                                                   'amount_usd': amount_usd,
                                                   'amount': amount,
                                                   'entry_price': float(pos['avgPx']),
                                                   'unrealized_pnl_usd': float(pos['upl']),
                                                   'realized_pnl_usd': 0,
                                                   'lever': self.leverage}})

    @try_exc_async
    async def _update_positions(self, obj):
        if not obj['data']:
            return
        for position in obj['data']:
            if not position.get('notionalUsd'):
                continue
            market = obj['arg']['instId']
            amount_usd = float(position['notionalUsd'])
            self.positions.update({market: {'side': 'LONG' if float(position['pos']) > 0 else 'SHORT',
                                            'amount_usd': amount_usd if float(position['pos']) > 0 else -amount_usd,
                                            'amount': amount_usd / float(position['markPx']),
                                            'entry_price': float(position['avgPx']),
                                            'unrealized_pnl_usd': float(position['upl']),
                                            'realized_pnl_usd': 0,
                                            'lever': self.leverage}})

    @try_exc_async
    async def _update_orderbook(self, obj):
        market = obj['arg']['instId']
        contract = self.get_contract_value(market)
        top_ask = self.orderbook.get(market, {}).get('asks', [[None, None]])[0][0]
        top_bid = self.orderbook.get(market, {}).get('bids', [[None, None]])[0][0]
        orderbook = obj['data'][0]
        self.orderbook.update({market: {'asks': [[float(x[0]), float(x[1]) / contract] for x in orderbook['asks']],
                                        'bids': [[float(x[0]), float(x[1]) / contract] for x in orderbook['bids']],
                                        'timestamp': float(orderbook['ts']) / 1000,
                                        'ts_ms': time.time()}})
        if self.market_finder:
            if top_ask != self.orderbook[market]['asks'][0][0] or top_bid != self.orderbook[market]['asks'][0][0]:
                await self.market_finder.count_one_coin(market.split('-')[0], self.EXCHANGE_NAME)

    @try_exc_async
    async def _update_account(self, obj):
        resp = obj['data']
        if len(resp):
            acc_data = resp[0]['details'][0]
            self.balance = {'free': float(acc_data['availBal']),
                            'total': float(acc_data['availBal']) + float(acc_data['frozenBal']),
                            'timestamp': round(datetime.utcnow().timestamp())}
        else:
            self.balance = {'free': 0,
                            'total': 0,
                            'timestamp': round(datetime.utcnow().timestamp())}

    @try_exc_async
    async def _update_orders(self, obj):
        if obj.get('data') and obj.get('arg'):
            for order in obj.get('data'):
                print(f"OKEX RESPONSE: {order}\n")
                status = self.get_order_status(order, 'WS')
                self.get_taker_fee(order)
                contract_value = self.get_contract_value(order['instId'])
                result = {
                    'exchange_order_id': order['ordId'],
                    'exchange_name': self.EXCHANGE_NAME,
                    'status': status,
                    'factual_price': float(order['fillPx']) if order['fillPx'] else 0,
                    'price': float(order['fillPx']) if order['fillPx'] else 0,
                    'factual_amount_coin': float(order['fillSz']) * contract_value if order['fillSz'] else 0,
                    'size': float(order['fillSz']) * contract_value if order['fillSz'] else 0,
                    'factual_amount_usd': float(order['fillNotionalUsd']) if order['fillNotionalUsd'] else 0,
                    'datetime_update': datetime.utcnow(),
                    'ts_update': int(round(datetime.utcnow().timestamp() * 1000)),
                    'api_response': order,
                    'timestamp': float(order['uTime']) / 1000,
                    'time_order_sent': self.time_sent,
                    'create_order_time': float(order['uTime']) / 1000 - self.time_sent}
                if client_id := order.get("clOrdId") and self.orders.get(order['ordId']):
                    self.responses.update({client_id: result})
                self.orders.update({order['ordId']: result})
        # example = {'accFillSz': '0', 'algoClOrdId': '', 'algoId': '', 'amendResult': '', 'amendSource': '',
        #            'attachAlgoClOrdId': '', 'attachAlgoOrds': [], 'avgPx': '0', 'cTime': '1706884013692',
        #            'cancelSource': '', 'category': 'normal', 'ccy': '', 'clOrdId': 'makerxxxOKXxxxlXicApxxxETH',
        #            'code': '0', 'execType': '', 'fee': '0', 'feeCcy': 'USDT', 'fillFee': '0', 'fillFeeCcy': '',
        #            'fillFwdPx': '', 'fillMarkPx': '', 'fillMarkVol': '', 'fillNotionalUsd': '', 'fillPnl': '0',
        #            'fillPx': '', 'fillPxUsd': '', 'fillPxVol': '', 'fillSz': '0', 'fillTime': '',
        #            'instId': 'ETH-USDT-SWAP', 'instType': 'SWAP', 'lastPx': '2288.05', 'lever': '5', 'msg': '',
        #            'notionalUsd': '217.2335816', 'ordId': '673655611477090329', 'ordType': 'limit', 'pnl': '0',
        #            'posSide': 'net', 'px': '2173.64', 'pxType': '', 'pxUsd': '', 'pxVol': '', 'quickMgnType': '',
        #            'rebate': '0', 'rebateCcy': 'USDT', 'reduceOnly': 'false', 'reqId': '', 'side': 'buy', 'slOrdPx': '',
        #            'slTriggerPx': '', 'slTriggerPxType': '', 'source': '', 'state': 'live', 'stpId': '', 'stpMode': '',
        #            'sz': '1', 'tag': '', 'tdMode': 'cross', 'tgtCcy': '', 'tpOrdPx': '', 'tpTriggerPx': '',
        #            'tpTriggerPxType': '', 'tradeId': '', 'uTime': '1706884013692'}

    @try_exc_regular
    def get_taker_fee(self, order):
        if not self.taker_fee:
            if order['fillNotionalUsd']:
                self.taker_fee = abs(float(order['fillFee'])) / float(order['fillNotionalUsd'])

    @try_exc_async
    async def _process_msg(self, msg: aiohttp.WSMessage):
        obj = json.loads(msg.data)
        if obj.get('event'):
            # print(obj)
            return
        if obj.get('arg'):
            if obj['arg']['channel'] == 'account':
                await self._update_account(obj)
            elif obj['arg']['channel'] in ['bbo-tbt', 'books50-l2-tbt', 'books5']:
                await self._update_orderbook(obj)
            elif obj['arg']['channel'] == 'positions':
                await self._update_positions(obj)
            elif obj['arg']['channel'] == 'orders':
                await self._update_orders(obj)

    @try_exc_regular
    def get_contract_value(self, symbol):
        contract_value = float(self.instruments[symbol]['contract_value'])
        return contract_value

    @try_exc_regular
    def get_instruments(self):
        resp = self.request_instruments()
        instruments = {}
        for instrument in resp:
            contract_value = float(instrument['ctVal'])
            if instrument['ctType'] == 'linear':
                contract_value = 1 / contract_value
            step_size = float(instrument['lotSz']) / contract_value
            quantity_precision = len(str(step_size).split('.')[1]) if '.' in str(step_size) else 1
            price_precision = self.get_price_precision(float(instrument['tickSz']))
            instruments.update({instrument['instId']: {'coin': instrument['ctValCcy'],
                                                       'state': instrument['state'],
                                                       'settleCcy': instrument['settleCcy'],
                                                       'tick_size': float(instrument['tickSz']),
                                                       'step_size': step_size,
                                                       'contract_value': contract_value,
                                                       'quantity_precision': quantity_precision,
                                                       'min_size': float(instrument['minSz']) / contract_value,
                                                       'price_precision': price_precision}})
        return instruments

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
    def fit_sizes(self, price, amount, symbol):
        instr = self.instruments[symbol]
        tick_size = instr['tick_size']
        quantity_precision = instr['quantity_precision']
        price_precision = instr['price_precision']
        amount = round(amount, quantity_precision)
        rounded_price = round(price / tick_size) * tick_size
        price = round(rounded_price, price_precision)
        return price, amount

    @try_exc_regular
    def get_order_response(self):
        if self.create_order_response:
            response = {
                'exchange_name': self.EXCHANGE_NAME,
                'exchange_order_id': self.LAST_ORDER_ID,
                'timestamp': int(round((datetime.utcnow().timestamp()) * 1000)),
                'status': ResponseStatus.SUCCESS
            }
            self.create_order_response = False
        else:
            response = {
                'exchange_name': self.EXCHANGE_NAME,
                'exchange_order_id': None,
                'timestamp': int(round((datetime.utcnow().timestamp()) * 1000)),
                'status': ResponseStatus.ERROR
            }
            self.error_info = "WS DOESN'T GIVE ANY DATA ABOUT ERROR"
        return response

    @staticmethod
    @try_exc_regular
    def get_timestamp():
        now = datetime.utcnow()
        t = now.isoformat("T", "milliseconds")
        return t + "Z"

    @try_exc_regular
    def request_instruments(self):
        way = f'https://www.okx.com/api/v5/public/instruments?instType=SWAP'
        resp = requests.get(url=way, headers=self.headers).json()
        return resp['data']

    @try_exc_regular
    def get_markets(self):
        markets = {}
        for market, instrument in self.instruments.items():
            if instrument['state'] == 'live':
                if instrument['settleCcy'] == 'USDT':
                    markets.update({instrument['coin']: market})
        return markets

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
    def get_private_headers(self, method, way, body=None):
        timestamp = self.get_timestamp()
        signature = self.signature(timestamp, method, way, body)
        headers = {'OK-ACCESS-KEY': self.public_key,
                   'OK-ACCESS-SIGN': signature,
                   'OK-ACCESS-TIMESTAMP': timestamp,
                   'OK-ACCESS-PASSPHRASE': self.passphrase}
        headers.update(self.headers)
        return headers

    @try_exc_regular
    def get_real_balance(self):
        way = '/api/v5/account/balance?ccy=USDT'
        headers = self.get_private_headers('GET', way)
        headers.update(self.headers)
        resp = requests.get(url=self.BASE_URL + way, headers=headers).json()
        if resp.get('code') == '0':
            self.balance = {'free': float(resp['data'][0]['details'][0]['availBal']),
                            'total': float(resp['data'][0]['details'][0]['eq']),
                            'timestamp': round(datetime.utcnow().timestamp())}
        # {'code': '0', 'data': [{'adjEq': '', 'borrowFroz': '', 'details': [
        #     {'availBal': '466.6748538968118', 'availEq': '466.6748538968118', 'borrowFroz': '',
        #      'cashBal': '500.0581872301451', 'ccy': 'USDT', 'crossLiab': '', 'disEq': '500.25821050503714',
        #      'eq': '500.0581872301451', 'eqUsd': '500.25821050503714', 'fixedBal': '0',
        #      'frozenBal': '33.38333333333333', 'interest': '', 'isoEq': '0', 'isoLiab': '', 'isoUpl': '0', 'liab': '',
        #      'maxLoan': '', 'mgnRatio': '', 'notionalLever': '0', 'ordFrozen': '33.333333333333336', 'spotInUseAmt': '',
        #      'spotIsoBal': '0', 'stgyEq': '0', 'twap': '0', 'uTime': '1698244053657', 'upl': '0', 'uplLiab': ''}],
        #                         'imr': '', 'isoEq': '0', 'mgnRatio': '', 'mmr': '', 'notionalUsd': '', 'ordFroz': '',
        #                         'totalEq': '500.25821050503714', 'uTime': '1698245152624'}], 'msg': ''}

    def set_leverage(self, symbol):
        way = '/api/v5/account/set-leverage'
        body = {"instId": symbol,
                "lever": "5",
                "mgnMode": "cross"
                }
        body_json = json.dumps(body)
        headers = self.get_private_headers('POST', way, body_json)
        resp = requests.post(url=self.BASE_URL + way, headers=headers, data=body_json).json()
        print(resp)

    @try_exc_regular
    def get_orderbook(self, symbol):
        return self.orderbook.get(symbol, {})

    @try_exc_async
    async def get_all_orders(self, symbol=None, session=None):
        way = '/api/v5/trade/orders-pending?'
        for coin in self.markets_list:
            way += self.markets[coin] + '&'
        way = way[:-1]
        headers = self.get_private_headers('GET', way)
        async with aiohttp.ClientSession() as session:
            async with session.get(url=self.BASE_URL + way, headers=headers) as resp:
                data = await resp.json()
                return self.reformat_orders(data)

    @try_exc_regular
    def _get_all_orders(self):
        way = '/api/v5/trade/orders-pending?'
        for coin in self.markets_list:
            way += self.markets[coin] + '&'
        way = way[:-1]
        headers = self.get_private_headers('GET', way)
        data = requests.get(url=self.BASE_URL + way, headers=headers).json()
        return self.reformat_orders(data)

    @try_exc_regular
    def get_order_by_id(self, symbol, order_id: str):
        way = '/api/v5/trade/order' + '?' + 'ordId=' + order_id + '&' + 'instId=' + symbol
        headers = self.get_private_headers('GET', way)
        headers.update({'instId': symbol})
        res = requests.get(url=self.BASE_URL + way, headers=headers).json()
        if res.get('data'):
            for order in res['data']:
                return {
                    'exchange_order_id': order_id,
                    'exchange': self.EXCHANGE_NAME,
                    'status': OrderStatus.FULLY_EXECUTED if order.get(
                        'state') == 'filled' else OrderStatus.NOT_EXECUTED,
                    'factual_price': float(order['avgPx']) if order['avgPx'] else 0,
                    'factual_amount_coin': float(order['fillSz']) if order['avgPx'] else 0,
                    'factual_amount_usd': float(order['fillSz']) * float(order['avgPx']) if order['avgPx'] else 0,
                    'datetime_update': datetime.utcnow(),
                    'ts_update': int(datetime.utcnow().timestamp() * 1000)
                }
        else:
            print(f"ERROR>GET ORDER BY ID RES: {res}")
            return {
                'exchange_order_id': order_id,
                'exchange': self.EXCHANGE_NAME,
                'status': OrderStatus.NOT_EXECUTED,
                'factual_price': 0,
                'factual_amount_coin': 0,
                'factual_amount_usd': 0,
                'datetime_update': datetime.utcnow(),
                'ts_update': int(datetime.utcnow().timestamp() * 1000)
            }

    @try_exc_regular
    def get_order_status(self, order, req_type):
        status = None
        if order['state'] == 'live':
            if req_type == 'HTTP':
                status = OrderStatus.PROCESSING
            else:
                self.LAST_ORDER_ID = order['ordId']
                # print(f"OKEX ORDER PLACE TIME: {float(order['uTime']) - self.time_sent} ms\n")
                status = OrderStatus.PROCESSING
        if order['state'] == 'filled':
            status = OrderStatus.FULLY_EXECUTED
        elif order['state'] == 'canceled' and order['fillSz'] != '0' and order['fillSz'] != order['sz']:
            status = OrderStatus.PARTIALLY_EXECUTED
        elif order['state'] == 'partially_filled':
            status = OrderStatus.PARTIALLY_EXECUTED
        elif order['state'] == 'canceled' and order['fillSz'] == '0':
            status = OrderStatus.NOT_EXECUTED
        return status

    @try_exc_regular
    def reformat_orders(self, response):
        orders = []
        for order in response['data']:
            symbol = order['instId']
            status = self.get_order_status(order, 'HTTP')
            real_fee = 0
            usd_size = 0
            if order['avgPx']:
                usd_size = float(order['fillSz']) * float(order['avgPx'])
                real_fee = abs(float(order['fee'])) / usd_size
            contract_value = self.get_contract_value(order['instId'])
            order.update({
                'id': uuid.uuid4(),
                'datetime': datetime.utcfromtimestamp(int(order['uTime']) / 1000),
                'ts': int(time.time()),
                'context': 'web-interface' if 'api_' not in order['clOrdId'] else order['clOrdId'].split('xxx')[0],
                'parent_id': uuid.uuid4(),
                'exchange_order_id': order['ordId'],
                'type': order['category'],
                'status': status,
                'exchange': self.EXCHANGE_NAME,
                'side': order['side'].lower(),
                'symbol': symbol,
                'expect_price': float(order['px']),
                'expect_amount_coin': float(order['sz']) / contract_value,
                'expect_amount_usd': float(order['px']) * float(order['sz']) / contract_value,
                'expect_fee': self.taker_fee,
                'factual_price': float(order['avgPx']) if order['avgPx'] else 0,
                'factual_amount_coin': float(order['fillSz']) * contract_value if order['fillSz'] else 0,
                'factual_amount_usd': usd_size,
                'factual_fee': real_fee,
                'order_place_time': 0,
                'env': '-',
                'datetime_update': datetime.utcnow(),
                'ts_update': int(datetime.utcnow().timestamp()),
                'client_id': order['clOrdId']
            })
            orders.append(order)
        return orders

    @try_exc_regular
    def cancel_all_orders(self):
        way = '/api/v5/trade/cancel-batch-orders'
        orders = self._get_all_orders()
        time.sleep(0.1)
        body = []
        for order in orders:
            body.append({'instId': order['instId'], 'ordId': order['ordId']})
        body_json = json.dumps(body)
        headers = self.get_private_headers('POST', way, body_json)
        resp = requests.post(url=self.BASE_URL + way, headers=headers, data=body_json).json()
        for order in resp['data']:
            if order['sCode'] == '0':
                print(f"ORDER {order['ordId']} SUCCESSFULLY CANCELED")
            else:
                print(f"ORDER {order['ordId']} ERROR: {order['sMsg']}")

    @try_exc_async
    async def get_orderbook_by_symbol(self, symbol):
        way = f'/api/v5/market/books?instId={symbol}&sz=10'
        async with aiohttp.ClientSession() as session:
            async with session.get(url=self.BASE_URL + way, headers=self.headers) as resp:
                data = await resp.json()
                orderbook = data['data'][0]
                contract = self.get_contract_value(symbol)
                new_asks = [[float(ask[0]), float(ask[1]) * contract] for ask in orderbook['asks']]
                new_bids = [[float(bid[0]), float(bid[1]) * contract] for bid in orderbook['bids']]
                orderbook['asks'] = new_asks
                orderbook['bids'] = new_bids
                orderbook['timestamp'] = int(orderbook['ts'])
                return orderbook

    @try_exc_async
    async def create_order(self, symbol, side, price, size, session, expire=10000, client_id=None, expiration=None):
        way = '/api/v5/trade/order'
        contract_value = self.instruments[symbol]['contract_value']
        body = {"instId": symbol,
                "tdMode": "cross",
                "side": side,
                "ordType": "limit",
                "px": price,
                "sz": int(size * contract_value)}
        print(body)
        json_body = json.dumps(body)
        headers = self.get_private_headers('POST', way, json_body)
        resp = requests.post(url=self.BASE_URL + way, headers=headers, data=json_body).json()
        print(f"OKEX RESPONSE: {resp}")
        if resp['code'] == '0':
            self.LAST_ORDER_ID = resp['data'][0]['ordId']
            exchange_order_id = resp['data'][0]['ordId']
            return {'exchange_name': self.EXCHANGE_NAME,
                    'exchange_order_id': exchange_order_id,
                    'timestamp': int(resp['inTime']),
                    'status': ResponseStatus.SUCCESS}
        else:
            self.error_info = str(resp['data'])
            return {'exchange_name': self.EXCHANGE_NAME,
                    'exchange_order_id': None,
                    'timestamp': int(round((datetime.utcnow().timestamp()) * 1000)),
                    'status': ResponseStatus.ERROR}

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


if __name__ == '__main__':
    import configparser
    import sys

    config = configparser.ConfigParser()
    config.read(sys.argv[1], "utf-8")
    client = OkxClient(keys=config['OKX'],
                       leverage=float(config['SETTINGS']['LEVERAGE']),
                       max_pos_part=int(config['SETTINGS']['PERCENT_PER_MARKET']),
                       markets_list=['ETH', 'BTC', 'LTC', 'BCH', 'SOL', 'MINA', 'XRP', 'PEPE', 'CFX', 'FIL'])

    client.run_updater()

    # time.sleep(3)
    # market = client.markets['ETH']
    # ob = client.get_orderbook(market)
    # size = 0.1
    # price = ob['bids'][0][0] * 0.95
    # price, size = client.fit_sizes(price, size, market)
    # order_data = {'market': market,
    #               'client_id': f'makerxxx{client.EXCHANGE_NAME}xxx' + client.id_generator() + 'xxx' + market.split('-')[
    #                   0],
    #               'price': price,
    #               'size': size,
    #               'side': 'buy'}
    # print(f"{order_data=}")
    # client.async_tasks.append(['create_order', order_data])
    # time.sleep(0.1)
    # print(client.responses)
    # for order_id, response in client.responses.items():
    #     cancel_data = ['cancel_order', {'order_id': response['exchange_order_id'],
    #                                     'market': market}]
    #     print(f"{cancel_data=}")
    #     client.async_tasks.append(cancel_data)

    while True:
        time.sleep(5)
        print(client.get_all_tops())
