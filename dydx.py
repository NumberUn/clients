import asyncio
import uuid
from datetime import datetime
import json
import threading
import time
import requests

import aiohttp
from dydx3 import Client
from dydx3.constants import API_HOST_MAINNET
from dydx3.constants import NETWORK_ID_MAINNET
from dydx3.helpers.request_helpers import epoch_seconds_to_iso
from dydx3.helpers.request_helpers import generate_now_iso
from dydx3.helpers.request_helpers import random_client_id
from dydx3.helpers.request_helpers import remove_nones
from dydx3.starkex.order import SignableOrder
from web3 import Web3

# from core.base_client import BaseClient
# from core.enums import ResponseStatus, OrderStatus, ClientsOrderStatuses
# from core.temp.wrappers import try_exc_regular, try_exc_async

from clients.core.base_client import BaseClient
from clients.core.enums import ResponseStatus, OrderStatus, ClientsOrderStatuses
from core.wrappers import try_exc_regular, try_exc_async


class DydxClient(BaseClient):
    BASE_WS = 'wss://api.dydx.exchange/v3/ws'
    BASE_URL = 'https://api.dydx.exchange'
    EXCHANGE_NAME = 'DYDX'
    urlMarkets = "https://api.dydx.exchange/v3/markets/"
    urlOrderbooks = "https://api.dydx.exchange/v3/orderbook/"

    def __init__(self, keys, leverage, state='Bot', markets_list=[], max_pos_part=20):
        super().__init__()
        self.markets_list = markets_list
        self.max_pos_part = max_pos_part
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._connected = asyncio.Event()
        self.API_KEYS = {"secret": keys['API_SECRET'],
                         "key": keys['API_KEY'],
                         "passphrase": keys['PASSPHRASE']}
        self.headers = {'DYDX-API-KEY': self.API_KEYS['key'],
                        'DYDX-PASSPHRASE': self.API_KEYS['passphrase'],
                        'Accept': 'application/json',
                        'Content-Type': 'application/json',
                        'User-Agent': 'dydx/python'}
        self.client = Client(
            network_id=NETWORK_ID_MAINNET,
            host=API_HOST_MAINNET,
            default_ethereum_address=keys['ETH_ADDRESS'],
            web3=Web3(Web3.WebsocketProvider(f'wss://mainnet.infura.io/ws/v3/{keys["INFURA_KEY"]}')),
            eth_private_key=keys['ETH_PRIVATE_KEY'],
            stark_private_key=keys['PRIVATE_KEY'],
            stark_public_key=keys['PUBLIC_KEY'],
            stark_public_key_y_coordinate=keys['PUBLIC_KEY_Y_COORDINATE'],
            web3_provider=f'https://mainnet.infura.io/v3/{keys["INFURA_KEY"]}',
            api_key_credentials=self.API_KEYS
        )
        self.error_info = None
        self.orders = {}
        self.fills = {}
        self.positions = {}
        self.count_flag = False

        self.requestLimit = 1050  # 175 за 10 секунд https://dydxprotocol.github.io/v3-teacher/#rate-limit-api
        self.markets = {}
        self.balance = {'free': 0, 'total': 0, 'timestamp': 0}
        self.orderbook = {}
        self.amount = 0
        self.price = 0

        self.keys = keys
        self.user = self.client.private.get_user().data
        self.account = self.client.private.get_account().data
        self.instruments = self.get_instruments()
        self.leverage = leverage

        self.get_real_balance()
        # self.maker_fee = float(self.user['user']['makerFeeRate'])
        self.taker_fee = float(self.user['user']['takerFeeRate'])

        self._updates = 0
        self.offsets = {}
        self.get_position()
        self.get_markets()
        self.wst = threading.Thread(target=self._run_ws_forever, daemon=True)
        self.pings = []

    @try_exc_regular
    def get_position(self):
        # NECESSARY
        self.positions = {}
        for pos in self.client.private.get_positions().data.get('positions', []):
            if pos['status'] == 'OPEN':
                amount_usd = (float(pos['size']) * float(pos['entryPrice'])) + float(pos.get('unrealizedPnl', 0))
                self.positions.update({pos['market']: {
                    'side': pos['side'],
                    'amount_usd': amount_usd,
                    'amount': float(pos['size']),
                    'entry_price': float(pos['entryPrice']),
                    'unrealized_pnl_usd': float(pos['unrealizedPnl']),
                    'realized_pnl_usd': float(pos['realizedPnl']),
                    'lever': self.leverage
                }})

    @try_exc_regular
    def cancel_all_orders(self, order_id=None):
        # NECESSARY
        for coin in self.markets_list:
            market = self.markets[coin]
            self.client.private.cancel_active_orders(market=market)

    @try_exc_regular
    def get_real_balance(self):
        # NECESSARY
        res = self.client.private.get_account().data
        self.balance = {'free': float(res['account']['freeCollateral']),
                        'total': float(res['account']['equity']),
                        'timestamp': round(datetime.utcnow().timestamp())}
        self.position_id = self.account['account']['positionId']

    @try_exc_regular
    def get_markets(self):
        # NECESSARY
        for market, value in self.instruments.items():
            if value['quoteAsset'] == 'USD' and value['status'] == 'ONLINE':
                coin = value['baseAsset']
                self.markets.update({coin: market})
        return self.markets

    @try_exc_async
    async def get_multi_orderbook(self, symbol):
        async with aiohttp.ClientSession() as session:
            async with session.get(self.urlOrderbooks + symbol) as response:
                ob = await response.json()
                try:
                    return {'top_bid': ob['bids'][0]['price'], 'top_ask': ob['asks'][0]['price'],
                            'bid_vol': ob['bids'][0]['size'], 'ask_vol': ob['asks'][0]['size'],
                            'ts_exchange': 0}
                except Exception as error:
                    print(f"Error from DyDx Module, symbol: {symbol}, error: {error}")

    @try_exc_regular
    def get_all_tops(self):
        # NECESSARY
        tops = {}
        for symbol, orderbook in self.orderbook.items():
            coin = symbol.upper().split('-')[0]
            if len(orderbook['bids']) and len(orderbook['asks']):
                tops.update({self.EXCHANGE_NAME + '__' + coin:
                                 {'top_bid': orderbook['bids'][0][0], 'top_ask': orderbook['asks'][0][0],
                                  'bid_vol': orderbook['bids'][0][1], 'ask_vol': orderbook['asks'][0][1],
                                  'ts_exchange': orderbook['timestamp']}})

        return tops

    @try_exc_regular
    def get_headers(self, path: str, method: str.upper, data):
        now_iso_string = generate_now_iso()
        signature = self.client.private.sign(
            request_path=path,
            method=method,
            iso_timestamp=now_iso_string,
            data=remove_nones(data)
        )
        headers = self.headers
        headers.update({'DYDX-SIGNATURE': signature,
                        'DYDX-TIMESTAMP': now_iso_string})
        return headers

    @try_exc_async
    async def get_funding_payments(self, session: aiohttp.ClientSession):
        path = f'/v3/funding?limit=100'
        headers = self.get_headers(path, 'GET', data={})
        async with session.get(url=self.BASE_URL + path, headers=headers) as resp:
            res = await resp.json()
            fundings = res['fundingPayments']
            for fund in fundings:
                datetime_obj = datetime.fromisoformat(fund['effectiveAt'][:-1])
                timestamp_ms = int(datetime_obj.timestamp() * 1000)
                fund.update({'time': timestamp_ms,
                             'datetime': datetime.strptime(fund['effectiveAt'], '%Y-%m-%dT%H:%M:%S.%fZ'),
                             'asset': 'USDC',
                             'tranId': 'hasNoTranId'})
            return fundings

    @try_exc_regular
    def get_http_fills(self):
        path = f'/v3/fills'
        headers = self.get_headers(path, 'GET', {})
        resp = requests.get(url=self.BASE_URL + path, headers=headers, data=json.dumps(remove_nones({}))).json()
        # print('GET_HTTP_FILLS RESPONSE', resp)
        return resp

    # example = {'fills': [
    #     {'id': 'cca2733c-e700-5f17-b038-38b26d3c8cfe', 'side': 'SELL', 'liquidity': 'TAKER', 'type': 'MARKET',
    #      'market': 'LINK-USD', 'price': '15.396', 'size': '0.4', 'fee': '0.003079',
    #      'createdAt': '2023-12-27T11:36:27.164Z',
    #      'orderId': '5a47c13f9e7e3ee56a7b105b1815588cf907d68d3a419da4530dbb25160d7cd'},
    #     {'id': '02e349f8-48e4-5438-b411-31b89bdf3c4f', 'side': 'BUY', 'liquidity': 'TAKER', 'type': 'LIMIT',
    #      'market': 'SNX-USD', 'price': '3.698', 'size': '1.9', 'fee': '0.003513',
    #      'createdAt': '2023-12-19T12:03:00.250Z',
    #      'orderId': '28ecd6695ad3aac64b5d0c031a2ab1bbdb5be52be69e3f245b93d0ece684287'}]}

    @try_exc_regular
    def get_http_order(self, order_id):
        path = f'/v3/orders/{order_id}'
        headers = self.get_headers(path, 'GET', {})
        resp = requests.get(url=self.BASE_URL + path, headers=headers, data=json.dumps(remove_nones({})))
        return resp.json()

    @try_exc_regular
    def get_order_by_id(self, symbol, order_id: str):
        fills = self.get_http_fills()
        av_price = 0
        real_size_coin = 0
        real_size_usd = 0
        for fill in fills['fills']:
            if fill['orderId'] == order_id:
                if av_price:
                    real_size_usd = av_price * real_size_coin + float(fill['size']) * float(fill['price'])
                    av_price = real_size_usd / (real_size_coin + float(fill['size']))
                else:
                    real_size_usd = float(fill['size']) * float(fill['price'])
                    av_price = float(fill['price'])
                real_size_coin += float(fill['size'])
        order = self.get_http_order(order_id)
        return {'exchange_order_id': order_id,
                'exchange': self.EXCHANGE_NAME,
                'status': OrderStatus.FULLY_EXECUTED if order.get(
                    'status') == ClientsOrderStatuses.FILLED else OrderStatus.NOT_EXECUTED,
                'factual_price': av_price,
                'factual_amount_coin': real_size_coin,
                'factual_amount_usd': real_size_usd,
                'datetime_update': datetime.utcnow(),
                'ts_update': int(datetime.utcnow().timestamp() * 1000)}

    @try_exc_regular
    def get_order_status(self, order):
        if order.get('status') == ClientsOrderStatuses.FILLED and float(order['origQty']) > float(
                order['executedQty']):
            status = OrderStatus.PARTIALLY_EXECUTED
        elif order.get('status') == ClientsOrderStatuses.FILLED:
            status = OrderStatus.FULLY_EXECUTED
        else:
            status = OrderStatus.NOT_EXECUTED
        return status

    @try_exc_async
    async def get_all_orders(self, symbol: str, session: aiohttp.ClientSession) -> list:
        # NECESSARY
        path = f'/v3/orders?market={symbol}'
        data = {}
        headers = self.get_headers(path, 'GET', data)
        async with session.get(url=self.BASE_URL + path, headers=headers, data=json.dumps(remove_nones(data))) as resp:
            res = await resp.json()
            orders = []
            for order in res['orders']:
                status = self.get_order_status(order)
                ts = order['createdAt']
                timestamp = int(datetime.timestamp(datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S.%fZ')) * 1000)
                orders.append(
                    {'id': uuid.uuid4(),
                     'datetime': timestamp,
                     'ts': int(time.time()),
                     'context': 'web-interface' if 'api_' not in order['clientId'] else order['clientId'].split('_')[1],
                     'parent_id': uuid.uuid4(),
                     'exchange_order_id': order['id'],
                     'type': order['timeInForce'],
                     'status': status,
                     'exchange': self.EXCHANGE_NAME,
                     'side': order['side'].lower(),
                     'symbol': symbol,
                     'expect_price': float(order['price']),
                     'expect_amount_coin': float(order['size']),
                     'expect_amount_usd': float(order['price']) * float(order['size']),
                     'expect_fee': self.taker_fee,
                     'factual_price': float(order['price']),
                     'factual_amount_coin': float(order['size']),
                     'factual_amount_usd': float(order['size']) * float(order['price']),
                     'factual_fee': self.taker_fee,
                     'order_place_time': 0,
                     'env': '-',
                     'datetime_update': datetime.utcnow(),
                     'ts_update': int(time.time()),
                     'client_id': order['clientId']})
            return orders

    @try_exc_regular
    def get_instruments(self):
        raw_data = self.client.public.get_markets().data
        instruments = {}
        for symbol, instrument in raw_data['markets'].items():
            tick_size = float(instrument['tickSize'])
            step_size = float(instrument['stepSize'])
            quantity_precision = len(str(step_size).split('.')[1]) if '.' in str(step_size) else 1
            price_precision = self.get_price_precision(tick_size)
            instruments.update({symbol: {'tick_size': tick_size,
                                         'step_size': step_size,
                                         'quantity_precision': quantity_precision,
                                         'price_precision': price_precision,
                                         'quoteAsset': instrument['quoteAsset'],
                                         'status': instrument['status'],
                                         'min_size': float(instrument['minOrderSize']),
                                         'baseAsset': instrument['baseAsset']}})
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
    def get_order_to_sign(self, expire_date, symbol, side, client_id):
        order_to_sign = SignableOrder(network_id=NETWORK_ID_MAINNET,
                                      position_id=self.position_id,
                                      client_id=client_id,
                                      market=symbol,
                                      side=side.upper(),
                                      human_size=str(self.amount),
                                      human_price=str(self.price),
                                      limit_fee='0.0008',
                                      expiration_epoch_seconds=expire_date)
        return order_to_sign

    @try_exc_regular
    def get_order_data(self, symbol, side, expiration, client_id, order_to_sign):
        data = {'market': symbol,
                'side': side.upper(),
                'type': 'LIMIT',
                'timeInForce': 'GTT',
                'size': str(self.amount),
                'price': str(self.price),
                'limitFee': '0.0008',
                'expiration': expiration,
                'postOnly': False,
                'clientId': client_id,
                'signature': order_to_sign.sign(self.keys['PRIVATE_KEY']),
                'cancelId': None,
                'triggerPrice': None,
                'trailingPercent': None,
                'reduceOnly': None}
        return data

    @try_exc_async
    async def create_order(self, symbol, side, session, expire=10000, client_id=None, expiration=None) -> dict:
        # NECESSARY
        path = '/v3/orders'
        expire_date = int(round(time.time()) + expire)
        time_sent = datetime.utcnow().timestamp()
        expiration = expiration or epoch_seconds_to_iso(expire_date)
        client_id = client_id if client_id else random_client_id()
        order_to_sign = self.get_order_to_sign(expire_date, symbol, side, client_id)
        data = self.get_order_data(symbol, side, expiration, client_id, order_to_sign)
        print(f'DYDX BODY: {data}')
        headers = self.get_headers(path, 'POST', data)
        json_data = json.dumps(remove_nones(data))
        async with session.post(url=self.BASE_URL + path, headers=headers, data=json_data) as resp:
            res = await resp.json()
            self.LAST_ORDER_ID = res.get('order', {'id': 'default'})['id']
            exchange_order_id = res.get('order', {'id': 'default'})['id']
            status, timestamp = self.get_order_response_status(res)
            self.ping_processing(timestamp, time_sent)
            return {'exchange_name': self.EXCHANGE_NAME,
                    'exchange_order_id': exchange_order_id,
                    'timestamp': timestamp,
                    'status': status}

    @try_exc_regular
    def ping_processing(self, timestamp, time_sent):
        ping = int(round(timestamp - (time_sent * 1000), 0))
        self.pings.append(ping)
        with open(f'{self.EXCHANGE_NAME}_pings.txt', 'a') as file:
            file.write(str(datetime.utcnow()) + ' ' + str(ping) + '\n')
        avr = int(round((sum(self.pings) / len(self.pings)), 0))
        print(f"{self.EXCHANGE_NAME}: ping {ping}|avr: {avr}|max: {max(self.pings)}|min: {min(self.pings)}")

    @try_exc_regular
    def get_order_response_status(self, response):
        timestamp = 0000000000000
        if response.get('errors'):
            status = ResponseStatus.ERROR
            self.error_info = response
        elif response.get('order') and response['order'].get('status'):
            ts = response['order']['createdAt']
            timestamp = int(datetime.timestamp(datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S.%fZ')) * 1000)
            status = ResponseStatus.SUCCESS
        else:
            status = ResponseStatus.NO_CONNECTION
        return status, timestamp

    # def get_funding_history(self):
    #     return self.client.public.get_historical_funding(market='ETH').data

    # def get_funding_payments(self):
    #     return self.client.private.get_funding_payments(market=self.symbol, limit=300).data
    @try_exc_regular
    def run_updater(self):
        # NECESSARY
        self.wst.start()
        # except Exception as e:
        #     print(f"Error line 33: {e}")

    @try_exc_regular
    def _run_ws_forever(self):
        while True:
            self._loop.run_until_complete(self._run_ws_loop())
            time.sleep(60)

    @try_exc_async
    async def _run_ws_loop(self):
        async with aiohttp.ClientSession() as s:
            async with s.ws_connect(self.BASE_WS) as ws:
                print("DyDx: connected")
                self._connected.set()
                self._ws = ws
                await self._loop.create_task(self._subscribe_orderbooks())
                await self._loop.create_task(self._subscribe_account())
                async for msg in ws:
                    self._process_msg(msg)
            self._connected.clear()

    @try_exc_async
    async def _subscribe_account(self):
        now_iso_string = generate_now_iso()
        signature = self.client.private.sign(
            request_path='/ws/accounts',
            method='GET',
            iso_timestamp=now_iso_string,
            data={},
        )
        msg = {
            'type': 'subscribe',
            'channel': 'v3_accounts',
            'accountNumber': '0',
            'apiKey': self.keys['API_KEY'],
            'passphrase': self.keys['PASSPHRASE'],
            'timestamp': now_iso_string,
            'signature': signature,
        }
        await self._connected.wait()
        await self._ws.send_json(msg)

    @try_exc_async
    async def _subscribe_orderbooks(self):
        # self.markets_list = list(self.markets.keys())[:10]
        for symbol in self.markets_list:
            if market := self.markets.get(symbol):
                msg = {'type': 'subscribe',
                       'channel': 'v3_orderbook',
                       'id': market,
                       'includeOffsets': True}
                await self._connected.wait()
                await self._ws.send_json(msg)

    @try_exc_regular
    def _first_orderbook_update(self, ob: dict):
        symbol = ob['id']
        self.orderbook.update({symbol: {'asks': [], 'bids': [], 'timestamp': None}})
        ob = ob['contents']
        for ask in ob['asks']:
            if float(ask['size']) > 0:
                self.orderbook[symbol]['asks'].append([float(ask['price']), float(ask['size']), int(ask['offset'])])
            self.offsets[ask['price']] = int(ask['offset'])
        for bid in ob['bids']:
            if float(bid['size']) > 0:
                self.orderbook[symbol]['bids'].append([float(bid['price']), float(bid['size']), int(bid['offset'])])
            self.offsets[bid['price']] = int(bid['offset'])
        self.orderbook[symbol]['asks'] = sorted(self.orderbook[symbol]['asks'])
        self.orderbook[symbol]['bids'] = sorted(self.orderbook[symbol]['bids'])[::-1]
        self.orderbook[symbol].update({'timestamp': int(time.time() * 1000)})
        self.count_flag = True

    @try_exc_regular
    def _append_new_order(self, ob, side):
        symbol = ob['id']
        ob = ob['contents']
        offset = int(ob['offset'])
        for new_order in ob[side]:
            if self.offsets.get(new_order[0]):
                if self.offsets[new_order[0]] > offset:
                    continue
            self.offsets[new_order[0]] = offset
            new_order = [float(new_order[0]), float(new_order[1]), offset]
            index = 0
            for order in self.orderbook[symbol][side]:
                if new_order[0] == order[0]:
                    if new_order[1] != 0.0:
                        order[1] = new_order[1]
                        order[2] = offset
                        break
                    else:
                        self.orderbook[symbol][side].remove(order)
                        break
                if side == 'bids':
                    if new_order[0] > order[0]:
                        self.orderbook[symbol][side].insert(index, new_order)
                        break
                elif side == 'asks':
                    if new_order[0] < order[0]:
                        self.orderbook[symbol][side].insert(index, new_order)
                        break
                index += 1
            if index == 0:
                self._check_for_error(symbol)
        self.orderbook[symbol]['timestamp'] = int(time.time())

    @try_exc_regular
    def _channel_orderbook_update(self, ob: dict):
        symbol = ob['id']
        if len(ob['contents']['bids']):
            self._append_new_order(ob, 'bids')
        if len(ob['contents']['asks']):
            self._append_new_order(ob, 'asks')
        self.orderbook[symbol]['timestamp'] = int(datetime.utcnow().timestamp() * 1000)

    @try_exc_regular
    def _check_for_error(self, symbol):
        orderbook = self.orderbook[symbol]
        top_ask = orderbook['asks'][0]
        top_bid = orderbook['bids'][0]
        if top_ask[0] < top_bid[0]:
            if top_ask[2] <= top_bid[2]:
                self.orderbook[symbol]['asks'].remove(top_ask)
            else:
                self.orderbook[symbol]['bids'].remove(top_bid)

    @try_exc_regular
    def _append_format_pos(self, position):
        amount_usd = (float(position['size']) * float(position['entryPrice'])) + float(position.get('unrealizedPnl', 0))
        position.update({'timestamp': int(time.time()),
                         'entry_price': float(position['entryPrice']),
                         'amount': float(position['size']),
                         'amount_usd': amount_usd})
        return position

    @try_exc_regular
    def _update_positions(self, positions):
        for position in positions:
            if position['market'].split('-')[0] in self.markets_list:
                position = self._append_format_pos(position)
                self.positions.update({position['market']: position})
            # position_example = [{'id': '312711e6-d172-5e5b-9dc8-362101e94756',
            # 'accountId': 'f47ae945-06ae-5c47-aaad-450c0ffc6164', 'market': 'SNX-USD',
            # 'side': 'LONG/SHORT',
            # 'status': 'OPEN', 'size': '13129.1', 'maxSize': '25107', 'entryPrice': '2.363965',
            # 'exitPrice': '2.398164', 'openTransactionId': '110960769',
            # 'closeTransactionId': None, 'lastTransactionId': '114164888', 'closedAt': None,
            # 'updatedAt': '2022-10-11T00:50:34.217Z', 'createdAt': '2022-10-11T00:50:34.217Z',
            # 'sumOpen': '219717.4', 'sumClose': '206588.3', 'netFunding': '706.266653',
            # 'realizedPnl': '7771.372704'}]

    # def get_pnl(self):
    #     try:
    #         position = self.get_positions()
    #     except:
    #         return 0
    #     realized_pnl = float(position['realizedPnl'])
    #     entry_price = float(position['entryPrice'])
    #     size = float(position['size'])
    #     ob = self.get_orderbook()
    #     index_price = (ob['asks'][0][0] + ob['bids'][0][0]) / 2
    #     unrealized_pnl = size * (index_price - entry_price)
    #     return unrealized_pnl + realized_pnl

    @try_exc_regular
    def get_positions(self):
        # NECESSARY
        return self.positions

    @try_exc_regular
    def _update_orders(self, orders):
        # print('WS resp:\n', orders, '\n\n')
        for order in orders:
            # print(f'DYDX websocket update {order=}')
            status = None
            if order['status'] == ClientsOrderStatuses.PENDING:
                if self.orders.get(order['id']):
                    continue
                else:
                    status = OrderStatus.PROCESSING
            if order['status'] == ClientsOrderStatuses.FILLED:
                status = OrderStatus.FULLY_EXECUTED
            elif order['status'] == ClientsOrderStatuses.CANCELED and order['remainingSize'] != order['size']:
                status = OrderStatus.PARTIALLY_EXECUTED
            elif order['status'] == ClientsOrderStatuses.CANCELED and order['remainingSize'] == order['size']:
                status = OrderStatus.NOT_EXECUTED

            if status:
                if status in [OrderStatus.PROCESSING, OrderStatus.NOT_EXECUTED]:
                    real_price = 0
                    executed_size = 0
                    executed_size_usd = 0
                else:
                    real_price = self.get_last_price(order['market'], order['side'])
                    executed_size = float(order['size']) - float(order['remainingSize'])
                    executed_size_usd = executed_size * real_price
                result = {
                    'exchange_order_id': order['id'],
                    'exchange': self.EXCHANGE_NAME,
                    'status': status,
                    'factual_price': real_price,
                    'factual_amount_coin': executed_size,
                    'factual_amount_usd': executed_size_usd,
                    'datetime_update': datetime.utcnow(),
                    'ts_update': int(time.time() * 1000)
                }
                self.orders.update({order['id']: result})
            # order_example = [{'id': '28c21ee875838a5e349cf96d678d8c6151a250f979d6a025b3f79dcca703558',
            # 'clientId': '7049071120643888', 'market': 'SNX-USD',
            # 'accountId': 'f47ae945-06ae-5c47-aaad-450c0ffc6164', 'side': 'SELL', 'size': '483.3',
            # 'remainingSize': '0', 'limitFee': '0.0008', 'price': '2.47', 'triggerPrice': None,
            # 'trailingPercent': None, 'type': 'LIMIT', 'status': 'FILLED/OPEN/PENDING/CANCELED',
            # 'signature': '',
            # 'timeInForce': 'GTT', 'postOnly': False, 'cancelReason': None,
            # 'expiresAt': '2022-11-04T13:11:20.000Z', 'unfillableAt': '2022-11-03T13:18:00.185Z',
            # 'updatedAt': '2022-11-03T13:18:00.185Z', 'createdAt': '2022-11-03T13:18:00.148Z',
            # 'reduceOnly': False, 'country': 'JP', 'client': None, 'reduceOnlySize': None}]

    @staticmethod
    @try_exc_regular
    def timestamp_from_date(date: str):
        # date = '2023-02-15T02:55:27.640Z'
        ms = int(date.split(".")[1].split('Z')[0]) / 1000
        return time.mktime(datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ").timetuple()) + ms

    @try_exc_regular
    def get_orders(self):
        # NECESSARY
        return self.orders

    @staticmethod
    @try_exc_regular
    def __update_fill(accumulated_fills, fill):
        old_size = accumulated_fills[fill['market']]['size']
        old_price = accumulated_fills[fill['market']]['price']
        old_fee = accumulated_fills[fill['market']]['fee']

        new_size = old_size + float(fill['size'])
        new_price = (old_price * old_size + float(fill['price']) * float(fill['size'])) / (new_size)
        new_fee = old_fee + float(fill['fee'])

        accumulated_fills[fill['market']]['size'] = new_size
        accumulated_fills[fill['market']]['price'] = new_price
        accumulated_fills[fill['market']]['fee'] = new_fee
        return accumulated_fills

    @try_exc_regular
    def _update_fills(self, fills):
        accumulated_fills = {}
        for fill in fills:
            if not accumulated_fills.get(fill['market']):
                for key in ['fee', 'price', 'size']:
                    fill[key] = float(fill[key])
                accumulated_fills.update({fill['market']: fill})
            else:
                accumulated_fills = self.__update_fill(accumulated_fills, fill)
        for market, fill in accumulated_fills.items():
            if self.fills.get(market):
                self.fills[market].insert(0, fill)
            else:
                self.fills.update({market: [fill]})

    @try_exc_regular
    def get_fills(self):
        return self.fills

    @try_exc_regular
    def get_balance(self):
        # NECESSARY
        if round(datetime.utcnow().timestamp()) - self.balance['timestamp'] > 60:
            self.get_real_balance()
        return self.balance['total']

    @try_exc_regular
    def _update_account(self, account):
        self.balance = {'free': float(account['freeCollateral']),
                        'total': float(account['equity']),
                        'timestamp': round(datetime.utcnow().timestamp())}
        for market, position in account['openPositions'].items():
            if market.split('-')[0] in self.markets_list:
                position = self._append_format_pos(position)
                self.positions[market] = position

    @try_exc_regular
    def get_last_price(self, market, side):
        side = side.upper()
        last_trade = self.get_fills()
        last_price = 0
        if last_trade.get(market):
            last_trade = last_trade[market][0]
            if last_trade['side'] == side:
                last_price = last_trade['price']
        return last_price

    @try_exc_regular
    def get_available_balance(self):
        return super().get_available_balance(
            leverage=self.leverage,
            max_pos_part=self.max_pos_part,
            positions=self.positions,
            balance=self.balance)

    @try_exc_regular
    def _process_msg(self, msg: aiohttp.WSMessage):
        if msg.type == aiohttp.WSMsgType.TEXT:
            obj = json.loads(msg.data)
            if obj.get('channel'):
                if obj['channel'] == 'v3_orderbook':
                    self._updates += 1
                    if obj['type'] == 'subscribed':
                        self._first_orderbook_update(obj)
                        self.count_flag = True
                    elif obj['type'] == 'channel_data':
                        self._channel_orderbook_update(obj)
                elif obj['channel'] == 'v3_accounts':
                    if obj['contents'].get('positions'):
                        if len(obj['contents']['positions']):
                            self._update_positions(obj['contents']['positions'])
                    if obj['contents'].get('fills'):
                        if len(obj['contents']['fills']):
                            self._update_fills(obj['contents']['fills'])
                    if obj['contents'].get('orders'):
                        if len(obj['contents']['orders']):
                            self._update_orders(obj['contents']['orders'])
                    if obj['contents'].get('account'):
                        if len(obj['contents']['account']):
                            self._update_account(obj['contents']['account'])

    @try_exc_async
    async def get_orderbook_by_symbol(self, symbol):
        # NECESSARY
        async with aiohttp.ClientSession() as session:
            path = f'/v3/orderbook/{symbol}'
            headers = self.get_headers(path, 'GET', data={})
            async with session.get(url=self.BASE_URL + path, headers=headers) as resp:
                res = await resp.json()
                if 'asks' in res and 'bids' in res:
                    orderbook = {
                        'asks': [[float(x['price']), float(x['size'])] for x in res['asks']],
                        'bids': [[float(x['price']), float(x['size'])] for x in res['bids']],
                        'timestamp': int(time.time() * 1000)
                    }
                return orderbook

    @try_exc_regular
    def get_orderbook(self, symbol):
        # NECESSARY
        return self.orderbook[symbol]


if __name__ == '__main__':
    import configparser

    config = configparser.ConfigParser()
    config.read('config.ini', "utf-8")
    client = DydxClient(keys=config['DYDX'],
                        leverage=float(config['SETTINGS']['LEVERAGE']),
                        max_pos_part=int(config['SETTINGS']['PERCENT_PER_MARKET']),
                        markets_list=['RUNE', 'ETH', 'SNX', 'LINK', 'ENJ', 'XLM'])
    client.run_updater()
    client.get_http_fills()
    time.sleep(1)

    # async def test_order():
    #     async with aiohttp.ClientSession() as session:
    #         ob = client.get_orderbook('SNX-USD')
    #         price = ob['asks'][5][0]
    #         # # client.get_markets()
    #         # client.fit_sizes(0.012, price, 'ETH-USD')
    #         client.amount = 1.9
    #         client.price = price
    #         data = await client.create_order('SNX-USD',
    #                                          'buy',
    #                                          session=session,
    #                                          client_id=f"api_deal_{str(uuid.uuid4()).replace('-', '')[:20]}")
    #         # orders = await client.get_all_orders('SNX-USD', session)
    #         time.sleep(1)
    #         print(data)
    #         order = client.get_order_by_id(data['exchange_order_id'])
    #         print(order)
    #         # client.cancel_all_orders()
    #
    #
    # asyncio.run(test_order())
    client.get_order_by_id('symbol', 'exchange_order_id')

    while True:
        time.sleep(5)
    #     print(client.get_all_tops())
    #     print(client.get_balance())
