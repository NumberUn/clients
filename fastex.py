import time
import json
import traceback
import requests
import hmac
import hashlib
import aiohttp
import asyncio
import threading
from core.wrappers import try_exc_regular, try_exc_async
from datetime import datetime
import uvloop
import gc
import socket
import aiodns
from aiohttp.resolver import AsyncResolver
from clients.core.enums import ResponseStatus, OrderStatus
import string
import random

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class FastexClient:
    BASE_URL = 'http://exchange.fastex.com'
    EXCHANGE_NAME = 'FASTEX'
    headers = {"Accept": "application/json",
               "Content-Type": "application/json",
               'Connection': 'keep-alive'}

    def __init__(self):
        super().__init__()
        self.nonces = []
        self.session = requests.session()
        self.session.headers.update(self.headers)
        self.instruments = {}
        self.markets = {}
        self.orderbook_loop = asyncio.new_event_loop()
        self.market_id_list = {}
        self.orderbook = {}
        self.positions = {}
        self.taker_fee = 0.0015  # * 0.65
        self.maker_fee = 0.0014  # * 0.65
        self.counter = 0
        self.time_start = None

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

    def get_markets(self):
        path = '/api/v1/pair/list'
        response = self.session.get(url=self.BASE_URL + path)
        resp = response.json()
        if not resp['errors']:
            markets = {}
            instruments = {}
            for market in resp['response']['entities']:
                if market['currency_id_to'] == 3:
                    coin = market['symbol'].split('-')[0]
                    markets.update({coin: market['symbol']})
                    tick_size = float(market['filters']['price']['step'])
                    step_size_str = market['filters']['amount']['step'].split('1')[0] + '1'
                    quantity_precision = len(step_size_str.split('.')[1]) if '.' in step_size_str else 1
                    instruments.update({market['symbol']: {'coin': coin,
                                                           'tick_size': tick_size,
                                                           'step_size': float(market['filters']['amount']['step']),
                                                           'quantity_precision': quantity_precision,
                                                           'min_size': float(market['filters']['amount']['min']),
                                                           'price_precision': self.get_price_precision(tick_size),
                                                           'instrument_id': market['pair_id']}})
            self.instruments = instruments
            self.markets = markets
        else:
            print(resp)
        # print(resp)

    @try_exc_async
    async def get_orderbook_by_symbol(self, symbol):
        async with aiohttp.ClientSession() as session:
            path = "/api/v1/orderbook/ticker"
            params = {'symbol': symbol, 'pair_id': self.instruments[symbol]['instrument_id']}
            post_string = '?' + "&".join([f"{key}={params[key]}" for key in sorted(params)])
            async with session.get(url=self.BASE_URL + path + post_string, headers=self.headers, data=params) as resp:
                try:
                    ob = await resp.json()
                except:
                    print(f"\nTIMELAPSE: {time.time() - self.time_start}\nREQUESTS: {self.counter}\n\n\n")
                    quit()
                if not ob['errors']:
                    ask = ob['response']['entities'][0]['ask']
                    bid = ob['response']['entities'][0]['bid']
                    orderbook = {
                        'timestamp': time.time(),
                        'asks': [[float(ask['price']), float(ask['amount'])]],
                        'bids': [[float(bid['price']), float(bid['amount'])]]
                    }
                    self.orderbook[symbol] = orderbook
                    if not self.time_start:
                        self.time_start = time.time()
                    self.counter += 1
                    print(symbol, orderbook)
                    return orderbook
                else:
                    print(ob)
            # ticker_resp = {'errors': False, 'response': {'entities': [
            #     {'pair_id': 17, 'pair_name': 'BTC/USDT', 'symbol': 'BTC-USDT',
            #      'ask': {'price': '69545.3', 'amount': '12.2716'},
            #      'bid': {'price': '69545.2', 'amount': '8.4347'}}]}}
            # info_resp = {'errors': False, 'response': {'entities': {
            #     'asks': [{'price': '0.12040000', 'amount': '312914.00000000'},
            #              {'price': '0.12046000', 'amount': '71448.30000000'},
            #              {'price': '0.12052000', 'amount': '20617.60000000'}],
            #     'bids': [{'price': '0.12030000', 'amount': '488583.10000000'},
            #              {'price': '0.12024000', 'amount': '106735.30000000'}]}}}

    @try_exc_regular
    def run_orderbook_loop(self):
        while True:
            self.orderbook_loop.run_until_complete(self.fetch_orderbooks(self.orderbook_loop))

    @try_exc_async
    async def fetch_orderbooks(self, loop):
        while True:
            for market in self.markets.values():
                loop.create_task(self.get_orderbook_by_symbol(market))
                await asyncio.sleep(0.3)

    @try_exc_regular
    def get_orderbook(self, symbol) -> dict:
        ob = self.orderbook.get(symbol, {})
        return ob


if __name__ == '__main__':
    # import configparser

    # config = configparser.ConfigParser()
    # config.read('config.ini', "utf-8")
    client = FastexClient()
    # client.markets_list = list(client.markets.values())
    client.get_markets()
    print(client.instruments)
    print(client.markets)
    client.time_start = time.time()
    client.run_orderbook_loop()
    # client.run_updater()
