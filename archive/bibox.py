import time
import aiohttp
import json
import requests
from datetime import datetime
import asyncio
import threading
import zlib


# from core.wrappers import try_exc_regular, try_exc_async


class BiboxClient:
    PUBLIC_WS_ENDPOINT = 'wss://npush.bibox360.com/cbu'
    EXCHANGE_NAME = 'BIBOX'

    def __init__(self, keys=None, leverage=None, markets_list=[], max_pos_part=20):
        self.headers = {'Content-Type': 'application/json'}
        self.markets = self.get_markets() # coin:symbol
        self.markets_symbol_coin ={value: key for key, value in self.markets.items()} #symbol:coin
        self._loop_public = asyncio.new_event_loop()
        self._connected = asyncio.Event()
        self.wst_public = threading.Thread(target=self._run_ws_forever, args=[self._loop_public])
        self.requestLimit = 1200
        self.orderbook = {}
        self.taker_fee = 0.001

    # @try_exc_regular
    def get_markets(self):
        way = "https://api.bibox.com/api/v4/cbu/marketdata/pairs"
        resp = requests.get(url=way, headers=self.headers).json()
        markets = {}
        for market in resp:
            markets.update({market['base']: market['symbol']})
        return markets

    # @try_exc_regular
    def get_all_tops(self):
        tops = {}
        for symbol, orderbook in self.orderbook.items():
            coin = self.markets_symbol_coin[symbol]
            if len(orderbook['bids']) and len(orderbook['asks']):
                tops.update({self.EXCHANGE_NAME + '__' + coin: {
                    'top_bid': orderbook['bids'][0][0], 'top_ask': orderbook['asks'][0][0],
                    'bid_vol': orderbook['bids'][0][1], 'ask_vol': orderbook['asks'][0][1],
                    'ts_exchange': orderbook['timestamp']}})
        return tops

    # @try_exc_regular
    def _run_ws_forever(self, loop):
        while True:
            loop.run_until_complete(self._run_ws_loop())

    # @try_exc_async
    async def _run_ws_loop(self):
        async with aiohttp.ClientSession() as s:
            async with s.ws_connect(self.PUBLIC_WS_ENDPOINT) as ws:
                self._connected.set()
                self._ws_public = ws
                for market in self.markets.values():
                    self._loop_public.create_task(self.subscribe_orderbooks(market + '_depth'))
                async for msg in ws:
                    self.update_orderbook(self.decode_data(msg.data))

    @staticmethod
    def decode_data(message):
        if message[0] == '\x01' or message[0] == 1:
            message = message[1:]
            data = zlib.decompress(message, zlib.MAX_WBITS | 32)
            jmsgs = json.loads(data)
        elif message[0] == '\x00' or message[0] == 0:
            message = message[1:]
            jmsgs = json.loads(message)
        else:
            jmsgs = json.loads(message)
        return jmsgs

    # @try_exc_regular
    def get_orderbook(self, symbol):
        return self.orderbook[symbol]

    # @try_exc_async
    async def subscribe_orderbooks(self, orderbook):
        method = {"sub": orderbook}
        await self._connected.wait()
        await self._ws_public.send_json(method)

    # @try_exc_regular
    def update_orderbook(self, data):
        if not data.get('d'):
            return
        market = data['d']['pair']
        if asks := data['d'].get('asks'):
            bids = data['d']['bids']
            self.orderbook.update({market: {'asks': [[float(x[1]), float(x[0])] for x in asks][:10],
                                            'bids': [[float(x[1]), float(x[0])] for x in bids][:10],
                                            'timestamp': datetime.utcnow().timestamp()}})
        else:
            if new_asks := data['d']['add']['asks']:
                for ask in new_asks:
                    self.orderbook[market]['asks'].append([float(ask[1]), float(ask[0])])
                self.orderbook[market]['asks'] = sorted(self.orderbook[market]['asks'])[:10]
            if new_bids := data['d']['add']['bids']:
                for bid in new_bids:
                    self.orderbook[market]['bids'].append([float(bid[1]), float(bid[0])])
                self.orderbook[market]['bids'] = sorted(self.orderbook[market]['bids'], reverse=True)[:10]
            if del_asks := data['d']['del']['asks']:
                for del_ask in del_asks:
                    to_delete = [float(del_ask[1]), float(del_ask[0])]
                    if to_delete in self.orderbook[market]['asks']:
                        self.orderbook[market]['asks'].remove(to_delete)
            if del_bids := data['d']['del']['bids']:
                for del_bid in del_bids:
                    to_delete = [float(del_bid[1]), float(del_bid[0])]
                    if to_delete in self.orderbook[market]['bids']:
                        self.orderbook[market]['bids'].remove(to_delete)
            self.orderbook[market]['timestamp'] = datetime.utcnow().timestamp()

    # @try_exc_regular
    def run_updater(self):
        self.wst_public.daemon = True
        self.wst_public.start()


if __name__ == '__main__':
    client = BiboxClient()
    # print(client.get_markets())
    client.run_updater()
    time.sleep(5)
    print(json.dumps(client.orderbook,indent=2))
    print(json.dumps(client.get_markets(), indent=2))
    print(json.dumps(client.get_all_tops(),indent=2))

