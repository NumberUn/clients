from clients.apollox import ApolloxClient
from clients.binance import BinanceClient
from clients.bitmex import BitmexClient
from clients.dydx import DydxClient
from clients.kraken import KrakenClient
from clients.okx import OkxClient
from clients.hitbtc import HitbtcClient
from clients.bitmake import BitmakeClient
from clients.globederivative import GlobeClient
from clients.bit import BitClient
from clients.btse import BtseClient
from clients.whitebit import WhiteBitClient

ALL_CLIENTS = {
    # 'BITMEX': BitmexClient,
    # 'DYDX': DydxClient,
    # 'BINANCE': BinanceClient,
    # 'APOLLOX': ApolloxClient,
    'OKX': OkxClient,
    # 'KRAKEN': KrakenClient,
    # 'HITBTC': HitbtcClient,
    # 'BITMAKE': BitmakeClient,
    # 'GLOBE': GlobeClient,
    # 'BIT': BitClient,
    'BTSE': BtseClient,
    'WHITEBIT': WhiteBitClient}
