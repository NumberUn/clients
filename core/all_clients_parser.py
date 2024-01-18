from clients.hitbtc import HitbtcClient
from clients.dydx import DydxClient
from clients.bitmake import BitmakeClient
from clients.globederivative import GlobeClient
from clients.bit import BitClient
from clients.btse import BtseClient
from clients.whitebit import WhiteBitClient
from clients.tapbit import TapbitClient
from clients.orangex import OrangexClient

ALL_CLIENTS = {
    'HITBTC': HitbtcClient,
    'BITMAKE': BitmakeClient,
    'GLOBE': GlobeClient,
    'BIT': BitClient,
    'BTSE': BtseClient,
    'WHITEBIT': WhiteBitClient,
    'TAPBIT': TapbitClient,
    'ORANGEX': OrangexClient,
    'DYDX': DydxClient
}
