import aiohttp
from abc import ABC, abstractmethod
import telebot
import configparser
import sys
# from core.wrappers import try_exc_regular, try_exc_async

config = configparser.ConfigParser()
config.read(sys.argv[1], "utf-8")


class BaseClient(ABC):
    BASE_URL = None
    BASE_WS = None
    EXCHANGE_NAME = None
    LAST_ORDER_ID = 'default'

    def __init__(self):
        self.chat_id = int(config['TELEGRAM']['CHAT_ID'])
        self.chat_token = config['TELEGRAM']['TOKEN']
        self.alert_id = int(config['TELEGRAM']['ALERT_CHAT_ID'])
        self.telegram_bot = telebot.TeleBot(self.chat_token)

    @abstractmethod
    # @try_exc_regular
    def get_available_balance(self, leverage, max_pos_part, positions: dict, balance: dict):
        available_balances = {}
        position_value_abs = 0
        available_margin = balance['total'] * leverage
        avl_margin_per_market = available_margin / 100 * max_pos_part
        for symbol, position in positions.items():
            if position.get('amount_usd'):
                # position_value += position['amount_usd']
                position_value_abs += abs(position['amount_usd'])
                available_balances.update({symbol: {'buy': avl_margin_per_market - position['amount_usd'],
                                                    'sell': avl_margin_per_market + position['amount_usd']}})
        if position_value_abs <= available_margin:
            # Это по сути доступный баланс для открытия новых позиций
            available_balances['buy'] = available_margin - position_value_abs
            available_balances['sell'] = available_margin - position_value_abs
        else:
            for symbol, position in positions.items():
                if position.get('amount_usd'):
                    if position['amount_usd'] < 0:
                        available_balances.update({symbol: {'buy': abs(position['amount_usd']), 'sell': 0}})
                    else:
                        available_balances.update({symbol: {'buy': 0, 'sell': abs(position['amount_usd'])}})
            available_balances['buy'] = 0
            available_balances['sell'] = 0
        available_balances['balance'] = balance['total']
        return available_balances

    @abstractmethod
    async def create_order(self, price: float, side: str,
                           session: aiohttp.ClientSession, expire: int = 100, client_ID: str = None) -> dict:
        """
        Create order func

        :param price: SELL/BUY price
        :param side: SELL/BUY in lowercase
        :param order_type: LIMIT or MARKET
        :param session: session from aiohttpClientSession
        :param expire: int value for exp of order
        :param client_ID:
        :return:
        """
        pass

    @abstractmethod
    def cancel_all_orders(self, orderID=None) -> dict:
        """
        cancels all orders by symbol or orderID
        :return: response from exchange api
        """
        pass

    @abstractmethod
    def get_positions(self) -> dict:
        pass

    @abstractmethod
    def get_real_balance(self) -> float:
        pass

    @abstractmethod
    def get_orderbook(self) -> dict:
        pass

    async def get_all_orders(self, symbol: str, session: aiohttp.ClientSession) -> list:
        return []