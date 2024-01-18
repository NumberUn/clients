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
        pass

