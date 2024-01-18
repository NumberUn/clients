from clients.binance import BinanceClient


class ApolloxClient(BinanceClient):
    BASE_WS = 'wss://fstream.apollox.finance/ws/'
    BASE_URL = 'https://fapi.apollox.finance'
    EXCHANGE_NAME = 'APOLLOX'

    def __init__(self, keys, leverage, markets_list=[], max_pos_part=20):
        super().__init__(keys, leverage, markets_list=[], max_pos_part=20)


if __name__ == '__main__':
    import configparser
    import sys
    import time

    config = configparser.ConfigParser()
    config.read(sys.argv[1], "utf-8")

    client = ApolloxClient(config['APOLLOX'],float(config['SETTINGS']['LEVERAGE']),
                           int(config['TELEGRAM']['ALERT_CHAT_ID']),config['TELEGRAM']['ALERT_BOT_TOKEN'],
                           [],int(config['SETTINGS']['PERCENT_PER_MARKET']))
    client.run_updater()
    time.sleep(3)
    print(client.get_balance())
    print()
    print(client.positions)
    print()
    print(client.new_get_available_balance())

    # async def test_order():
    #     async with aiohttp.ClientSession() as session:
    #         client.fit_amount(0.017)
    #         price = client.get_orderbook()[client.symbol]['bids'][10][0]
    #         data = await client.create_order(price, 'buy', session, client_id=uuid.uuid4())
    #         # data = await client.get_orderbook_by_symbol()
    #         print(data)
    #         client.cancel_all_orders()
    #
    #
    # asyncio.run(test_order())
    #
    # while True:
    #     time.sleep(5)
    #     asyncio.run(test_order())
