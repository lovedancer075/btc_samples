import ccxt.pro
from asyncio import run

import numpy as np
import talib
from talib import stream
import math

print('CCXT Pro version', ccxt.pro.__version__)

# https://github.com/lovedancer075/btc_samples

async def main():
    exchange = ccxt.pro.okex({'aiohttp_proxy':'http://127.0.0.1:7896'})
    #exchange = ccxt.pro.okex()

    symbol = 'ETH/USDT'  # or BNB/USDT, etc...
    timeframe = '1m'  # 5m, 1h, 1d
    limit = 10  # how many candles to return max
    method = 'watchOHLCV'

    closes = []

    if (method in exchange.has) and exchange.has[method]:
        max_iterations = 100000  # how many times to repeat the loop before exiting
        for i in range(0, max_iterations):
            try:
                ohlcvs = await exchange.watch_ohlcv(symbol, timeframe, None, limit)
                now = exchange.milliseconds()
                print('\n===============================================================================')
                print('Loop iteration:', i, 'current time:', exchange.iso8601(now), symbol, timeframe)
                print('-------------------------------------------------------------------------------')
                for _, item in enumerate(ohlcvs):
                    print(exchange.iso8601(item[0]), item[1:])

                # 使用talib的stream API求EMA均线
                closes.append(ohlcvs[0][4])
                if len(closes) > 100:
                    closes.pop(0)
                latest = stream.EMA(np.array(closes), timeperiod=5)
                if not math.isnan(latest):
                    print("EMA value = %.2f" % latest)
                else:
                    print("EMA value = nan")


            except Exception as e:
                print(type(e).__name__, str(e))
                break
        await exchange.close()
    else:
        print(exchange.id, method, 'is not supported or not implemented yet')


run(main())
