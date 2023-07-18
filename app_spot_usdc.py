#!/usr/bin/env python

"""
    A simple demo for how to:
    - Create a connection to the websocket api
    - Create a connection to the websocket stream
    - Subscribe to the user data stream from websocket stream
    - Create a new order from websocket api
"""

import logging
import time
from binance.lib.utils import config_logging
from binance.websocket.spot.websocket_api import SpotWebsocketAPIClient
from binance.websocket.spot.websocket_stream import SpotWebsocketStreamClient
from binance.spot import Spot as SpotAPIClient

import datetime
from apscheduler.schedulers.background import BackgroundScheduler

import orjson as json
from timeit import default_timer as timer

# import pdb
# pdb.set_trace()

import sys

sys.path.append('..')
import auth

api_key, api_secret = auth.key, auth.secret
symbol = "USDCUSDT"

# config_logging(logging, logging.DEBUG)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')

vl = 0.9991  # USDC的最小有效价格
vh = 1.0006  # USDC的最大有效价格
quantity = 50  # 准备购买的USDC枚数

status_prepare_buy = 0  # 准备买入USDC
status_new_buy = 1  # 买单挂单中
status_prepare_sell = 2  # 准备卖出USDC
status_new_sell = 3  # 卖单挂单中

# 会被修改的全局变量这里用字典
d = {'buy_price': 0.0,
     'sell_price': 0.0,
     'orderId': 0,
     'status': status_prepare_buy,
     'total_round': 0,
     'valid_round': 0,
     'error_round': 0,
     'profit': 0.0
     }

'''
def is_valid_price(bid1, ask1):
    if bid1 < vl or bid1 > vh:
        return False
    if ask1 < vl or ask1 > vh:
        return False

    return True
'''


# 只控制买价有没有脱锚，卖价只要不亏本就行
def is_valid_price(bid1):
    if bid1 < vl or bid1 > vh:
        return False

    return True


# m is a json object of str message
def get_balance(m, asset='USDC'):
    balance = 0

    for i, item in enumerate(m['result']['balances']):
        if item['asset'] == asset:
            balance = int(float(item['free']))
            break

    return balance


# m is a json object of str message
def get_message_type_stream(m):
    """
    'executionReport': 订单执行信息，由函数ws_stream_client.user_data订阅，类型多样
    'partial_book_depth': 目前挂单信息， 由函数ws_stream_client.partial_book_depth订阅，类型单一
    'account': 目前账户信息，由函数ws_api_client.account()触发，类型单一
    """
    message_type = ''

    try:
        if m['e'] == "executionReport":
            message_type = 'executionReport'
    except:
        pass

    try:
        m["lastUpdateId"] and m["bids"] and m["asks"]
        message_type = 'partial_book_depth'
    except:
        pass

    try:
        m['result']['balances']
        message_type = 'account'
    except:
        pass

    return message_type


# m is a json object of str message
def get_message_type_API(m):
    """
    'new_order': 订单执行信息，调用API new_order后返回
    """
    message_type = ''

    try:
        if m['result']['status'] and m['result']['side']:
            message_type = 'new_order'
    except:
        pass

    return message_type


def stat_to(s):
    d['status'] = s
    print("[Warning]status transfer to [%d]" % s)


def on_close(_):
    logging.info("Do custom stuff when connection is closed")


def websocket_api_message_handler(_, message):
    logging.info("message from websocket [API]:")
    logging.info(message)

    print('\n\n')

    '''
    m = json.loads(message)

    if get_message_type_API(m) == 'new_order':
        d['orderId'] = m['result']['orderId']

        if m['result']['side'] == 'BUY':
            stat_to(status_new_buy)
            print("[Warning][API]:" + str(sys._getframe().f_lineno) + "==>" + "receive new BUY, status-->%d\n\n" % d['status'])
        else:
            stat_to(status_new_sell)
            print("[Warning][API]:" + str(sys._getframe().f_lineno) + "==>" + "receive new SELL, status-->%d\n\n" % d['status'])
    '''


def websocket_stream_message_handler(_, message):
    # logging.info("message from websocket [Stream]:")
    # logging.info(message)

    m = json.loads(message)

    # 确定消息类型，根据消息类型修改状态
    message_type = get_message_type_stream(m)
    if not message_type:
        return

    # FSM
    if message_type == 'executionReport' and m['s'] == symbol:  # 状态转换
        logging.info("message from websocket [Stream]:")
        logging.info(message)
        if m['S'] == 'BUY' and m['X'] == 'NEW':  # api会接收到，stream这里也会接收到
            stat_to(status_new_buy)
            d['orderId'] = m['i']
            print(
                "[Warning][Stream]:" + str(sys._getframe().f_lineno) + "==>" + "receive BUY and NEW, status-->%d\n\n" %
                d['status'])
        elif m['S'] == 'BUY' and m['X'] == 'FILLED':  # FILLED的消息仅仅stream可以收到
            stat_to(status_prepare_sell)
            print("[Warning][Stream]:" + str(
                sys._getframe().f_lineno) + "==>" + "receive BUY and FILLED, status-->%d\n\n" % d['status'])
        elif m['S'] == 'SELL' and m['X'] == 'NEW':
            stat_to(status_new_sell)
            d['orderId'] = m['i']
            print(
                "[Warning][Stream]:" + str(sys._getframe().f_lineno) + "==>" + "receive SELL and NEW, status-->%d\n\n" %
                d['status'])
        elif m['S'] == 'SELL' and m['X'] == 'FILLED':
            stat_to(status_prepare_buy)
            # 统计盈利
            d['total_round'] += 1
            if d['sell_price'] > d['buy_price']:
                d['valid_round'] += 1
                d['profit'] += d['sell_price'] - d['buy_price']
            elif d['sell_price'] == d['buy_price']:
                print("[Warning][Stream]: use sell_price == buy_price to sell!!!!")
            else:
                d['error_round'] += 1
                print("[Stream][Error]: Fatal Error and Why sell_price < buy_price!!!!")
            print("[Warning][Stream]:" + str(sys._getframe().f_lineno) + "==>" +
                  "receive SELL and FILLED, status-->%d, total=%d, valid=%d, error=%d, profit=%f\n\n" %
                  (d['status'], d['total_round'], d['valid_round'], d['error_round'], d['profit']))
        elif m['S'] == 'BUY' and m['X'] == 'CANCELED':
            stat_to(status_prepare_buy)
        elif m['S'] == 'SELL' and m['X'] == 'CANCELED':
            stat_to(status_prepare_sell)
    elif message_type == 'partial_book_depth':
        # pdb.set_trace()
        ask1 = float(m['asks'][0][0])
        bid1 = float(m['bids'][0][0])
        # print("ask1=%f, bid1=%f, status=%d" % (ask1, bid1, d['status']))

        if d['status'] == status_prepare_buy:
            if is_valid_price(bid1):
                d['buy_price'] = bid1
                ws_api_client.new_order(  # 使用websocket api进行挂单买入
                    symbol=symbol,
                    side="BUY",
                    type="LIMIT",
                    timeInForce="GTC",
                    quantity=quantity,
                    price=d['buy_price'],
                    newOrderRespType="RESULT",
                )
                print("[Warning][Stream]:" + str(
                    sys._getframe().f_lineno) + "==>" + "[BUY]call new_order api: buy_price=%f\n\n" % d['buy_price'])
        elif d['status'] == status_new_buy:  # 如果买单挂单时，价格上涨则先撤单再挂新的买一价
            if d['buy_price'] < bid1:
                ws_api_client.cancel_order(symbol="USDCUSDT", orderId=d['orderId'])
                print("[Warning][Stream]:" + str(
                    sys._getframe().f_lineno) + "==>" + "[BUY]call cancel_order api\n\n")
        elif d['status'] == status_prepare_sell:
            d['sell_price'] = max(d['buy_price'], ask1)  # 挂的卖单不能低于成本【可以等于】，否则亏钱
            ws_api_client.new_order(  # 使用websocket api进行挂单卖出
                symbol=symbol,
                side="SELL",
                type="LIMIT",
                timeInForce="GTC",
                quantity=quantity,
                price=d['sell_price'],
                newOrderRespType="RESULT",
            )
            print("[Warning][Stream]:" + str(
                sys._getframe().f_lineno) + "==>" + "[SELL]call new_order api: sell_price=%f\n\n" % d['sell_price'])
        elif d['status'] == status_new_sell:
            # 条件1：挂的卖单价不是卖一了  条件2：挂的买单价大于成本价
            if d['sell_price'] > ask1 and d['sell_price'] > d['buy_price']:
                ws_api_client.cancel_order(symbol="USDCUSDT", orderId=d['orderId'])
                print("[Warning][Stream]:" + str(
                    sys._getframe().f_lineno) + "==>" + "[SELL]call cancel_order api: buy_price=%f, sell_price=%f, ask1=%f\n\n" % (
                      d['buy_price'], d['sell_price'], ask1))
        else:
            pass
    else:
        print("[Error][Stream]: Unknow Message Type\n\n")

def keep_alive_listen_key(spot_api_client, listen_key):
    logging.info(spot_api_client.renew_listen_key(listen_key))

if __name__ == '__main__':
    # make a connection to the websocket api
    ws_api_client = SpotWebsocketAPIClient(
        api_key=api_key,
        api_secret=api_secret,
        on_message=websocket_api_message_handler,
        on_close=on_close,
    )

    # make a connection to the websocket stream
    ws_stream_client = SpotWebsocketStreamClient(
        on_message=websocket_stream_message_handler,
    )

    # spot api client to call all restful api endpoints
    spot_api_client = SpotAPIClient(api_key)

    response = spot_api_client.new_listen_key()

    # You can subscribe to the user data stream from websocket stream, it will broadcast all the events
    # related to your account, including order updates, balance updates, etc.
    ws_stream_client.user_data(listen_key=response["listenKey"])
    ws_stream_client.partial_book_depth(symbol=symbol, level=5, speed=1000)

    # ws_api_client.account()

    # 每30分钟续时listen key
    scheduler = BackgroundScheduler()
    scheduler.add_job(keep_alive_listen_key, 'interval', args=[spot_api_client, response["listenKey"]],
                      next_run_time=datetime.datetime.now(),
                      seconds=30*60, id='my_job_id')
    scheduler.start()


    while True:
        time.sleep(3600)

    logging.info("closing ws connection")
    ws_api_client.stop()
    ws_stream_client.stop()
