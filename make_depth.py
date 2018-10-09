import copy
import json
import time
import math
import threading
import traceback
import logging
import websocket
import pybitflyer
import mysql.connector
from time import sleep
from collections import deque
from datetime import datetime


class DataPool:
    
    def __init__(self):
        self.mid_price = 0
        self.ask_board = []
        self.bid_board = []
        self.timestamp = int(time.time()-time.time()%5)
    
    def store_data(self):
        
        def on_message(ws, message):
            message = json.loads(message)
            if message["params"]["channel"] == "lightning_board_snapshot_FX_BTC_JPY":
                board_snapshot_info = message["params"]["message"]
                self.ask_board = board_snapshot_info["asks"]
                self.bid_board = board_snapshot_info["bids"]
                self.mid_price = board_snapshot_info["mid_price"]
            
            elif message["params"]["channel"] == "lightning_board_FX_BTC_JPY":
                board_info = message["params"]["message"]
                ask_board_list = board_info["asks"]
                bid_board_list = board_info["bids"]
                ask_board = self.ask_board
                bid_board = self.bid_board
                
                if (not len(ask_board_list) == 0) and (not len(ask_board) == 0):
                    ask_board_price_list = [i["price"] for i in ask_board_list]
                    for i in ask_board:
                        if i["price"] in ask_board_price_list:
                            ask_index = ask_board.index(i)
                            ask_board.pop(ask_index)
                    for i in ask_board_list:
                        if (not i["size"] == 0):
                            ask_board.append(i)
                
                if (not len(bid_board_list) == 0) and (not len(bid_board) == 0):
                    bid_board_price_list = [i["price"] for i in bid_board_list]
                    for i in bid_board:
                        if i["price"] in bid_board_price_list:
                            bid_index = bid_board.index(i)
                            bid_board.pop(bid_index)
                    for i in bid_board_list:
                        if (not i["size"] == 0):
                            bid_board.append(i)
                
                self.ask_board = ask_board
                self.bid_board = bid_board
                self.mid_price = board_info["mid_price"]
        
        def on_error(ws, error):
            logging.debug(error)
        
        def on_close(ws):
            try:
                logging.info("disconnected Bitflyer streaming server. Restarting websocket.")
                websocket.enableTrace(True)
                ws = websocket.WebSocketApp("wss://ws.lightstream.bitflyer.com/json-rpc", on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
                ws.run_forever()
            except KeyboardInterrupt:
                sys.exit()
        
        def on_open(ws):
            ws.send(json.dumps({"method": "subscribe", "params": {"channel": "lightning_board_snapshot_FX_BTC_JPY"}}))
            ws.send(json.dumps({"method": "subscribe", "params": {"channel": "lightning_board_FX_BTC_JPY"}}))
            logging.info("connected streaming server")
        
        websocket.enableTrace(True)
        ws = websocket.WebSocketApp("wss://ws.lightstream.bitflyer.com/json-rpc", on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
        try:
            print("ws.run_forever()")
            ws.run_forever()
        except KeyboardInterrupt:
            print("sys.exit()")
            sys.exit()
    
    def to_db(self):
        while True:
            if time.time() >= self.timestamp+5:
                board = json.dumps({"mid_price":self.mid_price, "ask_board":self.ask_board, "bid_board":self.bid_board})
                conn = mysql.connector.connect(host=host, port=port, user=user, password=password)
                cursor = conn.cursor()
                cursor.execute('''insert into bitflyer.depth (timestamp, board) values ({0}, \'{1}\');'''.format(int(self.timestamp+5), board))
                conn.commit()
                conn.close()
                self.timestamp += 5


if __name__ == "__main__":
    
    f = open("config/config.json", "r")
    config = json.load(f)
    host = config["host"]
    port = config["port"]
    user = config["user"]
    password = config["password"]
    
    logging.basicConfig(filename="Log/make_depth.log", level=logging.INFO, format="(%(threadName)-10s) %(message)s")
    data_pool = DataPool()
    
    store_data_thread = threading.Thread(target=data_pool.store_data)
    store_data_thread.start()
    
    sleep(10)
    
    to_db_thread = threading.Thread(target=data_pool.to_db)
    to_db_thread.start()





