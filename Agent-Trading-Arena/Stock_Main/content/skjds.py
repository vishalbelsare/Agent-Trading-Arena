import datetime
import numpy as np
import sqlite3
import json
import time

from database_utils import Database_operate, parse_orders
from Stock import Stock
from constant import Daily_Price_Limit, Fluctuation_Constant
class Market:
    def __init__(self, broker, persons, stocks, database: Database_operate, bid_step=1):
        self._bid_step = bid_step  # in percentage
        self.db = database
        self.stocks = stocks
        self.broker = broker
        self.persons = persons
        self.today_index = 7  # index of date
        self.sum_quantity=0
    def end_of_market(self, virtual_date):
        all_orders = self._fetch_orders("all", -1)
        for each_order in all_orders:
            stock_id=each_order["stock_id"]
            current_stock=self.stock[stock_id]
            trade_quantity = each_order["quantity"]
            total_quantity = current_stock.quantity
            cur_stock_price = current_stock.current_price
            deal_price =(current_stock.current_price+each_order["price"])/2
            if(
               self.broker.inventories[stock_id]<=0
               or each_order["person_id"]==-1
               or trade_quantity<=0
               ):
                continue
            if(
                    abs(deal_price - cur_stock_price)/cur_stock_price >Daily_Price_Limit
                    ):
                continue
            if each_order["type"]=="buy" and trade_quantity >self.broker.inventories[stock_id]:
                rest_quantity = trade_quantity-self.broker.inventories[stock_id]
                trade_quantity = self.broker.inventories[stock_id]
                status = "partially fulfilled"
                
            self.stocks[stock_id].current_price = (
                 deal_price *trade_quantity * Fluctuation_Constant
                 + cur_stock_price * total_quantity
                 )/(trade_quantity*Fluctuation_Constant+total_quantity)
            cur_stock_price=self.stocks[stock_id].current_price
            deal_price=cur_stock_price
            self.stocks[stock_id].update_trade_date(
                 virtual_date, cur_stock_price, trade_quantity
                 )
            self._update_order(
                each_order, deal_price, status, trade_quantity)
            if status == "partially fulfilled":
                self._update_order(
                    each_order, deal_price, "update", rest_quantity
                    )
            #broker update
            new_type = "buy" if each_order["type"]=="sell" else "sell"
            order={
                "stock_id":stock_id,
                "type":new_type,
                "virtual_date": virtual_date
                }
            self.broker.settlement(order, deal_price, trade_quantity)
            
                
        
        
        
    def _fetch_orders(self, type, fetch_stock_id):
        if type =="buy":
            fetch_cmd=("select * from active_orders where type = '{}' and stock_id={} and status='active'"
                       "order by price ASC, timestamp ASC".format(type, fetch_stock_id)
                       )
        elif type=="sell":
            fetch_cmd=(
                "select * from (select * from active_orders where type= '{type}'stock_id={id} "
                "and status='active' and"
                "person_id=-1 order by price DFSC, timestamp ASC) union all"
                "select * from (select * from active_orders where type= '{type}'stock_id={id} "
                "and status='active' and"
                "person_id>=0 order by price DFSC, timestamp ASC); ".format(type="sell", id=fetch_stock_id)
                )
        elif type=="all":
            fetch_cmd = (
                "select * from active_orders where status='active'"
                           "order by price ASC, timestamp ASC"
                )
        self.db.execute_sql(fetch_cmd)
        results = self.db.fetchall()
        # preprocess orders
        fetch_orders = parse_orders(results)
        return fetch_orders
    def _update_order(self, order, price, type, quantity):
        if type == "finished":
            if quantity<=0:
                quantity = order["quantity"]
            cmd="update active_order set quantity={}, status='finished' where timestamp={}".format(quantity, order["timestamp"])
            self.db.execute_sql(cmd)
            self.persons[order["person_id"]].settlement(order, price, quantity)
        if type == "partially fulfilled":
            cmd=(
                "Insert Into active_orders values({},{},{},{},{},{},'{}',{},{},'{}')"
                ).format(
                    order["timestamp"]+1,
                    order["virtual_date"],
                    order["weekday"],
                    order["iteration"],
                    order["stock_id"],
                    order["person_id"],
                    order["type"],
                    price,
                    quantity,
                    "finished",
                    )
            self.db.execute_sql(cmd)
            self.persons[order["person_id"]].settlement(order, price, quantity)
        if type == "update":
            cmd = "update active_orders set quantity={} where timestamp={}".format(
                quantity, order["timestamp"]
            )
            self.db.execute_sql(cmd)
            
            
            
            
            
            
            
            
            
            
            