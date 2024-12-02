
import datetime
import pickle
import sqlite3
import json
import time
import os.path as osp
from database_utils import query_all_stocks, Database_operate
from Person import Person, Broker
from Stock import Stock, Market_index
from Market import Market
from behavior import stock_ops, reflection, generate_gossip
from constant import persona_path, stock_path, Iterations_Daily, No_Days, Save_Path, Num_Person, Num_Stock
from load_json import save_all, load_all
# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.


def sort():
    myDict = {"ravi": 10, "rajnish": 9, "sanjeev": 15, "yash": 2, "suraj": 32}

    myKeys = list(myDict.keys())
    myKeys.sort()
    sorted_dict = {i: myDict[i] for i in myKeys}
    # print(sorted_dict)


def db_operate():
    conn = sqlite3.connect("test.db")
    cur = conn.cursor()
    cmd_create_table = "Create Table active_bids (timetag Integer, virtual_time text, stock_id INTEGER, type text check(type IN ('sell','buy')), price float)"
    cur.execute(cmd_create_table)
    cmd_insert = "Insert Into active_bids values({},'{}',{},'{}',{})".format(
        101, "vir", 1, "buy", 36.87
    )
    cur.execute(cmd_insert)
    conn.commit()
    conn.close()


def save_json():
    to_save = {1: 10, 2: 34, 3: 23}
    json.dumps(to_save)


def parse_data(order):
    # timestamp int, virtual_time text, weekday int, stock_id int, person_id int, type text, price float
    name_tags = [
        "timestamp",
        "virtual_time",
        "weekday",
        "stock_id",
        "person_id",
        "type",
        "price",
    ]
    return_dic = {}
    for index, name in enumerate(name_tags):
        return_dic[name] = order[index]
    # print(return_dic)
    return return_dic


def data_parse(inputs):
    for each in inputs:
        parse_data(each)


def db_op2():
    conn = sqlite3.connect("Simu0.db")
    cur = conn.cursor()
    cmd = (
        "SELECT  * from (select * from active_orders where stock_id=3 order by price,timestamp) union all "
        "SELECT  * from (select * from active_orders where stock_id=2 order by price,timestamp)"
    )
    # cmd = "SELECT  * from (select * from active_orders where stock_id=1 order by price,timestamp)"
    # cmd = "select * from stock order by price"
    cur.execute(cmd)
    results = cur.fetchall()
    for each in results:
        print(each)
    conn.commit()
    conn.close()


def round_two_decimal(input):
    try:
        res = float("{:.2f}".format(input))
        return res
    except Exception:
        return input


def db_op3():
    dic = {"a": 1, "b": 3, "c": 3}
    for count, (key, value) in enumerate(dic.items()):
        print(count, key, value)


def time_test():

    """
    gpt_response = "Operation: buy, Stock name: C, Investment Amount: $100.00, Best Buying Price: $490.6"
    gpt_response = "Operation: sell, Stock name: B,The number of shares: 32, Best Selling Price: 474.84"
    gpt_response = "Operation: buy, Stock name: C, Investment Amount: 10000, Best Buying Price: $493.5"
    gpt_response = "Operation: buy, Stock name: C, Investment Amount: $15840.0, Best Buying Price: $491.57"
    match = re.search(r"^\s*Operation:\s*buy,\s*Stock name:\s*([A-Z]),\s*Investment Amount:\s*\$?(\d+(\.\d+)?),\s*Best Buying Price:\s*\$?(\d+(\.\d+)?)\s*$", gpt_response)
    print(match.group(0))
    print(match.group(1))
    print(match.group(2))
    print(match.group(3))
    print(match.group(4))
    print(match.group(5))
    """


def pickle_test():
    import pickle

    database = Database_operate("Simu0")
    stk = Stock(0, database, stock_path)
    stk.db = None
    stk.current_price = 1000
    output = open("1.pkl", "wb")
    strs = pickle.dumps(stk)
    output.write(strs)
    output.close()


def pickle_load():
    database = Database_operate("Simu0")
    with open("1.pkl", "rb") as file:
        stk = pickle.loads(file.read())
    print(stk.current_price)


def init_all(load=False):
    if load:
        (
            current_date,
            current_iteration,
            broker,
            market_index,
            market,
            stocks,
            persons,
        ) = load_all()
    else:
        # initialize all objects
        database = Database_operate(osp.join(Save_Path, "data"))

        # clear tables
        cmd = "drop table if exists active_orders"
        database.execute_sql(cmd)
        cmd = "drop table if exists stock"
        database.execute_sql(cmd)
        cmd = "drop table if exists person"
        database.execute_sql(cmd)
        cmd = "drop table if exists account"
        database.execute_sql(cmd)
        cmd = "drop table if exists memory"
        database.execute_sql(cmd)
        cmd = "drop table if exists gossip"
        database.execute_sql(cmd)

        database.init_database()

        stocks = []
        persons = []

        for i in range(Num_Stock):
            stocks.append(Stock(i, database, stock_path))
        market_index = Market_index(stocks, database)
        broker = Broker(stocks, database)

        for i in range(Num_Person):
            persons.append(Person(i, broker, stocks, database, persona_path))
        persons.append(broker)
        market = Market(broker, persons, stocks, database)

    return 0, 0, broker, market_index, market, stocks, persons


def overall_test():
    (
        current_date,
        current_iteration,
        broker,
        market_index,
        market,
        stocks,
        persons,
    ) = init_all(False)
    for virtual_date in range(No_Days):
        if virtual_date == 0:
            broker.ipo(virtual_date)
        market_index.update_market_index(virtual_date)
        generate_gossip(virtual_date, persons, stocks)
        for iter in range(Iterations_Daily):
            ops = stock_ops(virtual_date, persons, stocks, market_index, iter)
            rand = random.sample(range(0,Num_Person),Num_Person)
            for i in rand:
                for j in range(2): 
                    op = ops[i][j]
                    persons[i].create_order(i, op, virtual_date, iter)
            market.match_order(virtual_date) 
            market.end_of_market(virtual_date)
            market_index.update_market_index(virtual_date)
            for each_person in persons:
                if each_person.person_id >= 0:
                    each_person.end_of_iteration(virtual_date, iter)


            reflection(virtual_date, persons, stocks, market_index, iter)
            save_all(virtual_date, iter, stocks, market_index, persons, market)

        # end of a trading day
        market.end_of_day(virtual_date)
        for each_person in persons:
            each_person.end_of_day(virtual_date)
        for each_stock in stocks:
            each_stock.end_of_day(virtual_date)
        market_index.end_of_day(virtual_date)


# Press the green button in the gutter to run the script.
if __name__ == "__main__":
    overall_test()
    # time_test()
    # db_op3()
    # pickle_test()
    # pickle_load()
