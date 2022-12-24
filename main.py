from kiteconnect import KiteConnect
import time, datetime, json, sys, os, yaml
import xlwings as xw
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from pyotp import TOTP
from pathlib import Path


def login_with_zerodha():
    global kite, login_credential, access_token

    try:

        with open("config/config.yaml") as f:
            login_credential = yaml.load(f, Loader=yaml.FullLoader)
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        driver.get(f'https://kite.trade/connect/login?api_key={login_credential["zerodha"]["api_key"]}&v=3')
        driver.maximize_window()
        login_id = WebDriverWait(driver, 10).until(lambda x: x.find_element('xpath', 'html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[1]/input'))
        pwd = WebDriverWait(driver, 10).until(lambda x: x.find_element('xpath', 'html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[2]/input'))
        login_id.send_keys(login_credential["zerodha"]["user_id"])
        pwd.send_keys(login_credential["zerodha"]["user_pwd"])

        submit = WebDriverWait(driver, 10).until(
            lambda x: x.find_element('xpath', '//*[@id="container"]/div/div/div[2]/form/div[4]/button'))
        submit.click()

        time.sleep(1)
        # adjustment to code to include totp
        totp = WebDriverWait(driver, 10).until(lambda x: x.find_element('xpath', '/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[2]/input'))

        authkey = TOTP(login_credential["zerodha"]["totp_key"]).now()
        totp.send_keys(authkey)
        # adjustment complete

        continue_btn = WebDriverWait(driver, 10).until(
            lambda x: x.find_element('xpath', '//*[@id="container"]/div/div/div[2]/form/div[3]/button'))
        continue_btn.click()
        time.sleep(5)
        url = driver.current_url
        initial_token = url.split('request_token=')[1]
        request_token = initial_token.split('&')[0]
        driver.close()
        kite = KiteConnect(api_key=login_credential["zerodha"]["api_key"])
        # print(request_token)
        data = kite.generate_session(request_token, api_secret=login_credential["zerodha"]["api_secret"])
        access_token = data['access_token']
        kite.set_access_token(data['access_token'])
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        sys.exit()
    print("Login success")
    return kite


def get_live_data(instruments):
    global kite, live_data
    try:
        live_data
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        live_data = {}
    try:
        live_data = kite.quote(instruments)
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
    return live_data


def place_order(symbol, qty, direction):
    try:
        order = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=symbol[0:3],
            tradingsymbol=symbol[4:],
            transaction_type=kite.TRANSACTION_TYPE_BUY if direction.upper() == "BUY" else kite.TRANSACTION_TYPE_SELL,
            quantity=int(qty),
            product=kite.PRODUCT_MIS,
            order_type=kite.ORDER_TYPE_MARKET,
            price=0.0,
            validity=kite.VALIDITY_DAY,
            tag="V_PYTHON"
        )
        print("Order placed for", symbol[4:])
        return order
    except Exception as err:
        return f"{err}"


def get_order_book():
    global orders
    try:
        orders
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        orders = {}
    try:
        data = pd.DataFrame(kite.orders())
        data = data[data["tag"] == "V_PYTHON"]
        data = data.filter(
            ["order_timestamp", "exchange", "tradingsymbol", "transaction_type", "quantity", "average_price", "status",
             "status_message_raw"])
        data.columns = data.columns.str.replace("_", "")
        data.columns = data.columns.str.title()
        data = data.set_index(["Order Timestamp"], drop=True)
        data = data.sort_index(ascending=True)
        orders = data
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
    return orders


def start_excel():
    global kite, live_data
    print("Excel Starting...")
    if not os.path.exists("V_PYTHON.xlsx"):
        try:
            wb = xw.Book()
            wb.save("V_PYTHON.xlsx")
            wb.close()
        except Exception as err:
            print(f"Unexpected {err=}, {type(err)=}")
            sys.exit()
    wb = xw.Book("V_PYTHON.xlsx")
    for sheet in ["Data", "Exchange", "OrderBook"]:
        try:
            wb.sheets(sheet)
        except Exception as err:
            print(f"Unexpected {err=}, {type(err)=}")
            wb.sheets.add(sheet)
    dt = wb.sheets("Data")
    ex = wb.sheets("Exchange")
    ob = wb.sheets("OrderBook")
    ex.range("a:j").value = ob.range("a:h").value = dt.range("p:q").value = None
    dt.range("a1:q1").value = ["Sr no", "Symbol", "Open", "High", "Low", "LTP", "Volume", "Vwap", "Best Bid Price",
                               "Best Ask Price", "Close", "Qty", "Direction", "Entry Signal", "Exit Signal",
                               "Entry", "Exit"]
    sub_list = []
    while True:
        try:
            master_contract = pd.DataFrame(kite.instruments())
            master_contract = master_contract.drop(["instrument_token", "exchange_token", "last_price", "tick_size"],
                                                   axis=1)
            master_contract["watchlist_symbol"] = master_contract["exchange"] + ":" + master_contract["tradingsymbol"]
            master_contract.columns = master_contract.columns.str.replace("_", " ")
            master_contract.columns = master_contract.columns.str.title()
            ex.range("a1").value = master_contract
            break
        except Exception as err:
            print(f"Unexpected {err=}, {type(err)=}")
            time.sleep(1)
    while True:
        try:
            time.sleep(0.5)
            get_live_data(sub_list)
            symbols = dt.range(f"b{2}:b{500}").value
            trading_info = dt.range(f"l{2}:q{500}").value

            for i in sub_list:
                if i not in symbols:
                    sub_list.remove(i)
                    try:
                        del live_data[i]
                    except Exception as err:
                        print(f"Unexpected {err=}, {type(err)=}")
            main_list = []
            idx = 0
            for i in symbols:
                lst = [None, None, None, None, None, None, None, None, None]
                if i:
                    if i not in sub_list:
                        sub_list.append(i)
                    if i in sub_list:
                        try:
                            lst = [live_data[i]["ohlc"]["open"],
                                   live_data[i]["ohlc"]["high"],
                                   live_data[i]["ohlc"]["low"],
                                   live_data[i]["last_price"]
                                   ]
                            try:
                                lst += [live_data[i]["volume"],
                                        live_data[i]["average_price"],
                                        live_data[i]["depth"]["buy"][0]["price"],
                                        live_data[i]["depth"]["sell"][0]["price"],
                                        live_data[i]["ohlc"]["close"]
                                        ]
                            except Exception as err:
                                lst += [0, 0, 0, 0, live_data[i]["ohlc"]["close"]]
                            trade_info = trading_info[idx]
                            if trade_info[0] is not None and trade_info[1] is not None:
                                if type(trade_info[0]) is float and type(trade_info[1]) is str:
                                    if trade_info[1].upper() == "BUY" and trade_info[2] is True:
                                        if trade_info[2] is True and trade_info[3] is not True and trade_info[
                                            4] is None and trade_info[5] is None:
                                            dt.range(f"p{idx + 2}").value = place_order(i, int(trade_info[0]), "BUY")
                                        elif trade_info[2] is True and trade_info[3] is True and trade_info[
                                            4] is None and trade_info[5] is None:
                                            dt.range(f"q{idx + 2}").value = place_order(i, int(trade_info[0]), "SELL")
                                        if trade_info[1].upper() == "SELL" and trade_info[2] is True:
                                            if trade_info[2] is True and trade_info[3] is not True and trade_info[
                                                4] is None and trade_info[5] is None:
                                                dt.range(f"p{idx + 2}").value = place_order(i, int(trade_info[0]),
                                                                                            "SELL")
                                            elif trade_info[2] is True and trade_info[3] is True and trade_info[
                                                4] is None and trade_info[5] is None:
                                                dt.range(f"q{idx + 2}").value = place_order(i, int(trade_info[0]),
                                                                                            "BUY")
                        except Exception as err:
                            print(f"Unexpected {err=}, {type(err)=}")
                    main_list.append(lst)
                    idx += 1
                dt.range("c2").value = main_list
                if wb.sheets.active.name == "OrderBook":
                    ob.range("a1").value = get_order_book()
        except Exception as err:
            print(f"Unexpected {err=}, {type(err)=}")


if __name__ == '__main__':
    login_with_zerodha()
    get_order_book()
    start_excel()

# END OF PROGRAM
