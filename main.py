from kiteconnect import KiteConnect
import time, datetime, json, sys, os
import xlwings as xw
import pandas as pd


def get_login_credentials():
    global login_credential

    def login_credentials():
        print("Enter your Zerodha login credentials...")
        login_credential = {
            "api_key": str(input("Enter API key: ")),
            "api_secret": str(input("Enter API Secret :"))
        }
        if str(input("Enter y/Y to save login credential: ")).upper() == 'Y':
            with open(f"Login_Credentials.txt", "w") as f:
                json.dump(login_credential, f)
            print("data saved.")
        else:
            print("data will not be saved")

    while True:
        try:
            with open(f"Login_Credentials.txt", "r") as f:
                login_credential = json.load(f)
            break
        except (OSError, Exception) as err:
            print(f"Unexpected {err=}, {type(err)=}")
            login_credentials()


def get_access_token():
    global access_token

    def login():
        global login_credential
        print("Trying loging in...")
        kite = KiteConnect(api_key=login_credential["api_key"])
        print("Login URL: ", kite.login_url())
        request_token = str(input("Enter request token : "))
        try:
            access_token = kite.generate_session(
                request_token=request_token,
                api_secret=login_credential["api_secret"]
            )["access_token"]
            os.makedirs(f"AccessToken", exist_ok=True)
            with open(f"AccessToken/{datetime.datetime.now().date()}.json", "w") as f:
                json.dump(access_token, f)
            print("Login successful!")
        except (OSError, Exception) as err:
            print(f"Unexpected {err=}, {type(err)=}")
            print("Login failed!")

    print("Loading access token...")
    while True:
        if os.path.exists(f"AccessToken/{datetime.datetime.now().date()}.json"):
            with open(f"AccessToken/{datetime.datetime.now().date()}.json", "r") as f:
                access_token = json.load(f)
            break
        else:
            login()
    return access_token


def get_kite():
    global kite, login_credential, access_token
    try:
        kite = KiteConnect(api_key=login_credential["api_key"])
        kite.set_access_token(access_token)
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        os.remove(f"AccessToken/{datetime.datetime.now().date()}.json") if os.path.exists(
            f"AccessToken/{datetime.datetime.now().date()}.json") else sys.exit()
        sys.exit()


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
    get_login_credentials()
    get_access_token()
    get_kite()
    get_order_book()
    start_excel()

# END OF PROGRAM
