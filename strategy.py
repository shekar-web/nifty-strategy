# -*- coding: utf-8 -*-
from SmartApi import SmartConnect
import pyotp
from logzero import logger
import time
import json
import urllib.request
import os
import threading
from datetime import datetime

api_key    = os.environ.get('ANGEL_API_KEY', '')
username   = os.environ.get('ANGEL_CLIENT_ID', '')
pwd        = os.environ.get('ANGEL_PWD', '')
TOTP_TOKEN = os.environ.get('ANGEL_TOTP', '')

EXCHANGE     = "NFO"
PRODUCT_TYPE = "CARRYFORWARD"
QUANTITY     = 65
POLL_SEC     = 0.5
MAIN_MIN     = 80.00
MAIN_MAX     = 80.80
HEDGE_MIN    = 9.50
HEDGE_MAX    = 10.80
DAILY_PROFIT_PCT  = 5.0
DIRECTION_SECONDS = 3
PAPER_TRADE  = False
CACHE_FILE   = "atm_cache.json"
today        = datetime.now()
daily_pnl    = 0.0
order_log    = []
order_results = {}

smartApi = SmartConnect(api_key)

def login():
    print("API KEY LENGTH: " + str(len(api_key)))
    print("CLIENT ID LENGTH: " + str(len(username)))
    print("PWD LENGTH: " + str(len(pwd)))
    print("TOTP LENGTH: " + str(len(TOTP_TOKEN)))
    print("Logging in...")
    try:
        totp = pyotp.TOTP(TOTP_TOKEN).now()
    except Exception as e:
        logger.error("Invalid TOTP Token")
        raise e
    data = smartApi.generateSession(username, pwd, totp)
    if data['status'] == False:
        logger.error(data)
        exit()
    print("Login successful")

login()

def wait_for_market_open():
    now = datetime.now()
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    if now >= market_open:
        print("Market already open.")
        return
    wait_secs = (market_open - now).seconds
    print("Waiting for 9:15 AM... " + str(wait_secs) + "s remaining")
    while True:
        now = datetime.now()
        if now >= market_open:
            break
        remaining = (market_open - now).seconds
        if remaining % 30 == 0:
            print(str(remaining) + "s to market open...")
        time.sleep(1)
    print("9:15 AM — Market open!")

def get_nifty_spot():
    try:
        resp = smartApi.ltpData("NSE", "Nifty 50", "26000")
        if resp and resp.get("data"):
            return float(resp["data"]["ltp"])
    except:
        pass
    return None

def get_direction():
    print("Detecting direction...")
    price1 = get_nifty_spot()
    if not price1:
        return None
    time.sleep(DIRECTION_SECONDS)
    price2 = get_nifty_spot()
    if not price2:
        return None
    if price2 > price1:
        print("UP -> Buying CE")
        return "CE"
    elif price2 < price1:
        print("DOWN -> Buying PE")
        return "PE"
    else:
        print("No movement. Retrying...")
        return None

def get_expiry_and_instruments():
    url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    with urllib.request.urlopen(url, timeout=30) as r:
        all_inst = json.loads(r.read().decode("utf-8", errors="ignore"))
    expiries = set()
    for item in all_inst:
        if (item.get("name") == "NIFTY" and
            item.get("exch_seg") == "NFO" and
            item.get("instrumenttype") == "OPTIDX"):
            expiries.add(item.get("expiry", ""))
    def parse_exp(e):
        try:
            return datetime.strptime(e, "%d%b%Y")
        except:
            return datetime.max
    nearest = sorted(expiries, key=parse_exp)[0]
    print("Using expiry: " + nearest)
    return nearest, all_inst

def load_token_map():
    if os.path.exists(CACHE_FILE):
        file_date = datetime.fromtimestamp(os.path.getmtime(CACHE_FILE)).date()
        if file_date == today.date():
            with open(CACHE_FILE, "r") as f:
                data = json.load(f)
            expiry = data["expiry"]
            token_map = {
                (int(k.split(",")[0]), k.split(",")[1]): v
                for k, v in data["token_map"].items()
            }
            print("Loaded cache for expiry " + expiry)
            return expiry, token_map
    print("Downloading master file...")
    expiry, all_inst = get_expiry_and_instruments()
    token_map = {}
    for item in all_inst:
        if (item.get("name") == "NIFTY" and
            item.get("exch_seg") == "NFO" and
            item.get("expiry") == expiry and
            item.get("instrumenttype") == "OPTIDX"):
            strike = int(float(item.get("strike", 0)) / 100)
            sym = item.get("symbol", "")
            otype = "CE" if sym.endswith("CE") else "PE"
            token_map[(strike, otype)] = {
                "symbol": sym,
                "token": item.get("token", ""),
                "strike": strike
            }
    with open(CACHE_FILE, "w") as f:
        json.dump({
            "expiry": expiry,
            "token_map": {str(k[0]) + "," + k[1]: v for k, v in token_map.items()}
        }, f)
    print("Cached " + str(len(token_map)) + " tokens")
    return expiry, token_map

def bulk_ltp(strikes):
    if not strikes:
        return []
    priced = []
    for i in range(0, len(strikes), 50):
        chunk = strikes[i:i+50]
        try:
            resp = smartApi.getMarketData("LTP", {EXCHANGE: [s["token"] for s in chunk]})
            if resp and resp.get("data") and resp["data"].get("fetched"):
                for item in resp["data"]["fetched"]:
                    tok = str(item.get("symbolToken", ""))
                    ltp = float(item.get("ltp", 0))
                    match = next((s for s in chunk if str(s["token"]) == tok), None)
                    if match and ltp > 0:
                        match["price"] = ltp
                        priced.append(match)
        except:
            pass
    return priced

def find_main_strike(token_map, direction, spot):
    atm = round(spot / 50) * 50
    candidates = []
    for offset in range(0, 1500, 50):
        strike = atm + offset if direction == "CE" else atm - offset
        key = (strike, direction)
        if key in token_map:
            candidates.append(token_map[key])
    if not candidates:
        return None
    priced = bulk_ltp(candidates)
    valid = sorted(
        [s for s in priced if MAIN_MIN <= s["price"] <= MAIN_MAX],
        key=lambda x: x["price"], reverse=True
    )
    return valid[0] if valid else None

def find_hedge_strike(token_map, hedge_direction, spot):
    atm = round(spot / 50) * 50
    candidates = []
    for offset in range(0, 3000, 50):
        strike = atm + offset if hedge_direction == "CE" else atm - offset
        key = (strike, hedge_direction)
        if key in token_map:
            candidates.append(token_map[key])
    if not candidates:
        return None
    priced = bulk_ltp(candidates)
    valid = sorted(
        [s for s in priced if HEDGE_MIN <= s["price"] <= HEDGE_MAX],
        key=lambda x: x["price"], reverse=True
    )
    return valid[0] if valid else None

def place_order(tradingsymbol, tok, side="BUY"):
    if PAPER_TRADE:
        fake_id = "PAPER_" + side + "_" + tradingsymbol
        order_log.append("[PAPER] " + side + ": " + tradingsymbol)
        order_results[tradingsymbol] = fake_id
        return fake_id
    try:
        orderid = smartApi.placeOrder({
            "variety": "NORMAL",
            "tradingsymbol": tradingsymbol,
            "symboltoken": tok,
            "transactiontype": side,
            "exchange": EXCHANGE,
            "ordertype": "MARKET",
            "producttype": PRODUCT_TYPE,
            "duration": "DAY",
            "price": "0",
            "squareoff": "0",
            "stoploss": "0",
            "quantity": str(QUANTITY)
        })
        order_log.append(side + ": " + tradingsymbol + " | ID: " + str(orderid))
        order_results[tradingsymbol] = orderid
        return orderid
    except Exception as e:
        order_log.append("Order FAILED " + tradingsymbol + ": " + str(e))
        order_results[tradingsymbol] = None
        return None

def place_both(main, hedge, side="BUY"):
    order_log.clear()
    t1 = threading.Thread(target=place_order, args=(main["symbol"], main["token"], side))
    t2 = threading.Thread(target=place_order, args=(hedge["symbol"], hedge["token"], side))
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    for log in order_log:
        print(log)

def monitor_trade(main_info, hedge_info, daily_target):
    global daily_pnl
    main_entry = main_info["price"]
    entry_value = (main_info["price"] * QUANTITY) + (hedge_info["price"] * QUANTITY)
    target_value = entry_value * (1 + DAILY_PROFIT_PCT / 100)
    print("Main: " + main_info["symbol"] + " @ Rs." + str(main_info["price"]))
    print("Hedge: " + hedge_info["symbol"] + " @ Rs." + str(hedge_info["price"]))
    print("Entry: Rs." + str(round(entry_value, 2)))
    print("Target: Rs." + str(round(target_value, 2)) + " (5%)")
    print("SL: Main back to Rs." + str(main_entry))
    while True:
        try:
            both = bulk_ltp([
                {"symbol": main_info["symbol"], "token": main_info["token"]},
                {"symbol": hedge_info["symbol"], "token": hedge_info["token"]}
            ])
            prices = {s["token"]: s["price"] for s in both}
            main_ltp = prices.get(main_info["token"])
            hedge_ltp = prices.get(hedge_info["token"])
            if main_ltp and hedge_ltp:
                current_value = (main_ltp * QUANTITY) + (hedge_ltp * QUANTITY)
                pnl = current_value - entry_value
                pnl_pct = (pnl / entry_value) * 100
                print(
                    "Main: Rs." + str(main_ltp) +
                    " | Hedge: Rs." + str(hedge_ltp) +
                    " | Value: Rs." + str(round(current_value, 2)) +
                    " | PnL: Rs." + str(round(pnl, 2)) +
                    " (" + str(round(pnl_pct, 2)) + "%)"
                )
                if current_value >= target_value:
                    place_both(main_info, hedge_info, side="SELL")
                    daily_pnl += pnl
                    print("5% Target hit! PnL: Rs." + str(round(pnl, 2)))
                    return "PROFIT"
                if main_ltp <= main_entry:
                    place_both(main_info, hedge_info, side="SELL")
                    daily_pnl += pnl
                    print("SL hit! PnL: Rs." + str(round(pnl, 2)))
                    return "SL"
        except Exception as e:
            print("Monitor error: " + str(e))
        time.sleep(POLL_SEC)

def run():
    global daily_pnl
    wait_for_market_open()
    expiry, token_map = load_token_map()
    market_close = datetime.now().replace(hour=15, minute=30, second=0)
    initial_entry = (MAIN_MAX * QUANTITY) + (HEDGE_MAX * QUANTITY)
    daily_target = initial_entry * (DAILY_PROFIT_PCT / 100)
    print("Daily target: Rs." + str(round(daily_target, 2)))
    if PAPER_TRADE:
        print("*** PAPER TRADE MODE ***")
    while True:
        if datetime.now() >= market_close:
            print("Market closed. Daily PnL: Rs." + str(round(daily_pnl, 2)))
            break
        if daily_pnl >= daily_target:
            print("Daily target hit! Total PnL: Rs." + str(round(daily_pnl, 2)))
            break
        direction = None
        while not direction:
            if datetime.now() >= market_close:
                break
            direction = get_direction()
            if not direction:
                time.sleep(1)
        if not direction:
            break
        hedge_direction = "PE" if direction == "CE" else "CE"
        spot = get_nifty_spot()
        if not spot:
            print("Could not get spot. Retrying...")
            time.sleep(1)
            continue
        print("Nifty spot: " + str(spot))
        main = find_main_strike(token_map, direction, spot)
        if not main:
            print("No main strike found. Retrying...")
            time.sleep(1)
            continue
        hedge = find_hedge_strike(token_map, hedge_direction, spot)
        if not hedge:
            print("No hedge strike found. Retrying...")
            time.sleep(1)
            continue
        print("Main: " + main["symbol"] + " @ Rs." + str(main["price"]))
        print("Hedge: " + hedge["symbol"] + " @ Rs." + str(hedge["price"]))
        place_both(main, hedge, side="BUY")
        if not order_results.get(main["symbol"]) or not order_results.get(hedge["symbol"]):
            print("Order failed. Retrying...")
            time.sleep(2)
            continue
        time.sleep(3)
        filled = bulk_ltp([
            {"symbol": main["symbol"], "token": main["token"]},
            {"symbol": hedge["symbol"], "token": hedge["token"]}
        ])
        prices = {s["token"]: s["price"] for s in filled}
        main_actual = prices.get(main["token"], main["price"])
        hedge_actual = prices.get(hedge["token"], hedge["price"])
        main_info = {"symbol": main["symbol"], "token": main["token"], "price": main_actual}
        hedge_info = {"symbol": hedge["symbol"], "token": hedge["token"], "price": hedge_actual}
        result = monitor_trade(main_info, hedge_info, daily_target)
        if result == "PROFIT":
            print("Daily target reached. Stopping.")
            break
        elif result == "SL":
            print("SL hit. Waiting 3s for new direction...")
            time.sleep(3)

run()
