# -*- coding: utf-8 -*-
from SmartApi import SmartConnect
import pyotp
from logzero import logger
import time
import json
import urllib.request
import os
import threading
from datetime import datetime, timedelta

# ── CREDENTIALS ──────────────────────────────────────────────────
api_key    = os.environ.get('ANGEL_API_KEY', '')
username   = os.environ.get('ANGEL_CLIENT_ID', '')
pwd        = os.environ.get('ANGEL_PWD', '')
TOTP_TOKEN = os.environ.get('ANGEL_TOTP', '')

# ── CONFIG ───────────────────────────────────────────────────────
EXPIRY       = "13APR2026"
EXCHANGE     = "NFO"
PRODUCT_TYPE = "CARRYFORWARD"
QUANTITY     = 195
PROFIT_PCT   = 3.0
POLL_SEC     = 0.2

CE_START    = 26150
PE_START    = 20100
SCAN_STEPS  = 100
STEP        = 50

PREMIUM_MIN = 9.10
PREMIUM_MAX = 10.90

CACHE_FILE  = "nifty_cache_" + EXPIRY + ".json"
today       = datetime.now()

# ── LOGIN ─────────────────────────────────────────────────────────
smartApi = SmartConnect(api_key)

def login():
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
    authToken    = data['data']['jwtToken']
    refreshToken = data['data']['refreshToken']
    print("Login successful")

login()


# ── STEP 1: Load Token Map ────────────────────────────────────────
def load_token_map():
    if os.path.exists(CACHE_FILE):
        file_date = datetime.fromtimestamp(os.path.getmtime(CACHE_FILE)).date()
        if file_date == today.date():
            with open(CACHE_FILE, "r") as f:
                token_map = json.load(f)
            return {(int(k.split(",")[0]), k.split(",")[1]): v
                    for k, v in token_map.items()}

    print("Downloading master file...")
    with urllib.request.urlopen(
        "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json",
        timeout=30
    ) as r:
        all_inst = json.loads(r.read().decode("utf-8", errors="ignore"))

    token_map = {}
    for item in all_inst:
        if (item.get("name") == "NIFTY" and
            item.get("exch_seg") == "NFO" and
            item.get("expiry") == EXPIRY and
            item.get("instrumenttype") == "OPTIDX"):
            strike = int(float(item.get("strike", 0)) / 100)
            sym    = item.get("symbol", "")
            otype  = "CE" if sym.endswith("CE") else "PE"
            token_map[(strike, otype)] = {
                "symbol": sym,
                "token" : item.get("token", ""),
                "strike": strike
            }

    with open(CACHE_FILE, "w") as f:
        json.dump({str(k[0]) + "," + k[1]: v for k, v in token_map.items()}, f)
    print("Cached " + str(len(token_map)) + " NIFTY option tokens")
    return token_map


# ── STEP 2: Build Strike Lists ────────────────────────────────────
def get_scan_strikes(token_map):
    ce_strikes = []
    for i in range(SCAN_STEPS):
        strike = CE_START - (i * STEP)
        if (strike, "CE") in token_map:
            ce_strikes.append(token_map[(strike, "CE")])

    pe_strikes = []
    for i in range(SCAN_STEPS):
        strike = PE_START + (i * STEP)
        if (strike, "PE") in token_map:
            pe_strikes.append(token_map[(strike, "PE")])

    return ce_strikes, pe_strikes


# ── STEP 3: Bulk Price Fetch ──────────────────────────────────────
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
                    tok   = str(item.get("symbolToken", ""))
                    ltp   = float(item.get("ltp", 0))
                    match = next((s for s in chunk if str(s["token"]) == tok), None)
                    if match and ltp > 0:
                        match["price"] = ltp
                        priced.append(match)
        except:
            pass
    return priced


# ── STEP 4: Find Best Pair ────────────────────────────────────────
def find_pair(ce_strikes, pe_strikes):
    all_priced = bulk_ltp(ce_strikes + pe_strikes)

    ce_valid = sorted(
        [s for s in all_priced
         if s["symbol"].endswith("CE")
         and PREMIUM_MIN <= s["price"] <= PREMIUM_MAX],
        key=lambda x: x["price"], reverse=True
    )
    pe_valid = sorted(
        [s for s in all_priced
         if s["symbol"].endswith("PE")
         and PREMIUM_MIN <= s["price"] <= PREMIUM_MAX],
        key=lambda x: x["price"], reverse=True
    )

    if not ce_valid or not pe_valid:
        return None

    ce = ce_valid[0]
    pe = pe_valid[0]

    return {
        "ce_symbol": ce["symbol"],
        "ce_token" : ce["token"],
        "ce_strike": ce["strike"],
        "ce_price" : ce["price"],
        "pe_symbol": pe["symbol"],
        "pe_token" : pe["token"],
        "pe_strike": pe["strike"],
        "pe_price" : pe["price"],
        "diff"     : round(abs(ce["price"] - pe["price"]), 2)
    }


# ── STEP 5: Place Order ───────────────────────────────────────────
order_results = {}
order_log     = []

def place_order(tradingsymbol, tok, side="BUY"):
    try:
        orderid = smartApi.placeOrder({
            "variety"        : "NORMAL",
            "tradingsymbol"  : tradingsymbol,
            "symboltoken"    : tok,
            "transactiontype": side,
            "exchange"       : EXCHANGE,
            "ordertype"      : "MARKET",
            "producttype"    : PRODUCT_TYPE,
            "duration"       : "DAY",
            "price"          : "0",
            "squareoff"      : "0",
            "stoploss"       : "0",
            "quantity"       : str(QUANTITY)
        })
        order_log.append(side + ": " + tradingsymbol + " | ID: " + str(orderid))
        order_results[tradingsymbol] = orderid
        return orderid
    except Exception as e:
        order_log.append("Order FAILED " + tradingsymbol + ": " + str(e))
        order_results[tradingsymbol] = None
        return None


# ── STEP 6: Place Both Orders Simultaneously ─────────────────────
def place_both_orders(pair, side="BUY"):
    order_log.clear()
    t1 = threading.Thread(target=place_order, args=(pair["ce_symbol"], pair["ce_token"], side))
    t2 = threading.Thread(target=place_order, args=(pair["pe_symbol"], pair["pe_token"], side))
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    for log in order_log:
        print(log)
    ce_id = order_results.get(pair["ce_symbol"])
    pe_id = order_results.get(pair["pe_symbol"])
    return ce_id, pe_id


# ── STEP 7: Wait for Fills from Order Book ────────────────────────
def wait_for_fills(ce_id, pe_id, ce_fallback, pe_fallback, timeout=10):
    start = time.time()
    while time.time() - start < timeout:
        try:
            ce_order  = smartApi.individual_order_details(ce_id)
            pe_order  = smartApi.individual_order_details(pe_id)
            ce_status = ce_order["data"]["status"].upper()
            pe_status = pe_order["data"]["status"].upper()
            ce_avg    = float(ce_order["data"]["averageprice"])
            pe_avg    = float(pe_order["data"]["averageprice"])
            if ce_status == "COMPLETE" and pe_status == "COMPLETE":
                if ce_avg > 0 and pe_avg > 0:
                    print("Both filled! CE: Rs." + str(ce_avg) + " | PE: Rs." + str(pe_avg))
                    return ce_avg, pe_avg
        except:
            pass
        time.sleep(0.002)
    print("Timeout. Using scanned price.")
    return ce_fallback, pe_fallback


# ── STEP 8: Monitor and Exit ──────────────────────────────────────
def monitor_and_exit(ce_info, pe_info):
    ce_value     = ce_info["price"] * QUANTITY
    pe_value     = pe_info["price"] * QUANTITY
    entry_value  = ce_value + pe_value
    target_value = entry_value * (1 + PROFIT_PCT / 100)

    print("CE fill : Rs." + str(ce_info["price"]) + " x " + str(QUANTITY) + " = Rs." + str(round(ce_value, 2)))
    print("PE fill : Rs." + str(pe_info["price"]) + " x " + str(QUANTITY) + " = Rs." + str(round(pe_value, 2)))
    print("Entry   : Rs." + str(round(entry_value, 2)))
    print("Target  : Rs." + str(round(target_value, 2)) + " (" + str(PROFIT_PCT) + "%)")

    while True:
        try:
            both  = bulk_ltp([
                {"symbol": ce_info["symbol"], "token": ce_info["token"]},
                {"symbol": pe_info["symbol"], "token": pe_info["token"]}
            ])
            prices = {s["token"]: s["price"] for s in both}
            ce_ltp = prices.get(ce_info["token"])
            pe_ltp = prices.get(pe_info["token"])

            if ce_ltp and pe_ltp:
                current_value = (ce_ltp * QUANTITY) + (pe_ltp * QUANTITY)
                pnl           = current_value - entry_value
                pnl_pct       = (pnl / entry_value) * 100
                print(
                    "CE: Rs." + str(ce_ltp) +
                    " | PE: Rs." + str(pe_ltp) +
                    " | Value: Rs." + str(round(current_value, 2)) +
                    " | PnL: Rs." + str(round(pnl, 2)) +
                    " (" + str(round(pnl_pct, 2)) + "%)"
                )

                if current_value >= target_value:
                    place_both_orders({
                        "ce_symbol": ce_info["symbol"],
                        "ce_token" : ce_info["token"],
                        "pe_symbol": pe_info["symbol"],
                        "pe_token" : pe_info["token"]
                    }, side="SELL")
                    print("Target hit! Value: Rs." + str(round(current_value, 2)))
                    print("PnL: Rs." + str(round(pnl, 2)) + " (" + str(round(pnl_pct, 2)) + "%)")
                    print("Both legs exited.")
                    break

        except Exception as e:
            print("Monitor error: " + str(e))

        time.sleep(POLL_SEC)


# ── MAIN ─────────────────────────────────────────────────────────
def run():
    token_map              = load_token_map()
    ce_strikes, pe_strikes = get_scan_strikes(token_map)

    if not ce_strikes or not pe_strikes:
        print("No strikes found. Check CE_START/PE_START.")
        return

    pair = find_pair(ce_strikes, pe_strikes)

    if not pair:
        print("No pair found in range Rs." + str(PREMIUM_MIN) + " to Rs." + str(PREMIUM_MAX))
        return

    print(
        "Pair: CE " + str(pair["ce_strike"]) +
        " @ Rs." + str(pair["ce_price"]) +
        " | PE " + str(pair["pe_strike"]) +
        " @ Rs." + str(pair["pe_price"]) +
        " | Diff: Rs." + str(pair["diff"])
    )

    ce_id, pe_id = place_both_orders(pair, side="BUY")

    if not ce_id or not pe_id:
        print("One or both orders failed.")
        return

    ce_actual, pe_actual = wait_for_fills(
        ce_id, pe_id,
        pair["ce_price"], pair["pe_price"]
    )

    ce_info = {"symbol": pair["ce_symbol"], "token": pair["ce_token"], "price": ce_actual}
    pe_info = {"symbol": pair["pe_symbol"], "token": pair["pe_token"], "price": pe_actual}

    monitor_and_exit(ce_info, pe_info)


run()
