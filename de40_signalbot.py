import os
import json
import time
import requests
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(__file__)
ENV_PATH = os.path.join(BASE_DIR, ".env")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_DIR = os.getenv("DATA_DIR", BASE_DIR)  # default: Projektordner
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_DIR = os.getenv("DATA_DIR", BASE_DIR)  # default: Projektordner
CSV_PATH = os.path.join(DATA_DIR, "DAX_H1.csv")
STATE_PATH = os.path.join(DATA_DIR, "state_de40.json")



YAHOO_URL = "https://query1.finance.yahoo.com/v8/finance/chart/%5EGDAXI"

def fetch_data():
    params = {"range": "60d", "interval": "60m"}
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(YAHOO_URL, params=params, headers=headers, timeout=30)
    r.raise_for_status()

    j = r.json()["chart"]["result"][0]

    rows = []
    q = j["indicators"]["quote"][0]
    for i, ts in enumerate(j["timestamp"]):
        if q["close"][i] is None:
            continue
        rows.append({
            "time": datetime.fromtimestamp(ts, tz=timezone.utc),
            "open": q["open"][i],
            "high": q["high"][i],
            "low": q["low"][i],
            "close": q["close"][i]
        })

    return pd.DataFrame(rows)


def update_csv():
    df = fetch_data()
    if os.path.exists(CSV_PATH):
        old = pd.read_csv(CSV_PATH, parse_dates=["time"])
        df = pd.concat([old, df]).drop_duplicates(subset=["time"]).sort_values("time")
    df.to_csv(CSV_PATH, index=False)
    return df

def ema(series, n):
    return series.ewm(span=n).mean()

def atr(df, n):
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - df["close"].shift()).abs(),
        (df["low"] - df["close"].shift()).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(n).mean()

def load_state():
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH) as f:
            return json.load(f)
    return {}

def save_state(s):
    with open(STATE_PATH, "w") as f:
        json.dump(s, f)

def send_telegram(token, chat, text):
    requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat, "text": text}
    )

def main():
    load_dotenv(ENV_PATH)
    token = os.environ["TELEGRAM_BOT_TOKEN"]
    chat = os.environ["TELEGRAM_CHAT_ID"]

    state = load_state()
    last_bar = state.get("last_bar")

    while True:
        df = update_csv()
        df["ema20"] = ema(df["close"], 20)
        df["ema50"] = ema(df["close"], 50)
        df["ema200"] = ema(df["close"], 200)
        df["atr"] = atr(df, 14)

        last = df.iloc[-1]
        prev = df.iloc[-2]
        bar_time = str(last["time"])

        if bar_time != last_bar:
            if last["close"] > last["ema200"] and prev["ema20"] <= prev["ema50"] and last["ema20"] > last["ema50"]:
                send_telegram(token, chat, f"DE40 LONG Signal\nEntry: {last['close']:.1f}")
            if last["close"] < last["ema200"] and prev["ema20"] >= prev["ema50"] and last["ema20"] < last["ema50"]:
                send_telegram(token, chat, f"DE40 SHORT Signal\nEntry: {last['close']:.1f}")

            state["last_bar"] = bar_time
            save_state(state)

        time.sleep(600)

if __name__ == "__main__":
    main()
    while True:
        time.sleep(60)




