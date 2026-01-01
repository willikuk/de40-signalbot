import os
import sys
import json
import time
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf
import requests


# -----------------------------
# Paths / Environment
# -----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.getenv("DATA_DIR", BASE_DIR)
STATE_PATH = os.path.join(DATA_DIR, "state_de40.json")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


# -----------------------------
# Boot logs
# -----------------------------
print("BOOT: started", flush=True)
print("BOOT: python", sys.version, flush=True)
print("BOOT: STATE_PATH =", STATE_PATH, flush=True)
print("BOOT: has TELEGRAM_BOT_TOKEN =", bool(TELEGRAM_BOT_TOKEN), flush=True)
print("BOOT: has TELEGRAM_CHAT_ID =", bool(TELEGRAM_CHAT_ID), flush=True)


# -----------------------------
# Telegram helpers
# -----------------------------
def telegram_send(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("TELEGRAM: missing token/chat_id; cannot send", flush=True)
        return

    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": text},
            timeout=30,
        )
        if r.status_code != 200:
            print("TELEGRAM: send failed", r.status_code, r.text[:200], flush=True)
    except Exception as e:
        print("TELEGRAM: send exception:", e, flush=True)


# -----------------------------
# State (dedupe / last signal)
# -----------------------------
def load_state() -> dict:
    try:
        if os.path.exists(STATE_PATH):
            with open(STATE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        print("STATE: load failed:", e, flush=True)
    return {}


def save_state(state: dict) -> None:
    try:
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(state, f)
    except Exception as e:
        print("STATE: save failed:", e, flush=True)


# -----------------------------
# Indicators
# -----------------------------
def ema(series: pd.Series, n: int) -> pd.Series:
    return series.ewm(span=n, adjust=False).mean()


# -----------------------------
# Data
# -----------------------------
def load_dax_h1() -> pd.DataFrame | None:
    print("DATA: loading DAX H1 from Yahoo Finance", flush=True)

    df = yf.download("^GDAXI", interval="1h", period="14d", progress=False)

    # Robust flatten
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

    if df.empty:
        print("DATA: no data received", flush=True)
        return None

    df = df.reset_index()
    df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

    # Normalize column names
    df.rename(
        columns={
            "Datetime": "time",
            "Date": "time",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
        },
        inplace=True,
    )

    # Keep only core columns
    needed = ["time", "open", "high", "low", "close"]
    for col in needed:
        if col not in df.columns:
            print("DATA: missing column:", col, "available:", list(df.columns), flush=True)
            return None

    df = df[needed].copy()
    df = df.dropna().sort_values("time").reset_index(drop=True)

    # Ensure numeric
    df["open"] = df["open"].astype(float)
    df["high"] = df["high"].astype(float)
    df["low"] = df["low"].astype(float)
    df["close"] = df["close"].astype(float)

    print(f"DATA: loaded {len(df)} candles, last time = {df.iloc[-1]['time']}", flush=True)
    return df


# -----------------------------
# Strategy / Main loop
# -----------------------------
def check_signals_once() -> None:
    df = load_dax_h1()
    if df is None:
        return

    # Need enough candles for EMA200
    if len(df) < 210:
        print(f"STATUS: not enough candles for EMA200 (have {len(df)})", flush=True)
        return

    df["ema20"] = ema(df["close"], 20)
    df["ema50"] = ema(df["close"], 50)
    df["ema200"] = ema(df["close"], 200)

    # Use dicts to avoid pandas Series comparison issues
    prev = df.iloc[-2].to_dict()
    last = df.iloc[-1].to_dict()

    # Basic crossover + trend filter (as in your condition)
    long_signal = (
        last["close"] > last["ema200"]
        and prev["ema20"] <= prev["ema50"]
        and last["ema20"] > last["ema50"]
    )

    short_signal = (
        last["close"] < last["ema200"]
        and prev["ema20"] >= prev["ema50"]
        and last["ema20"] < last["ema50"]
    )

    state = load_state()
    last_sent_key = state.get("last_sent_key")

    # Create a unique key per candle to prevent duplicates
    candle_time = last["time"]
    # Convert to string safely
    candle_time_str = str(candle_time)
    signal_type = "LONG" if long_signal else ("SHORT" if short_signal else None)

    if signal_type is None:
        print("STATUS: no signal this candle", flush=True)
        return

    sent_key = f"{signal_type}:{candle_time_str}"
    if sent_key == last_sent_key:
        print("STATUS: signal already sent for this candle", sent_key, flush=True)
        return

    msg = (
        f"DE40 Signal: {signal_type}\n"
        f"Time (candle): {candle_time_str}\n"
        f"Close: {last['close']:.2f}\n"
        f"EMA20: {last['ema20']:.2f} | EMA50: {last['ema50']:.2f} | EMA200: {last['ema200']:.2f}"
    )
    telegram_send(msg)

    state["last_sent_key"] = sent_key
    save_state(state)
    print("STATUS: sent", sent_key, flush=True)


def main() -> None:
    telegram_send("Render: Bot gestartet und läuft.")

    # Check every 5 minutes; strategy dedupes by candle time
    # (H1 candle will only trigger once due to last_sent_key)
    while True:
        try:
            check_signals_once()
        except Exception as e:
            print("ERROR: main loop exception:", e, flush=True)

        print("HEARTBEAT: alive", flush=True)
        time.sleep(300)


if __name__ == "__main__":
    main()

