"""
每週台股趨勢訊號系統 v1
===================
Repository : github.com/ryanhsu1983/AI_stock_market_weekly
從每日版模型改造為週報：追蹤台股加權與中大型權值股的本週變化、趨勢判斷與下週觀察。
"""

import html as html_lib
import base64, json, os, re, smtplib, sys, requests
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from urllib.parse import quote_plus
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

CACHE_DIR = Path(__file__).parent / ".yfinance_cache"
CACHE_DIR.mkdir(exist_ok=True)
yf.cache.set_cache_location(str(CACHE_DIR))

UP_COLOR = "#c0392b"
DOWN_COLOR = "#168f4d"
WARN_COLOR = "#e67e22"
INFO_COLOR = "#3498db"
NEUTRAL_COLOR = "#95a5a6"
TAIPEI_TZ = timezone(timedelta(hours=8))
WEEKLY_DARK = "#12322b"
WEEKLY_DARK_2 = "#1f493f"
WEEKLY_GOLD = "#c9a227"
WEEKLY_BG = "#f4f2ea"
WEEKLY_PANEL = "#fffdf7"


def get_report_meta(report_date: datetime | None = None) -> dict:
    dt = report_date or datetime.now(TAIPEI_TZ)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TAIPEI_TZ)
    iso = dt.date().isocalendar()
    return {
        "date": dt.strftime("%Y-%m-%d"),
        "date_key": dt.strftime("%Y%m%d"),
        "year": dt.strftime("%Y"),
        "week": iso.week,
        "week_key": f"{iso.year}-W{iso.week:02d}",
        "week_label": f"第{iso.week}週",
    }


def pct_text(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:+.2f}%"

WEIGHTS = {
    "trend": 25,
    "macd": 20,
    "institutional": 15,
    "kd": 12,
    "obv": 8,
    "fx": 7,
    "rates": 7,
    "vol": 6,
}

SIGNAL_LEVELS = [
    (70, "STRONG", "強訊號"),
    (50, "MID", "中訊號"),
    (30, "WEAK", "弱訊號"),
    (15, "NOTICE", "提醒"),
    (0, "NEUTRAL", "無訊號"),
]

TRADE_BASE_PCTS = {
    "STRONG": 50,
    "MID": 40,
    "WEAK": 10,
    "NOTICE": 0,
    "NEUTRAL": 0,
}


# ── 讀取設定 ────────────────────────────────────────────────
def load_config() -> dict:
    with open(Path(__file__).parent / "config.json", "r", encoding="utf-8") as f:
        return json.load(f)


def get_stock_cfg(stock: dict, global_cfg: dict) -> dict:
    """
    將全域設定與個股 overrides 合併，個股設定優先。
    回傳該股票實際使用的完整設定。
    """
    ov  = stock.get("overrides", {})
    thr = dict(global_cfg["thresholds"])
    ma  = dict(global_cfg["ma_periods"])

    # 覆蓋 thresholds
    for key in ("kd_buy","kd_sell","bias20_buy","bias20_sell",
                "bias60_p_low","bias60_p_high","vol_ma_period","obv_ma_period"):
        if key in ov:
            thr[key] = ov[key]

    # 向下相容舊欄位名稱
    if "bias_buy"  in thr and "bias20_buy"  not in thr: thr["bias20_buy"]  = thr["bias_buy"]
    if "bias_sell" in thr and "bias20_sell" not in thr: thr["bias20_sell"] = thr["bias_sell"]

    # 覆蓋 ma_periods
    if "ma_periods" in ov:
        ma.update(ov["ma_periods"])

    return {
        "thresholds":       thr,
        "ma_periods":       ma,
        "pyramid":          global_cfg.get("pyramid", {}),
        "use_obv":          ov.get("use_obv",          True),
        "use_vol_trend":    ov.get("use_vol_trend",     True),
        "use_institutional":ov.get("use_institutional", True),
        "use_fx":           ov.get("use_fx",            True),
        "use_rates":        ov.get("use_rates",         True),
        "macro_sensitivity": ov.get("macro_sensitivity", "market"),
        "leverage_warning": ov.get("leverage_warning",  False),
        "bias60_locked":    ov.get("bias60_locked",     True),
    }


def _parse_int(value) -> int:
    try:
        return int(str(value).replace(",", "").replace(" ", ""))
    except Exception:
        return 0


def _find_field(fields: list, *keywords: str) -> int | None:
    for idx, field in enumerate(fields):
        if all(keyword in field for keyword in keywords):
            return idx
    return None


def _find_exact_field(fields: list, name: str) -> int | None:
    try:
        return fields.index(name)
    except ValueError:
        return None


# ── 三大法人資料 ─────────────────────────────────────────────
def fetch_institutional(ticker: str, lookback_days: int = 7) -> dict:
    stock_id = ticker.upper().replace(".TW", "").replace(".TWO", "")
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
        ),
        "Referer": "https://www.twse.com.tw/",
    }

    last_error = ""
    for offset in range(lookback_days):
        date_str = (datetime.now(TAIPEI_TZ) - timedelta(days=offset)).strftime("%Y%m%d")
        url = (
            "https://www.twse.com.tw/rwd/zh/fund/T86"
            f"?response=json&date={date_str}&selectType=ALL"
        )
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            last_error = f"證交所連線失敗:{str(exc)[:80]}"
            continue

        if data.get("stat") != "OK":
            last_error = f"{date_str} 狀態:{data.get('stat')}"
            continue

        fields = data.get("fields", [])
        rows = data.get("data", [])
        idx_id = _find_field(fields, "證券代號")
        idx_foreign = (
            _find_exact_field(fields, "外陸資買賣超股數(不含外資自營商)")
            or _find_field(fields, "外陸資", "買賣超")
        )
        idx_invest = _find_exact_field(fields, "投信買賣超股數") or _find_field(fields, "投信", "買賣超")
        idx_dealer = _find_exact_field(fields, "自營商買賣超股數")
        idx_total = _find_exact_field(fields, "三大法人買賣超股數")

        if None in (idx_id, idx_foreign, idx_invest, idx_dealer):
            last_error = f"{date_str} 欄位格式異動"
            continue

        for row in rows:
            if str(row[idx_id]).strip() == stock_id:
                foreign = _parse_int(row[idx_foreign])
                invest = _parse_int(row[idx_invest])
                dealer = _parse_int(row[idx_dealer])
                total = _parse_int(row[idx_total]) if idx_total is not None else foreign + invest + dealer
                return {
                    "success": True,
                    "date": date_str,
                    "foreign_net": foreign,
                    "invest_net": invest,
                    "dealer_net": dealer,
                    "total_net": total,
                    "error": "",
                }
        last_error = f"{date_str} 找不到 {stock_id}"

    return {
        "success": False,
        "date": "",
        "foreign_net": 0,
        "invest_net": 0,
        "dealer_net": 0,
        "total_net": 0,
        "error": last_error or "無三大法人資料",
    }


def fetch_weekly_institutional(ticker: str, end_date: datetime | None = None, lookback_days: int = 10) -> dict:
    stock_id = ticker.upper().replace(".TW", "").replace(".TWO", "")
    if not stock_id.isdigit():
        return {
            "success": False,
            "date_range": "",
            "foreign_net": 0,
            "invest_net": 0,
            "dealer_net": 0,
            "total_net": 0,
            "days": 0,
            "error": "指數不適用三大法人個股買賣超",
        }

    base = end_date or datetime.now(TAIPEI_TZ)
    if base.tzinfo is None:
        base = base.replace(tzinfo=TAIPEI_TZ)
    iso = base.date().isocalendar()
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
        ),
        "Referer": "https://www.twse.com.tw/",
    }
    totals = {"foreign_net": 0, "invest_net": 0, "dealer_net": 0, "total_net": 0}
    hit_dates = []
    last_error = ""

    for offset in range(lookback_days):
        day = base.date() - timedelta(days=offset)
        if day.isocalendar()[:2] != iso[:2]:
            continue
        date_str = day.strftime("%Y%m%d")
        url = (
            "https://www.twse.com.tw/rwd/zh/fund/T86"
            f"?response=json&date={date_str}&selectType=ALL"
        )
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            last_error = f"{date_str} 證交所連線失敗:{str(exc)[:80]}"
            continue
        if data.get("stat") != "OK":
            last_error = f"{date_str} 狀態:{data.get('stat')}"
            continue

        fields = data.get("fields", [])
        idx_id = _find_field(fields, "證券代號")
        idx_foreign = (
            _find_exact_field(fields, "外陸資買賣超股數(不含外資自營商)")
            or _find_field(fields, "外陸資", "買賣超")
        )
        idx_invest = _find_exact_field(fields, "投信買賣超股數") or _find_field(fields, "投信", "買賣超")
        idx_dealer = _find_exact_field(fields, "自營商買賣超股數")
        idx_total = _find_exact_field(fields, "三大法人買賣超股數")
        if None in (idx_id, idx_foreign, idx_invest, idx_dealer):
            last_error = f"{date_str} 欄位格式異動"
            continue

        for row in data.get("data", []):
            if str(row[idx_id]).strip() != stock_id:
                continue
            foreign = _parse_int(row[idx_foreign])
            invest = _parse_int(row[idx_invest])
            dealer = _parse_int(row[idx_dealer])
            total = _parse_int(row[idx_total]) if idx_total is not None else foreign + invest + dealer
            totals["foreign_net"] += foreign
            totals["invest_net"] += invest
            totals["dealer_net"] += dealer
            totals["total_net"] += total
            hit_dates.append(date_str)
            break

    if not hit_dates:
        return dict(success=False, date_range="", days=0, error=last_error or "本週無三大法人資料", **totals)

    hit_dates.sort()
    return dict(
        success=True,
        date_range=f"{hit_dates[0]}-{hit_dates[-1]}",
        days=len(hit_dates),
        error="",
        **totals,
    )


def fetch_market_institutional_value_day(day) -> dict | None:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36",
        "Referer": "https://www.twse.com.tw/",
    }
    date_str = day.strftime("%Y%m%d") if hasattr(day, "strftime") else str(day)
    url = f"https://www.twse.com.tw/rwd/zh/fund/BFI82U?response=json&dayDate={date_str}&type=day"
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return None
    if data.get("stat") != "OK":
        return None
    fields = data.get("fields", [])
    rows = data.get("data", [])
    idx_name = _find_exact_field(fields, "單位名稱") or 0
    idx_net = _find_exact_field(fields, "買賣差額")
    if idx_net is None:
        return None
    totals = {"foreign": 0, "trust": 0, "dealer": 0, "total": 0}
    for row in rows:
        name = str(row[idx_name])
        net = _parse_int(row[idx_net])
        if "合計" in name:
            totals["total"] = net
        elif "外資及陸資" in name and "不含" in name:
            totals["foreign"] += net
        elif "投信" in name:
            totals["trust"] += net
        elif "自營商" in name:
            totals["dealer"] += net
    if not any(totals.values()):
        return None
    return {"date": date_str, **totals}


def fetch_market_institutional_value_week(end_date: datetime | None = None, lookback_days: int = 10) -> dict:
    base = end_date or datetime.now(TAIPEI_TZ)
    if base.tzinfo is None:
        base = base.replace(tzinfo=TAIPEI_TZ)
    iso = base.date().isocalendar()
    daily = []
    for offset in range(lookback_days):
        day = base.date() - timedelta(days=offset)
        if day.isocalendar()[:2] != iso[:2]:
            continue
        item = fetch_market_institutional_value_day(day)
        if item:
            daily.append(item)
    daily.sort(key=lambda x: x["date"])
    if not daily:
        return {"success": False, "daily": [], "foreign": 0, "trust": 0, "dealer": 0, "total": None, "error": "本週無三大法人金額資料"}
    return {
        "success": True,
        "daily": daily,
        "foreign": sum(x["foreign"] for x in daily),
        "trust": sum(x["trust"] for x in daily),
        "dealer": sum(x["dealer"] for x in daily),
        "total": sum(x["total"] for x in daily),
        "date_range": f"{daily[0]['date']}-{daily[-1]['date']}",
        "error": "",
    }


# ── 抓取資料 ────────────────────────────────────────────────
def fetch_data(ticker: str, days: int) -> pd.DataFrame:
    # yfinance 的 end 是「不含當日」的結束日期；收盤後要抓到今天資料，必須設成台灣明天。
    end   = datetime.now(TAIPEI_TZ).date() + timedelta(days=1)
    start = end - timedelta(days=days)
    df = yf.download(ticker,
                     start=start.strftime("%Y-%m-%d"),
                     end=end.strftime("%Y-%m-%d"),
                     progress=False, auto_adjust=True)
    if df.empty:
        raise ValueError(f"無法取得 {ticker} 資料")
    df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    return df[["Open","High","Low","Close","Volume"]].dropna()


def _fetch_close_series(ticker: str, days: int = 180) -> pd.Series:
    end   = datetime.now(TAIPEI_TZ).date() + timedelta(days=1)
    start = end - timedelta(days=days)
    df = yf.download(ticker,
                     start=start.strftime("%Y-%m-%d"),
                     end=end.strftime("%Y-%m-%d"),
                     progress=False, auto_adjust=True)
    if df.empty:
        raise ValueError(f"無法取得 {ticker} 資料")
    df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    return df["Close"].dropna()


def _series_change_pct(series: pd.Series, periods: int) -> float | None:
    if len(series) <= periods:
        return None
    prev = float(series.iloc[-1 - periods])
    if prev == 0:
        return None
    return (float(series.iloc[-1]) - prev) / prev * 100


def fetch_market_context() -> dict:
    """
    抓取每日會影響台股風險偏好的總體資料。
    USD/TWD 上升代表美元變貴、台幣轉弱；美債殖利率上升代表估值壓力提高。
    """
    context = {"success": True, "fx": None, "rates": None, "errors": []}

    try:
        fx = _fetch_close_series("TWD=X", 180)
        context["fx"] = {
            "ticker": "TWD=X",
            "label": "美元/台幣",
            "value": float(fx.iloc[-1]),
            "chg_5d_pct": _series_change_pct(fx, 5),
            "chg_20d_pct": _series_change_pct(fx, 20),
            "series": [float(x) for x in fx.tail(20)],
        }
    except Exception as exc:
        context["success"] = False
        context["errors"].append(f"匯率資料失敗:{str(exc)[:80]}")

    try:
        rates = _fetch_close_series("^TNX", 180)
        current = float(rates.iloc[-1])
        context["rates"] = {
            "ticker": "^TNX",
            "label": "美國10年期公債殖利率",
            "value": current,
            "chg_5d_bp": (current - float(rates.iloc[-6])) * 100 if len(rates) > 5 else None,
            "chg_20d_bp": (current - float(rates.iloc[-21])) * 100 if len(rates) > 20 else None,
            "series": [float(x) for x in rates.tail(20)],
        }
    except Exception as exc:
        context["success"] = False
        context["errors"].append(f"利率資料失敗:{str(exc)[:80]}")

    return context


# ── 計算指標 ────────────────────────────────────────────────
def calc_indicators(df: pd.DataFrame, scfg: dict) -> pd.DataFrame:
    ma  = scfg["ma_periods"]
    thr = scfg["thresholds"]
    s, m, l = ma["short"], ma["mid"], ma["long"]

    df[f"MA{s}"] = df["Close"].rolling(s).mean()
    df[f"MA{m}"] = df["Close"].rolling(m).mean()
    df[f"MA{l}"] = df["Close"].rolling(l).mean()

    # BIAS60（季線乖離，固定60日，用於Z-Score）
    ma60         = df["Close"].rolling(60).mean()
    df["BIAS60"] = (df["Close"] - ma60) / ma60 * 100
    b60_clean    = df["BIAS60"].dropna()
    p_low        = thr.get("bias60_p_low",  5)
    p_high       = thr.get("bias60_p_high", 95)
    df.attrs["bias60_p_high"] = float(b60_clean.quantile(p_high / 100))
    df.attrs["bias60_p_low"]  = float(b60_clean.quantile(p_low  / 100))
    df.attrs["bias60_mean"]   = float(b60_clean.mean())
    df.attrs["bias60_std"]    = float(b60_clean.std())
    df["BIAS60_Z"] = (df["BIAS60"] - df.attrs["bias60_mean"]) / df.attrs["bias60_std"]

    # 短線乖離率（依各股 mid MA）
    df["Bias20"] = (df["Close"] - df[f"MA{m}"]) / df[f"MA{m}"] * 100

    # KD
    low_min  = df["Low"].rolling(9).min()
    high_max = df["High"].rolling(9).max()
    rsv      = (df["Close"] - low_min) / (high_max - low_min) * 100
    df["K"]  = rsv.ewm(com=2, adjust=False).mean()
    df["D"]  = df["K"].ewm(com=2, adjust=False).mean()

    # MACD
    ema12           = df["Close"].ewm(span=12, adjust=False).mean()
    ema26           = df["Close"].ewm(span=26, adjust=False).mean()
    df["DIF"]       = ema12 - ema26
    df["Signal"]    = df["DIF"].ewm(span=9, adjust=False).mean()
    df["MACD_hist"] = df["DIF"] - df["Signal"]

    # 量能趨勢
    vp           = thr["vol_ma_period"]
    df["Vol_MA"] = df["Volume"].rolling(vp).mean()
    df["Vol_Trend"] = df["Vol_MA"] - df["Vol_MA"].shift(3)

    # OBV
    obv = [0]
    for i in range(1, len(df)):
        if   df["Close"].iloc[i] > df["Close"].iloc[i-1]: obv.append(obv[-1] + df["Volume"].iloc[i])
        elif df["Close"].iloc[i] < df["Close"].iloc[i-1]: obv.append(obv[-1] - df["Volume"].iloc[i])
        else:                                               obv.append(obv[-1])
    df["OBV"]    = obv
    df["OBV_MA"] = df["OBV"].rolling(thr["obv_ma_period"]).mean()

    return df


# ── BIAS60 Z-Score 評估 ──────────────────────────────────────
def eval_bias60(df: pd.DataFrame, scfg: dict) -> dict:
    latest  = df.iloc[-1]
    bias60  = float(latest["BIAS60"])
    z       = float(latest["BIAS60_Z"])
    p_high  = df.attrs["bias60_p_high"]
    p_low   = df.attrs["bias60_p_low"]
    p_high_pct = scfg["thresholds"].get("bias60_p_high", 95)
    p_low_pct  = scfg["thresholds"].get("bias60_p_low",   5)
    can_lock   = scfg.get("bias60_locked", True)

    if bias60 >= p_high:
        zone   = "overheated"
        locked = can_lock
        label  = f"🔥 過熱{'鎖定' if can_lock else '警示'}（季線乖離{bias60:.1f}%，歷史{p_high_pct}%分位）"
        color  = UP_COLOR
        note   = f"Z={z:.2f}｜超過歷史{p_high_pct}%分位({p_high:.1f}%)｜{'強制禁止買進' if can_lock else '僅警示，不鎖定'}"
    elif bias60 <= p_low:
        zone   = "oversold"
        locked = False
        label  = f"❄️ 超跌部署區（季線乖離{bias60:.1f}%，歷史{p_low_pct}%分位）"
        color  = DOWN_COLOR
        note   = f"Z={z:.2f}｜低於歷史{p_low_pct}%分位({p_low:.1f}%)｜統計黃金建倉區"
    else:
        zone   = "normal"
        locked = False
        label  = f"正常範圍（季線乖離{bias60:.1f}%）"
        color  = NEUTRAL_COLOR
        note   = f"Z={z:.2f}｜介於{p_low_pct}%({p_low:.1f}%)～{p_high_pct}%({p_high:.1f}%)分位之間"

    return dict(zone=zone, locked=locked, bias60=bias60,
                z_score=z, p_high=p_high, p_low=p_low,
                label=label, color=color, note=note)


# ── 金字塔建倉計算 ───────────────────────────────────────────
def calc_pyramid(df: pd.DataFrame, scfg: dict, signal_level: str) -> dict:
    py         = scfg.get("pyramid", {})
    drop_step  = py.get("add_per_drop_pct",    5.0)
    add_ratio  = py.get("add_ratio_pct",       20.0)
    time_days  = py.get("time_rebalance_days", 20)
    time_ratio = py.get("time_add_ratio_pct",   5.0)

    close    = float(df["Close"].iloc[-1])
    recent   = df["Close"].iloc[-time_days:]
    high_ref = float(recent.max())
    drop_pct = (close - high_ref) / high_ref * 100
    range_pct = (float(recent.max()) - float(recent.min())) / float(recent.min()) * 100
    is_consolidating = range_pct < 5.0
    suggestions = []

    if signal_level.startswith("BUY_"):
        batches = int(abs(drop_pct) / drop_step) if drop_pct < 0 else 0
        if batches == 0:
            suggestions.append(
                f"📌 第1批建倉：建議投入可用資金 <b>{add_ratio:.0f}%</b>（首批試單）")
        else:
            suggestions.append(
                f"📌 第{batches+1}批加碼：距高點回落 {abs(drop_pct):.1f}%，"
                f"建議再投入剩餘資金 <b>{add_ratio:.0f}%</b>")
            suggestions.append(
                f"　　累計已達 {batches} 次加碼條件（每跌 {drop_step:.0f}% 加一批）")
        if is_consolidating:
            suggestions.append(
                f"⏱️ 時間補位提醒：近 {time_days} 日盤整幅度僅 {range_pct:.1f}%，"
                f"可考慮投入剩餘資金 <b>{time_ratio:.0f}%</b> 進行時間性補位")

    return dict(drop_pct=drop_pct, is_consolidating=is_consolidating,
                range_pct=range_pct, suggestions=suggestions)


def score_to_signal(score: float) -> tuple:
    for threshold, key, label in SIGNAL_LEVELS:
        if score >= threshold:
            return key, label
    return "NEUTRAL", "無訊號"


def classify_market_regime(close: float, ma_s: float, ma_m: float, ma_l: float,
                           ma_s_prev: float, ma_m_prev: float, ma_l_prev: float) -> dict:
    ma_s_up = ma_s > ma_s_prev
    ma_m_up = ma_m > ma_m_prev
    ma_l_up = ma_l > ma_l_prev

    if ma_m > ma_l and close > ma_m and ma_s_up and ma_m_up and ma_l_up:
        return {
            "key": "STRONG_BULL",
            "label": "大多頭",
            "color": UP_COLOR,
            "note": "中期均線維持多頭排列，價格也站在主要均線上方；此時重點是抱住核心部位，不因短線弱賣出訊號頻繁下車。",
        }
    if ma_m > ma_l:
        return {
            "key": "BULL_PULLBACK",
            "label": "多頭修正",
            "color": WARN_COLOR,
            "note": "中期仍是多頭，但短線轉弱或跌回均線附近；此時適合觀察是否回到支撐，而不是把它直接當成空頭。",
        }
    if ma_m < ma_l and close < ma_m:
        return {
            "key": "BEAR",
            "label": "空頭",
            "color": DOWN_COLOR,
            "note": "中期均線偏空且價格落在主要均線下方；此時賣出訊號權重提高，買進訊號需更保守。",
        }
    return {
        "key": "RANGE",
        "label": "盤整",
        "color": NEUTRAL_COLOR,
        "note": "趨勢方向尚未明確；此時可依分數分批，但不宜把單一弱訊號視為重倉依據。",
    }


def _parse_signal_level(level: str) -> tuple:
    if level.startswith("BUY_"):
        return "BUY", level.replace("BUY_", "")
    if level.startswith("SELL_"):
        return "SELL", level.replace("SELL_", "")
    if level.startswith("OVERHEATED_"):
        return "OVERHEATED", level.replace("OVERHEATED_", "")
    return "HOLD", "NEUTRAL"


def build_trade_plan(level: str, regime: dict, b60: dict, lev_warn: bool = False) -> dict:
    direction, level_key = _parse_signal_level(level)
    base_pct = TRADE_BASE_PCTS.get(level_key, 0)
    regime_key = regime["key"]
    action = "觀察"
    trade_pct = 0
    color = NEUTRAL_COLOR
    headline = "不建議交易"
    reason = "目前訊號不足，保留觀察即可。"

    if direction == "BUY":
        action = "買進或加碼"
        color = UP_COLOR
        if regime_key == "BEAR":
            trade_pct = {"STRONG": 20, "MID": 10, "WEAK": 0, "NOTICE": 0, "NEUTRAL": 0}.get(level_key, 0)
            reason = "空頭環境下即使出現買訊，也先視為反彈或試單，不建議直接重倉。"
        elif regime_key == "STRONG_BULL" and b60["zone"] == "overheated":
            trade_pct = 0
            action = "暫停追買"
            color = WARN_COLOR
            reason = "大多頭仍可續抱，但季線乖離已高，不建議用新資金追價。"
        elif regime_key == "STRONG_BULL":
            trade_pct = base_pct
            reason = "大多頭環境下，買訊可順勢執行，但仍只在訊號首次出現或升級時加碼。"
        elif regime_key == "BULL_PULLBACK":
            trade_pct = base_pct
            reason = "多頭修正中的買訊較有分批布局意義，但仍需保留後續加碼空間。"
        else:
            trade_pct = base_pct
            reason = "盤整環境下依訊號分批，不一次打滿部位。"

    elif direction == "SELL":
        action = "賣出或減碼"
        color = DOWN_COLOR
        if regime_key == "STRONG_BULL":
            trade_pct = {"STRONG": 30, "MID": 10, "WEAK": 0, "NOTICE": 0, "NEUTRAL": 0}.get(level_key, 0)
            reason = "大多頭下弱賣出通常只是震盪提醒；中訊號才小幅降風險，強訊號再明顯減碼。"
        elif regime_key == "BULL_PULLBACK":
            trade_pct = {"STRONG": 40, "MID": 20, "WEAK": 0, "NOTICE": 0, "NEUTRAL": 0}.get(level_key, 0)
            reason = "多頭修正時先守核心持股，弱賣出不急著動作，中強訊號才分批降部位。"
        elif regime_key == "BEAR":
            trade_pct = base_pct
            reason = "空頭環境下賣出訊號可信度提高，可依原始比例控管風險。"
        else:
            trade_pct = base_pct
            reason = "盤整環境下依原始比例分批，避免單日判斷過度影響部位。"

    elif direction == "OVERHEATED":
        action = "禁止追買"
        color = WARN_COLOR
        if level_key in ("MID", "STRONG"):
            if regime_key == "STRONG_BULL":
                trade_pct = 10 if level_key == "MID" else 30
                reason = "行情仍屬大多頭，但已過熱且賣壓分數升高；以小幅停利或降低槓桿為主，不清空核心部位。"
            elif regime_key == "BULL_PULLBACK":
                trade_pct = 20 if level_key == "MID" else 40
                reason = "過熱後進入修正，賣壓分數已不低，可分批降部位並等待下一次整理。"
            else:
                trade_pct = base_pct
                reason = "過熱且賣壓明顯，先降低風險，不新增買進。"
            action = "減碼"
            color = DOWN_COLOR
        else:
            trade_pct = 0
            reason = "過熱代表不追買；但賣出分數還不夠強，若處在多頭中不建議只因過熱就下車。"

    if level_key == "NOTICE":
        trade_pct = 0
        reason = "提醒等級只代表市場溫度有變化，不作為實際交易依據。"

    if lev_warn and trade_pct > 0:
        trade_pct = min(trade_pct, 20)
        reason += " 槓桿ETF波動與耗損較高，單次動作上限先壓低。"

    if trade_pct > 0:
        headline = f"{action} {trade_pct}%"
    elif action == "暫停追買":
        headline = "暫停追買"
    elif action == "禁止追買":
        headline = "禁止追買，核心部位續抱觀察"
    else:
        headline = "不交易，觀察"

    return {
        "headline": headline,
        "action": action,
        "trade_pct": trade_pct,
        "base_pct": base_pct,
        "color": color,
        "reason": reason,
        "regime": regime,
        "repeat_rule": "同一等級訊號連續出現時，不建議每週重複操作；只有首次出現、訊號升級，或部位尚未達計畫比例時才執行。",
    }


def classify_weekly_posture(regime: dict, b60: dict, week_chg_pct: float | None,
                            close: float, ma_s: float, ma_m: float, ma_l: float,
                            effective_buy: float, effective_sell: float) -> tuple[str, str, str]:
    if b60.get("zone") == "overheated":
        return "過熱不追", WARN_COLOR, "趨勢仍可偏多看待，但季線乖離偏高；下週重點是量縮拉回或高檔爆量轉弱。"
    if close > ma_s > ma_m > ma_l and (week_chg_pct or 0) > 0 and effective_buy >= effective_sell:
        return "強勢續抱", UP_COLOR, "價格站在主要均線上方且本週收高；下週觀察能否守住10日線並延續法人買超。"
    if close < ma_m and effective_sell >= max(30, effective_buy):
        return "轉弱觀察", DOWN_COLOR, "收盤跌回中短均線下方且風險分數升溫；下週先看20日線能否重新站回。"
    if week_chg_pct is not None and week_chg_pct < -3 and close >= ma_l:
        return "修正等待", WARN_COLOR, "本週拉回但尚未跌破季線；下週觀察量能是否收斂，以及是否出現止跌K線。"
    if regime.get("key") == "RANGE" or abs(week_chg_pct or 0) < 1.5:
        return "盤整區間", NEUTRAL_COLOR, "本週變化有限或均線方向不明；下週以區間高低點突破或跌破作為方向確認。"
    if effective_buy > effective_sell:
        return "續強觀察", UP_COLOR, "多方條件仍優於風險條件；下週觀察本週高點能否帶量突破。"
    return "修正等待", WARN_COLOR, "風險條件略占上風；下週先觀察支撐與法人賣壓是否收斂。"


def build_weekly_metrics(df: pd.DataFrame, scfg: dict, inst_week: dict | None,
                         regime: dict, b60: dict, effective_buy: float,
                         effective_sell: float) -> dict:
    ma = scfg["ma_periods"]
    s, m, l = ma["short"], ma["mid"], ma["long"]
    latest = df.iloc[-1]
    close = float(latest["Close"])
    week = df.tail(5)
    prev_close = float(df["Close"].iloc[-6]) if len(df) >= 6 else None
    week_chg = close - prev_close if prev_close else None
    week_chg_pct = (week_chg / prev_close * 100) if prev_close else None
    week_high = float(week["High"].max())
    week_low = float(week["Low"].min())
    week_volume = float(week["Volume"].sum())
    avg_volume_20 = float(df["Volume"].tail(20).mean()) if len(df) >= 20 else float(df["Volume"].mean())
    week_avg_volume = week_volume / max(len(week), 1)
    volume_ratio = week_avg_volume / avg_volume_20 if avg_volume_20 else None
    ma_values = {
        f"MA{s}": float(latest[f"MA{s}"]),
        f"MA{m}": float(latest[f"MA{m}"]),
        f"MA{l}": float(latest[f"MA{l}"]),
    }
    ma_position = " / ".join(
        f"{key}{'上' if close >= value else '下'}{(close - value) / value * 100:+.1f}%"
        for key, value in ma_values.items()
    )
    posture, color, next_focus = classify_weekly_posture(
        regime, b60, week_chg_pct, close, ma_values[f"MA{s}"], ma_values[f"MA{m}"], ma_values[f"MA{l}"],
        effective_buy, effective_sell
    )
    range_pos = (close - week_low) / (week_high - week_low) * 100 if week_high != week_low else 50.0
    trend_summary = f"{posture}｜本週{pct_text(week_chg_pct)}，收盤位於本週區間{range_pos:.0f}%"
    inst_total = inst_week.get("total_net") if inst_week and inst_week.get("success") else None
    chart_rows = df.tail(60)
    chart_points = []
    for idx, row in chart_rows.iterrows():
        chart_points.append({
            "date": idx.strftime("%m/%d") if hasattr(idx, "strftime") else str(idx),
            "close": float(row["Close"]),
            "ma_s": float(row[f"MA{s}"]) if pd.notna(row[f"MA{s}"]) else None,
            "ma_m": float(row[f"MA{m}"]) if pd.notna(row[f"MA{m}"]) else None,
            "ma_l": float(row[f"MA{l}"]) if pd.notna(row[f"MA{l}"]) else None,
            "volume": float(row["Volume"]),
        })

    return {
        "week_chg": week_chg,
        "week_chg_pct": week_chg_pct,
        "week_high": week_high,
        "week_low": week_low,
        "week_volume": week_volume,
        "week_avg_volume": week_avg_volume,
        "avg_volume_20": avg_volume_20,
        "volume_ratio": volume_ratio,
        "institutional_week": inst_week,
        "institutional_total": inst_total,
        "institutional_value": inst_value,
        "institutional_value_text": format_twd_billion_short(inst_value),
        "institutional_daily_values": inst_daily_shares,
        "ma_position": ma_position,
        "posture": posture,
        "posture_color": color,
        "trend_summary": trend_summary,
        "next_focus": next_focus,
        "range_pos": range_pos,
        "chart_points": chart_points,
    }


def trade_plan_html(result: dict, compact: bool = False) -> str:
    trade_plan = result.get("trade_plan", {})
    if not trade_plan:
        return ""

    regime = trade_plan.get("regime", result.get("regime", {}))
    signal_badge = (
        f'<span style="background:{result.get("border", NEUTRAL_COLOR)};color:#fff;'
        f'font-size:12px;font-weight:bold;padding:4px 8px;border-radius:5px;'
        f'white-space:nowrap;display:inline-block;margin-right:6px;">'
        f'{result.get("summary", "無訊號")}</span>'
    )
    status_tags = (
        f'<span style="background:{regime.get("color", NEUTRAL_COLOR)};color:#fff;'
        f'font-size:12px;font-weight:bold;padding:4px 8px;border-radius:5px;'
        f'white-space:nowrap;display:inline-block;margin-right:6px;">'
        f'{regime.get("label", "市場狀態不明")}</span>'
    )
    if result.get("b60", {}).get("zone") == "overheated":
        status_tags += (
            f'<span style="background:#c0392b;color:#fff;font-size:12px;'
            f'font-weight:bold;padding:4px 8px;border-radius:5px;white-space:nowrap;'
            f'display:inline-block;margin-right:6px;">過熱鎖定</span>'
        )
    elif result.get("b60", {}).get("zone") == "oversold":
        status_tags += (
            f'<span style="background:#2980b9;color:#fff;font-size:12px;'
            f'font-weight:bold;padding:4px 8px;border-radius:5px;white-space:nowrap;'
            f'display:inline-block;margin-right:6px;">超跌區</span>'
        )

    margin = "margin-top:8px;" if compact else ""
    return (
        f'<div style="{margin}background:#fff;border:1px solid #eee;border-left:5px solid '
        f'{trade_plan.get("color", NEUTRAL_COLOR)};border-radius:8px;padding:10px 12px;">'
        f'<div style="display:flex;gap:6px;flex-wrap:wrap;align-items:center;margin-bottom:7px;">'
        f'{signal_badge}'
        f'{status_tags}'
        f'<span style="color:{trade_plan.get("color", NEUTRAL_COLOR)};'
        f'font-size:15px;font-weight:bold;">{trade_plan.get("headline", "不交易，觀察")}</span>'
        f'</div>'
        f'<div style="font-size:12px;color:#555;line-height:1.7;">'
        f'{trade_plan.get("reason", "")}</div>'
        f'</div>'
    )


def _direction_style(direction: str, level_key: str, locked: bool = False) -> tuple:
    if locked:
        return "🔥", "#fdecea", UP_COLOR
    if direction == "buy":
        return {
            "STRONG": ("🔴", "#fdecea", UP_COLOR),
            "MID": ("🟠", "#fef5e7", WARN_COLOR),
            "WEAK": ("🟡", "#fef9e7", "#f39c12"),
            "NOTICE": ("🔵", "#eaf4fb", INFO_COLOR),
            "NEUTRAL": ("⚪", "#f8f9fa", NEUTRAL_COLOR),
        }.get(level_key, ("⚪", "#f8f9fa", NEUTRAL_COLOR))
    return {
        "STRONG": ("🟢", "#eafaf1", DOWN_COLOR),
        "MID": ("🟣", "#f4ecf7", "#8e44ad"),
        "WEAK": ("🟡", "#f8f9fa", "#7f8c8d"),
        "NOTICE": ("⚪", "#f8f9fa", NEUTRAL_COLOR),
        "NEUTRAL": ("⚪", "#f8f9fa", NEUTRAL_COLOR),
    }.get(level_key, ("⚪", "#f8f9fa", NEUTRAL_COLOR))


def format_market_value(value: float, unit: str = "張") -> str:
    if value > 0:
        return f'<span style="color:{UP_COLOR};font-weight:bold;">買超 {value:.0f}{unit}</span>'
    if value < 0:
        return f'<span style="color:{DOWN_COLOR};font-weight:bold;">賣超 {abs(value):.0f}{unit}</span>'
    return f'平盤 0{unit}'


def format_market_value_text(value: float, unit: str = "張") -> str:
    if value > 0:
        return f"買超 {value:.0f}{unit}"
    if value < 0:
        return f"賣超 {abs(value):.0f}{unit}"
    return f"平盤 0{unit}"


def format_twd_billion(value: float | None) -> str:
    if value is None:
        return "-"
    action = "買超" if value >= 0 else "賣超"
    return f"{action} {abs(value) / 100000000:.1f} 億元台幣"


def format_twd_billion_short(value: float | None) -> str:
    if value is None:
        return "-"
    action = "買超" if value >= 0 else "賣超"
    return f"{action}{abs(value) / 100000000:.1f}億"


def volume_ratio_note(value: float | None) -> str:
    if value is None:
        return "量能資料不足。"
    if value >= 1.3:
        return "週日均量明顯高於20日均量；上漲放量偏多，下跌放量偏風險。"
    if value >= 1.05:
        return "週日均量略高於20日均量；代表本週交易熱度較平常增加。"
    if value >= 0.85:
        return "週日均量接近20日均量；量能屬正常範圍。"
    return "週日均量低於20日均量；通常代表觀望或量縮整理。"


def format_ratio_value(value: float) -> str:
    if value > 0:
        return f'<span style="color:{UP_COLOR};font-weight:bold;">+{value:.2f}%</span>'
    if value < 0:
        return f'<span style="color:{DOWN_COLOR};font-weight:bold;">{value:.2f}%</span>'
    return "0.00%"


# ── 評估訊號 ────────────────────────────────────────────────
def evaluate(df: pd.DataFrame, scfg: dict, inst: dict | None = None) -> dict:
    thr        = scfg["thresholds"]
    ma         = scfg["ma_periods"]
    use_obv    = scfg.get("use_obv", True)
    use_vol    = scfg.get("use_vol_trend", True)
    lev_warn   = scfg.get("leverage_warning", False)
    s, m, l    = ma["short"], ma["mid"], ma["long"]

    latest = df.iloc[-1]
    prev   = df.iloc[-2]

    close     = float(latest["Close"])
    ma_s      = float(latest[f"MA{s}"])
    ma_m      = float(latest[f"MA{m}"])
    ma_l      = float(latest[f"MA{l}"])
    ma_s_prev = float(prev[f"MA{s}"])
    ma_m_prev = float(prev[f"MA{m}"])
    ma_l_prev = float(prev[f"MA{l}"])
    k, d      = float(latest["K"]),        float(latest["D"])
    kp, dp    = float(prev["K"]),          float(prev["D"])
    hist      = float(latest["MACD_hist"])
    hist_p    = float(prev["MACD_hist"])
    bias20    = float(latest["Bias20"])
    vol       = float(latest["Volume"])
    vol_ma    = float(latest["Vol_MA"])
    vol_trend = float(latest["Vol_Trend"])
    obv       = float(latest["OBV"])
    obv_ma    = float(latest["OBV_MA"])
    obv_prev  = float(prev["OBV"])

    items  = []
    l2_buy = l2_sell = 0

    # 槓桿ETF警示標籤
    if lev_warn:
        items.append(("⚠️ 槓桿警示", "每日重置ETF，不適合長抱", "#e67e22",
                      "槓桿ETF有長期耗損效應，僅適合短線波段操作"))

    # ── BIAS60 Z-Score ────────────────────────────────────────
    b60 = eval_bias60(df, scfg)
    items.append(("BIAS60 Z-Score", b60["label"], b60["color"], b60["note"]))

    # ── 第一層：趨勢環境 ──────────────────────────────────────
    ma_s_dir   = ma_s > ma_s_prev
    above_ma_s = close > ma_s

    if ma_m > ma_l and above_ma_s and ma_s_dir:     trend = "healthy_bull"
    elif ma_m > ma_l and (not above_ma_s or not ma_s_dir): trend = "weak_bull"
    elif ma_m < ma_l:                                trend = "bear"
    else:                                            trend = "neutral"

    trend_label = {"healthy_bull":"多頭健康","weak_bull":"多頭轉弱",
                   "bear":"空頭確認","neutral":"方向不明"}[trend]
    trend_color = {"healthy_bull":"#2ecc71","weak_bull":"#f39c12",
                   "bear":"#e74c3c","neutral":"#95a5a6"}[trend]
    items.append(("趨勢環境", trend_label, trend_color,
                  f"MA{s}={ma_s:.1f}｜MA{m}={ma_m:.1f}｜MA{l}={ma_l:.1f}｜"
                  f"收盤{'站上' if above_ma_s else '跌破'}{s}日線（{s}日線{'向上' if ma_s_dir else '向下'}）｜"
                  f"多頭健康：MA{m}>MA{l} 且收盤站上{s}日線｜"
                  f"多頭轉弱：MA{m}>MA{l} 但跌破{s}日線或{s}日線轉向｜"
                  f"空頭確認：MA{m}<MA{l}"))
    # ── 第二層：時機指標 ──────────────────────────────────────

    # MACD
    # 計算歷史MACD柱狀範圍供參考
    hist_series = df["MACD_hist"].dropna()
    hist_p10 = float(hist_series.quantile(0.10))
    hist_p90 = float(hist_series.quantile(0.90))
    macd_range_note = f"當前={hist:.4f}｜歷史正常區間[{hist_p10:.4f}～{hist_p90:.4f}]｜正=多頭動能，負=空頭動能，0軸為中性"
    if hist > 0 and hist_p <= 0:
        l2_buy += 1
        items.append(("MACD", "柱狀由負翻正 ✅", "#2ecc71", macd_range_note + "｜剛翻正，動能轉強"))
    elif hist < 0 and hist_p >= 0:
        l2_sell += 1
        items.append(("MACD", "柱狀由正翻負 ⚠️", "#e74c3c", macd_range_note + "｜剛翻負，動能轉弱"))
    else:
        sign = "正（多頭）" if hist > 0 else "負（空頭）"
        items.append(("MACD", f"柱狀持續為{sign}", "#95a5a6", macd_range_note))

    # KD（使用個股門檻）
    kd_buy  = k > d and kp <= dp and k < thr["kd_buy"]
    kd_sell = k < d and kp >= dp and k > thr["kd_sell"]
    kd_note = (f"當前 K={k:.1f} D={d:.1f}｜"
               f"買進區：K<{thr['kd_buy']}且K上穿D｜"
               f"賣出區：K>{thr['kd_sell']}且K下穿D｜"
               f"正常區間：{thr['kd_buy']}～{thr['kd_sell']}")
    if kd_buy:
        l2_buy += 1
        items.append(("KD", "低檔黃金交叉 ✅", "#2ecc71", kd_note))
    elif kd_sell:
        l2_sell += 1
        items.append(("KD", "高檔死亡交叉 ⚠️", "#e74c3c", kd_note))
    else:
        items.append(("KD", "無交叉訊號", "#95a5a6", kd_note))

    # 短線乖離率（使用個股門檻）
    b20_buy  = thr.get("bias20_buy",  thr.get("bias_buy",  -4.0))
    b20_sell = thr.get("bias20_sell", thr.get("bias_sell",  5.0))
    bias20_note = (f"當前={bias20:.2f}%（收盤偏離MA{m}的幅度）｜"
                   f"正常區間：{b20_buy}%～+{b20_sell}%｜"
                   f"低於{b20_buy}%=跌深買進區，高於+{b20_sell}%=漲多賣出區")
    if bias20 < b20_buy:
        l2_buy += 1
        items.append(("乖離率(MA{})".format(m), "跌深反彈機會 ✅", "#2ecc71", bias20_note))
    elif bias20 > b20_sell:
        l2_sell += 1
        items.append(("乖離率(MA{})".format(m), "漲幅過高警示 ⚠️", "#e74c3c", bias20_note))
    else:
        items.append(("乖離率(MA{})".format(m), "正常範圍", "#95a5a6", bias20_note))

    # 均線交叉
    ma_bull = ma_m > ma_l and ma_m_prev <= ma_l_prev
    ma_bear = ma_m < ma_l and ma_m_prev >= ma_l_prev
    ma_note = (f"MA{s}={ma_s:.1f}｜MA{m}={ma_m:.1f}｜MA{l}={ma_l:.1f}｜"
               f"MA{m}>MA{l}=多頭排列，MA{m}<MA{l}=空頭排列｜"
               f"剛發生交叉才觸發訊號，持續排列為中性")
    if ma_bull:
        l2_buy += 1
        items.append(("均線交叉", f"MA{m}上穿MA{l} ✅", "#2ecc71", ma_note + "｜趨勢剛確立"))
    elif ma_bear:
        l2_sell += 1
        items.append(("均線交叉", f"MA{m}下穿MA{l} ⚠️", "#e74c3c", ma_note + "｜趨勢剛反轉"))
    else:
        rel = ">" if ma_m > ma_l else "<"
        status = "多頭排列持續" if ma_m > ma_l else "空頭排列持續"
        items.append(("均線交叉", status, "#95a5a6", ma_note))

    # 量能趨勢（可關閉）
    vol_ratio = vol / vol_ma if vol_ma > 0 else 1
    if use_vol:
        vol_note = (f"最新成交量／{thr['vol_ma_period']}日均量={vol_ratio:.2f}倍｜"
                    f"正常範圍：0.8～1.2倍｜"
                    f">1.2倍且價漲=量能擴張買訊，<0.8倍=量能萎縮警示")
        if vol_trend > 0 and vol_ratio > 1.2:
            vol_label, vol_color = "量能擴張 ✅", "#2ecc71"
            if close > float(prev["Close"]): l2_buy += 1
        elif vol_trend < 0 and vol_ratio < 0.8:
            vol_label, vol_color = "量能萎縮 ⚠️", "#e74c3c"
        else:
            vol_label, vol_color = "量能平穩", "#95a5a6"
        items.append(("量能趨勢", vol_label, vol_color, vol_note))
    else:
        items.append(("量能趨勢", "已關閉（槓桿ETF不適用）", "#bdc3c7",
                      "槓桿ETF成交量主要來自當沖套利，無法反映真實多空"))

    # OBV（可關閉）
    if use_obv:
        obv_rising  = obv > obv_ma and obv > obv_prev
        obv_falling = obv < obv_ma and obv < obv_prev
        price_up    = close > float(prev["Close"])
        obv_note = (f"OBV={'高於' if obv>obv_ma else '低於'}{thr['obv_ma_period']}日均線｜"
                    f"OBV持續累積=買盤入場，OBV持續下滑=賣盤出場｜"
                    f"OBV領先價格=強力買訊，價漲OBV跌=背離警示")
        if obv_rising and price_up:
            obv_label, obv_color = "量價齊揚 ✅",    "#2ecc71"; l2_buy  += 1
        elif obv_rising and not price_up:
            obv_label, obv_color = "OBV領先價格", "#3498db"
        elif obv_falling and not price_up:
            obv_label, obv_color = "量價齊跌 ⚠️",   "#e74c3c"; l2_sell += 1
        elif obv_falling and price_up:
            obv_label, obv_color = "價漲量縮背離 ⚠️","#f39c12"
        else:
            obv_label, obv_color = "OBV中性",        "#95a5a6"
        items.append(("OBV", obv_label, obv_color, obv_note))
    else:
        items.append(("OBV", "已關閉（槓桿ETF不適用）", "#bdc3c7",
                      "槓桿ETF成交量結構特殊，OBV訊號不具參考價值"))

    # 價格行為
    is_red   = close > float(latest["Open"])
    open_p   = float(latest["Open"])
    chg_pct  = (close - open_p) / open_p * 100
    price_note = (f"開盤={open_p:.2f}｜收盤={close:.2f}｜當日漲跌={chg_pct:+.2f}%｜"
                  f"紅K：收盤>開盤，買方強勢｜黑K：收盤<開盤，賣方強勢｜"
                  f"長上影線：上漲被壓回，賣壓重｜長下影線：下跌被撐回，買盤強")
    items.append(("價格行為",
                  f"紅K（+{chg_pct:.2f}%）" if is_red else f"黑K（{chg_pct:.2f}%）",
                  "#2ecc71" if is_red else "#e74c3c",
                  price_note))

    # ── 綜合訊號 ──────────────────────────────────────────────
    if b60["locked"]:
        if l2_sell >= 2 or trend == "bear":
            level, emoji, summary = "STRONG_SELL", "🔵", "強賣出訊號"
            advice = f"市場過熱且技術面轉弱，建議出場"
            bg, border = "#eaf4fb", "#3498db"
        else:
            level, emoji, summary = "OVERHEATED", "🔥", "過熱鎖定｜禁止追買"
            advice = (f"季線乖離{b60['bias60']:.1f}%超過歷史{scfg['thresholds'].get('bias60_p_high',95)}%分位"
                      f"({b60['p_high']:.1f}%)，Z={b60['z_score']:.2f}，強制停止買進")
            bg, border = "#fdecea", "#c0392b"

    elif b60["zone"] == "oversold":
        if trend in ("healthy_bull","weak_bull") and l2_buy >= 1:
            level, emoji, summary = "STRONG_BUY", "🔴", "強買進訊號（超跌加碼區）"
            advice = (f"季線乖離{b60['bias60']:.1f}%低於歷史{scfg['thresholds'].get('bias60_p_low',5)}%分位"
                      f"({b60['p_low']:.1f}%)，統計超跌，高信心建倉機會")
            bg, border = "#fdecea", "#e74c3c"
        else:
            level, emoji, summary = "WEAK_BUY", "🟡", "超跌觀察區"
            advice = "季線乖離統計超跌，但技術面尚未確認，可列入觀察"
            bg, border = "#fef9e7", "#f39c12"

    else:
        if trend == "healthy_bull" and l2_buy >= 2:
            level, emoji, summary = "STRONG_BUY",  "🔴", "強買進訊號"
            advice = "多頭健康，多指標共振，建議關注進場機會"
            bg, border = "#fdecea", "#e74c3c"
        elif (trend == "healthy_bull" and l2_buy == 1) or \
             (trend == "weak_bull"    and l2_buy >= 2):
            level, emoji, summary = "WEAK_BUY",    "🟡", "弱買進提醒"
            advice = "單一訊號或趨勢轉弱，列入觀察，勿躁進"
            bg, border = "#fef9e7", "#f39c12"
        elif trend in ("weak_bull","healthy_bull") and not ma_s_dir:
            level, emoji, summary = "WARNING",     "🟠", "風險警示"
            advice = f"{s}日線走弱，建議降低部位或暫緩操作"
            bg, border = "#fef5e7", "#e67e22"
        elif trend == "bear" and l2_sell >= 2:
            level, emoji, summary = "STRONG_SELL", "🔵", "強賣出訊號"
            advice = "空頭確認，多指標共振，建議考慮出場"
            bg, border = "#eaf4fb", "#3498db"
        elif trend == "neutral":
            level, emoji, summary = "NEUTRAL",     "⚪", "方向不明"
            advice = "均線糾結或訊號矛盾，建議觀望"
            bg, border = "#f8f9fa", "#95a5a6"
        else:
            level, emoji, summary = "NEUTRAL",     "⚪", "無明顯訊號"
            advice = "目前無強烈進出依據，繼續觀察"
            bg, border = "#f8f9fa", "#95a5a6"

    pyramid = calc_pyramid(df, scfg, level)

    return dict(
        level=level, emoji=emoji, summary=summary, advice=advice,
        bg=bg, border=border, items=items,
        close=close, bias20=bias20, is_red=is_red,
        l2_buy=l2_buy, l2_sell=l2_sell,
        b60=b60, pyramid=pyramid,
    )


def evaluate_weighted(df: pd.DataFrame, scfg: dict, inst: dict | None = None,
                      macro: dict | None = None, inst_week: dict | None = None) -> dict:
    thr = scfg["thresholds"]
    ma = scfg["ma_periods"]
    use_obv = scfg.get("use_obv", True)
    use_vol = scfg.get("use_vol_trend", True)
    use_inst = scfg.get("use_institutional", True)
    use_fx = scfg.get("use_fx", True)
    use_rates = scfg.get("use_rates", True)
    macro_sensitivity = scfg.get("macro_sensitivity", "market")
    lev_warn = scfg.get("leverage_warning", False)
    s, m, l = ma["short"], ma["mid"], ma["long"]

    latest = df.iloc[-1]
    prev = df.iloc[-2]

    close = float(latest["Close"])
    prev_close = float(prev["Close"])
    ma_s = float(latest[f"MA{s}"])
    ma_m = float(latest[f"MA{m}"])
    ma_l = float(latest[f"MA{l}"])
    ma_s_prev = float(prev[f"MA{s}"])
    ma_m_prev = float(prev[f"MA{m}"])
    ma_l_prev = float(prev[f"MA{l}"])
    k, d = float(latest["K"]), float(latest["D"])
    kp, dp = float(prev["K"]), float(prev["D"])
    hist = float(latest["MACD_hist"])
    hist_p = float(prev["MACD_hist"])
    bias20 = float(latest["Bias20"])
    vol = float(latest["Volume"])
    vol_ma = float(latest["Vol_MA"])
    vol_trend = float(latest["Vol_Trend"])
    obv = float(latest["OBV"])
    obv_ma = float(latest["OBV_MA"])
    obv_prev = float(prev["OBV"])

    items = []
    buy_score = 0.0
    sell_score = 0.0
    max_possible = float(sum(WEIGHTS.values()))

    def add_item(label, value, color, note, buy=0.0, sell=0.0):
        nonlocal buy_score, sell_score
        buy_score += buy
        sell_score += sell
        if buy or sell:
            note = f"{note}｜分數影響:買進+{buy:.0f}/賣出+{sell:.0f}"
        items.append((label, value, color, note))

    if lev_warn:
        add_item("⚠️ 槓桿警示", "每日重置ETF，不適合長抱", "#e67e22",
                 "槓桿ETF有長期耗損效應，僅適合短線波段操作")

    b60 = eval_bias60(df, scfg)
    add_item("BIAS60 Z-Score", b60["label"], b60["color"],
             b60["note"] + "｜用途:判斷中期位置是否過熱或超跌；過熱時不建議追買")

    ma_s_dir = ma_s > ma_s_prev
    above_ma_s = close > ma_s
    if ma_m > ma_l and above_ma_s and ma_s_dir:
        trend = "healthy_bull"
        trend_label, trend_color = "多頭健康", DOWN_COLOR
        trend_buy, trend_sell = WEIGHTS["trend"], 0
    elif ma_m > ma_l and (not above_ma_s or not ma_s_dir):
        trend = "weak_bull"
        trend_label, trend_color = "多頭轉弱", "#f39c12"
        trend_buy, trend_sell = 0, WEIGHTS["trend"] * 0.4
    elif ma_m < ma_l:
        trend = "bear"
        trend_label, trend_color = "空頭確認", UP_COLOR
        trend_buy, trend_sell = 0, WEIGHTS["trend"]
    else:
        trend = "neutral"
        trend_label, trend_color = "方向不明", NEUTRAL_COLOR
        trend_buy = trend_sell = 0
    add_item(
        "趨勢環境", trend_label, trend_color,
        f"MA{s}={ma_s:.1f}｜MA{m}={ma_m:.1f}｜MA{l}={ma_l:.1f}｜"
        f"收盤{'站上' if above_ma_s else '跌破'}{s}日線（{s}日線{'向上' if ma_s_dir else '向下'}）｜"
        f"趨勢代表目前市場主方向，是本模型最重要的判斷項目｜均線交叉已包含在趨勢判斷中，不重複加分",
        trend_buy, trend_sell,
    )
    regime = classify_market_regime(close, ma_s, ma_m, ma_l, ma_s_prev, ma_m_prev, ma_l_prev)

    if use_fx:
        fx = macro.get("fx") if macro else None
        if fx:
            fx_5d = fx.get("chg_5d_pct")
            fx_20d = fx.get("chg_20d_pct")
            fx_value = fx["value"]
            fx_note = (
                f"美元/台幣={fx_value:.3f}｜5日變動={fx_5d:+.2f}%｜20日變動={fx_20d:+.2f}%｜"
                "數字變高代表美元變貴、台幣轉弱；台幣快速貶值常伴隨外資撤出壓力，"
                "但對台積電、聯發科等出口股有部分匯兌抵銷"
            )
            exporter = macro_sensitivity == "exporter"
            full = WEIGHTS["fx"] * (0.75 if exporter else 1.0)
            half = full * 0.5
            if fx_5d is not None and fx_20d is not None and (fx_5d >= 1.0 or fx_20d >= 2.0):
                add_item("美元/台幣匯率", "台幣明顯轉弱 ⚠️", "#e67e22", fx_note, 0, full)
            elif fx_5d is not None and fx_20d is not None and (fx_5d <= -1.0 or fx_20d <= -2.0):
                add_item("美元/台幣匯率", "台幣明顯轉強 ✅", UP_COLOR, fx_note, full, 0)
            elif fx_5d is not None and fx_20d is not None and (fx_5d >= 0.5 or fx_20d >= 1.0):
                add_item("美元/台幣匯率", "台幣偏弱", "#f39c12", fx_note, 0, half)
            elif fx_5d is not None and fx_20d is not None and (fx_5d <= -0.5 or fx_20d <= -1.0):
                add_item("美元/台幣匯率", "台幣偏強", "#3498db", fx_note, half, 0)
            else:
                add_item("美元/台幣匯率", "匯率中性", NEUTRAL_COLOR, fx_note)
        else:
            reason = "；".join(macro.get("errors", [])) if macro else "未取得總體資料"
            add_item("美元/台幣匯率", "資料暫不可用", "#bdc3c7",
                     f"{reason}｜不計分，避免資料源異常影響判斷")
    else:
        add_item("美元/台幣匯率", "已關閉", "#bdc3c7", "此標的不使用匯率權重")

    if use_rates:
        rates = macro.get("rates") if macro else None
        if rates:
            rate_value = rates["value"]
            bp_5d = rates.get("chg_5d_bp")
            bp_20d = rates.get("chg_20d_bp")
            rate_note = (
                f"美國10年期殖利率={rate_value:.2f}%｜5日變動={bp_5d:+.0f}bp｜20日變動={bp_20d:+.0f}bp｜"
                "殖利率上升會提高股市折現率，通常壓抑科技股評價；殖利率下行則有利成長股估值修復"
            )
            if bp_5d is not None and bp_20d is not None and (bp_5d >= 10 or bp_20d >= 20):
                add_item("利率環境", "殖利率快速上升 ⚠️", DOWN_COLOR, rate_note, 0, WEIGHTS["rates"])
            elif bp_5d is not None and bp_20d is not None and (bp_5d <= -10 or bp_20d <= -20):
                add_item("利率環境", "殖利率明顯下行 ✅", UP_COLOR, rate_note, WEIGHTS["rates"], 0)
            elif bp_5d is not None and bp_20d is not None and (bp_5d >= 5 or bp_20d >= 10):
                add_item("利率環境", "利率偏上行", "#f39c12", rate_note, 0, WEIGHTS["rates"] * 0.5)
            elif bp_5d is not None and bp_20d is not None and (bp_5d <= -5 or bp_20d <= -10):
                add_item("利率環境", "利率偏下行", "#3498db", rate_note, WEIGHTS["rates"] * 0.5, 0)
            else:
                add_item("利率環境", "利率中性", NEUTRAL_COLOR, rate_note)
        else:
            reason = "；".join(macro.get("errors", [])) if macro else "未取得總體資料"
            add_item("利率環境", "資料暫不可用", "#bdc3c7",
                     f"{reason}｜不計分，避免資料源異常影響判斷")
    else:
        add_item("利率環境", "已關閉", "#bdc3c7", "此標的不使用利率權重")

    hist_series = df["MACD_hist"].dropna()
    hist_p10 = float(hist_series.quantile(0.10))
    hist_p90 = float(hist_series.quantile(0.90))
    macd_note = f"當前={hist:.4f}｜歷史正常區間[{hist_p10:.4f}～{hist_p90:.4f}]｜正=多頭動能，負=空頭動能"
    if hist > 0 and hist_p <= 0:
        add_item("MACD", "柱狀由負翻正 ✅", UP_COLOR, macd_note + "｜剛翻正，動能轉強", WEIGHTS["macd"], 0)
    elif hist < 0 and hist_p >= 0:
        add_item("MACD", "柱狀由正翻負 ⚠️", DOWN_COLOR, macd_note + "｜剛翻負，動能轉弱", 0, WEIGHTS["macd"])
    elif hist > 0 and hist > hist_p:
        add_item("MACD", "多頭動能延續", UP_COLOR, macd_note + "｜動能仍改善", WEIGHTS["macd"] * 0.5, 0)
    elif hist < 0 and hist < hist_p:
        add_item("MACD", "空頭動能延續", DOWN_COLOR, macd_note + "｜動能仍惡化", 0, WEIGHTS["macd"] * 0.5)
    else:
        sign = "正（多頭）" if hist > 0 else "負（空頭）"
        add_item("MACD", f"柱狀持續為{sign}", NEUTRAL_COLOR, macd_note)

    avg_vol20 = float(df["Volume"].tail(20).mean())
    if use_inst:
        if inst and inst.get("success"):
            total_net = float(inst["total_net"])
            net_ratio = total_net / avg_vol20 * 100 if avg_vol20 > 0 else 0.0
            nets = [inst["foreign_net"], inst["invest_net"], inst["dealer_net"]]
            buy_breadth = sum(1 for n in nets if n > 0)
            sell_breadth = sum(1 for n in nets if n < 0)
            inst_note = (
                f"資料日={inst['date']}｜"
                f"外資 {format_market_value(inst['foreign_net']/1000)}｜"
                f"投信 {format_market_value(inst['invest_net']/1000)}｜"
                f"自營 {format_market_value(inst['dealer_net']/1000)}｜"
                f"合計 {format_market_value(total_net/1000)}｜"
                f"占20日均量 {format_ratio_value(net_ratio)}"
            )
            if net_ratio >= 5 and buy_breadth >= 2:
                add_item("三大法人", "法人明顯買超 ✅", UP_COLOR, inst_note, WEIGHTS["institutional"], 0)
            elif net_ratio <= -5 and sell_breadth >= 2:
                add_item("三大法人", "法人明顯賣超 ⚠️", DOWN_COLOR, inst_note, 0, WEIGHTS["institutional"])
            elif net_ratio > 1 or buy_breadth >= 2:
                add_item("三大法人", "法人偏買", UP_COLOR, inst_note, WEIGHTS["institutional"] * 0.5, 0)
            elif net_ratio < -1 or sell_breadth >= 2:
                add_item("三大法人", "法人偏賣", DOWN_COLOR, inst_note, 0, WEIGHTS["institutional"] * 0.5)
            else:
                add_item("三大法人", "籌碼中性", NEUTRAL_COLOR, inst_note)
        else:
            reason = inst.get("error", "未取得資料") if inst else "未取得資料"
            add_item("三大法人", "資料暫不可用", "#bdc3c7",
                     f"{reason}｜不計分，避免資料源異常影響整體判斷")
    else:
        add_item("三大法人", "已關閉（此標的不適用）", "#bdc3c7",
                 "此標的無法直接使用個股三大法人買賣超，避免用錯資料來源")

    kd_buy = k > d and kp <= dp and k < thr["kd_buy"]
    kd_sell = k < d and kp >= dp and k > thr["kd_sell"]
    kd_note = (
        f"當前 K={k:.1f} D={d:.1f}｜買進區:K<{thr['kd_buy']}且K上穿D｜"
        f"賣出區:K>{thr['kd_sell']}且K下穿D｜KD適合抓時機，但容易鈍化"
    )
    if kd_buy:
        add_item("KD", "低檔黃金交叉 ✅", UP_COLOR, kd_note, WEIGHTS["kd"], 0)
    elif kd_sell:
        add_item("KD", "高檔死亡交叉 ⚠️", DOWN_COLOR, kd_note, 0, WEIGHTS["kd"])
    elif k > d and k < 50:
        add_item("KD", "低檔轉強但未交叉", "#3498db", kd_note, WEIGHTS["kd"] * 0.4, 0)
    elif k < d and k > 50:
        add_item("KD", "高檔轉弱但未交叉", "#f39c12", kd_note, 0, WEIGHTS["kd"] * 0.4)
    else:
        add_item("KD", "無交叉訊號", NEUTRAL_COLOR, kd_note)

    ma_bull = ma_m > ma_l and ma_m_prev <= ma_l_prev
    ma_bear = ma_m < ma_l and ma_m_prev >= ma_l_prev
    ma_note = (
        f"MA{s}={ma_s:.1f}｜MA{m}={ma_m:.1f}｜MA{l}={ma_l:.1f}｜"
        f"這項只說明均線是否剛轉向；分數已在趨勢環境反映，不另外加分"
    )
    if ma_bull:
        add_item("均線交叉", f"MA{m}上穿MA{l} ✅", UP_COLOR, ma_note)
    elif ma_bear:
        add_item("均線交叉", f"MA{m}下穿MA{l} ⚠️", DOWN_COLOR, ma_note)
    else:
        status = "多頭排列持續" if ma_m > ma_l else "空頭排列持續"
        add_item("均線交叉", status, NEUTRAL_COLOR, ma_note)

    vol_ratio = vol / vol_ma if vol_ma > 0 else 1
    if use_vol:
        vol_note = (
            f"最新成交量/{thr['vol_ma_period']}日均量={vol_ratio:.2f}倍｜"
            f"量能是確認項，權重較低"
        )
        if vol_trend > 0 and vol_ratio > 1.2 and close > prev_close:
            add_item("量能趨勢", "價漲量增 ✅", UP_COLOR, vol_note, WEIGHTS["vol"], 0)
        elif vol_trend > 0 and vol_ratio > 1.2 and close < prev_close:
            add_item("量能趨勢", "價跌量增 ⚠️", DOWN_COLOR, vol_note, 0, WEIGHTS["vol"])
        elif vol_trend < 0 and vol_ratio < 0.8 and close < prev_close:
            add_item("量能趨勢", "價跌量縮", "#f39c12", vol_note, 0, WEIGHTS["vol"] * 0.4)
        else:
            add_item("量能趨勢", "量能平穩", NEUTRAL_COLOR, vol_note)
    else:
        add_item("量能趨勢", "已關閉（此標的不適用）", "#bdc3c7",
                 "此標的成交量資料不適合直接作為多空分數")

    if use_obv:
        obv_rising = obv > obv_ma and obv > obv_prev
        obv_falling = obv < obv_ma and obv < obv_prev
        price_up = close > prev_close
        obv_note = (
            f"OBV={'高於' if obv > obv_ma else '低於'}{thr['obv_ma_period']}日均線｜"
            f"OBV可觀察量價累積，但雜訊高於趨勢與MACD"
        )
        if obv_rising and price_up:
            add_item("OBV", "量價齊揚 ✅", UP_COLOR, obv_note, WEIGHTS["obv"], 0)
        elif obv_rising and not price_up:
            add_item("OBV", "OBV領先價格", "#3498db", obv_note, WEIGHTS["obv"] * 0.5, 0)
        elif obv_falling and not price_up:
            add_item("OBV", "量價齊跌 ⚠️", DOWN_COLOR, obv_note, 0, WEIGHTS["obv"])
        elif obv_falling and price_up:
            add_item("OBV", "價漲量縮背離 ⚠️", "#f39c12", obv_note, 0, WEIGHTS["obv"] * 0.5)
        else:
            add_item("OBV", "OBV中性", NEUTRAL_COLOR, obv_note)
    else:
        add_item("OBV", "已關閉（此標的不適用）", "#bdc3c7",
                 "此標的成交量結構不適合用OBV作為主要判斷")

    is_red = close > float(latest["Open"])
    open_p = float(latest["Open"])
    chg_pct = (close - open_p) / open_p * 100
    price_note = (
        f"開盤={open_p:.2f}｜收盤={close:.2f}｜當日漲跌={chg_pct:+.2f}%｜"
        f"只用來輔助理解今天盤勢，不直接加分"
    )
    add_item("價格行為",
             f"紅K（+{chg_pct:.2f}%）" if is_red else f"黑K（{chg_pct:.2f}%）",
             UP_COLOR if is_red else DOWN_COLOR, price_note)

    effective_buy = 0.0 if b60["locked"] else buy_score
    effective_sell = sell_score
    if b60["locked"]:
        level_key, level_label = score_to_signal(effective_sell)
        level, emoji = f"OVERHEATED_{level_key}", "🔥"
        if effective_sell >= 15:
            summary = f"過熱鎖定｜賣出{level_label}({effective_sell:.0f}/{max_possible:.0f}分)"
        else:
            summary = "過熱鎖定｜禁止追買"
        advice = (
            f"季線乖離{b60['bias60']:.1f}%超過歷史門檻，"
            f"原始買進分數{buy_score:.0f}分僅供參考，實際買進分數歸零"
        )
        bg, border = "#fdecea", UP_COLOR
    elif effective_buy >= effective_sell:
        score = effective_buy
        level_key, level_label = score_to_signal(score)
        emoji, bg, border = _direction_style("buy", level_key)
        level = f"BUY_{level_key}"
        prefix = "超跌買進" if b60["zone"] == "oversold" and score >= 15 else "買進"
        summary = f"{emoji} {prefix}{level_label}({score:.0f}/{max_possible:.0f}分)"
        advice = {
            "STRONG": "多項高權重指標共振，可依金字塔計畫分批執行",
            "MID": "訊號有一定一致性，可考慮小部位或分批試單",
            "WEAK": "值得關注，但仍需等待更多確認",
            "NOTICE": "微弱買進跡象，僅列入觀察",
            "NEUTRAL": "買進依據不足，繼續觀察",
        }[level_key]
    else:
        score = effective_sell
        level_key, level_label = score_to_signal(score)
        emoji, bg, border = _direction_style("sell", level_key)
        level = f"SELL_{level_key}"
        summary = f"{emoji} 賣出{level_label}({score:.0f}/{max_possible:.0f}分)"
        advice = {
            "STRONG": "多項高權重風險指標共振，應優先控管部位風險",
            "MID": "賣出訊號有一定一致性，持有者應提高警覺",
            "WEAK": "風險升溫，可檢查停損或降低追價",
            "NOTICE": "微弱賣出跡象，僅列入觀察",
            "NEUTRAL": "賣出依據不足，繼續觀察",
        }[level_key]

    trade_plan = build_trade_plan(level, regime, b60, lev_warn)
    pyramid = calc_pyramid(df, scfg, level)
    weekly = build_weekly_metrics(df, scfg, inst_week, regime, b60, effective_buy, effective_sell)

    add_item(
        "本週變化",
        f"{pct_text(weekly['week_chg_pct'])}｜高{weekly['week_high']:.2f} / 低{weekly['week_low']:.2f}",
        UP_COLOR if (weekly["week_chg_pct"] or 0) >= 0 else DOWN_COLOR,
        f"本週收盤變化={weekly['week_chg']:+.2f}｜5日漲跌幅={pct_text(weekly['week_chg_pct'])}｜用來看本週價格方向"
    )
    add_item(
        "週成交量",
        f"{weekly['week_volume'] / 1000:.0f}千股｜均量比{weekly['volume_ratio']:.2f}x" if weekly["volume_ratio"] else f"{weekly['week_volume'] / 1000:.0f}千股",
        UP_COLOR if (weekly["volume_ratio"] or 0) >= 1.15 else NEUTRAL_COLOR,
        f"本週日均量={weekly['week_avg_volume']:.0f}｜20日均量={weekly['avg_volume_20']:.0f}｜量能放大代表趨勢確認度提高"
    )
    if inst_week and inst_week.get("success"):
        inst_color = UP_COLOR if inst_week["total_net"] >= 0 else DOWN_COLOR
        inst_shares = format_market_value(inst_week["total_net"] / 1000)
        inst_amount = format_twd_billion(weekly.get("institutional_value"))
        inst_value = f"{inst_amount}｜{inst_shares}"
        inst_note = f"本週三大法人個股買賣超張數來自證交所 T86；金額為張數乘以收盤價的約略估算｜統計{inst_week.get('days', 0)}個交易日｜{inst_week.get('date_range', '')}"
    else:
        inst_color = NEUTRAL_COLOR
        inst_value = "不適用或未取得"
        inst_note = (inst_week or {}).get("error", "本週法人資料未取得")
    add_item("本週三大法人", inst_value, inst_color, inst_note)
    add_item("均線位置", weekly["ma_position"], weekly["posture_color"], "觀察收盤價相對10/20/60日線的位置，判斷續強、轉弱或盤整")
    add_item("本週趨勢總結", weekly["trend_summary"], weekly["posture_color"], "週報偏向中短線趨勢追蹤，不作為每日買賣提醒")
    add_item("下週觀察", weekly["next_focus"], weekly["posture_color"], "下週以關鍵均線、本週高低點、量能與法人買賣超是否延續作為觀察重點")

    return dict(
        level=level, emoji=emoji, summary=summary, advice=advice,
        bg=bg, border=border, items=items,
        close=close, bias20=bias20, is_red=is_red,
        buy_score=buy_score, sell_score=sell_score,
        effective_buy=effective_buy, effective_sell=effective_sell,
        score_note="季線乖離過熱，買進分數已鎖定" if b60["locked"] else "",
        max_possible=max_possible, b60=b60, regime=regime,
        trade_plan=trade_plan, pyramid=pyramid, weekly=weekly,
    )


# ── 產生單檔 HTML 區塊 ───────────────────────────────────────
def stock_html_block(name: str, ticker: str, result: dict, note: str = "") -> str:
    rows = ""
    for idx, (label, value, color, n) in enumerate(result["items"]):
        # 把備註用｜切開，每段變成一個編號子項目
        parts = [p.strip() for p in n.split("｜") if p.strip()]
        note_items = "".join(
            f'<span style="display:block;margin:1px 0;">'
            f'<span style="color:#aaa;margin-right:4px;">{i+1}.</span>{p}</span>'
            for i, p in enumerate(parts)
        )
        bg_row = "#fafafa" if idx % 2 == 0 else "#ffffff"
        rows += (
            f'<tr style="background:{bg_row};border-bottom:1px solid #eee;">'
            f'<td style="padding:10px 12px;color:#555;width:22%;font-size:13px;'
            f'font-weight:bold;vertical-align:top;line-height:1.5;">{label}</td>'
            f'<td style="padding:8px 10px;font-weight:bold;color:{color};'
            f'font-size:13px;vertical-align:top;line-height:1.5;width:25%;">{value}</td>'
            f'<td style="padding:10px 12px;color:#666;font-size:12px;'
            f'line-height:1.6;vertical-align:top;">{note_items}</td>'
            f'</tr>'
        )

    note_html = ""
    if note:
        note_html = (f'<div style="background:#fef9e7;padding:8px 16px;'
                     f'font-size:12px;color:#7d6608;border-bottom:1px solid #eee;">'
                     f'{note}</div>')

    trade_html = (
        f'<div style="background:#fff;padding:12px 16px;border-bottom:1px solid #eee;">'
        f'{trade_plan_html(result)}</div>'
    )

    pyramid_html = ""
    if result["pyramid"]["suggestions"]:
        sugg = "".join(f'<li style="margin:4px 0;font-size:13px;">{s}</li>'
                       for s in result["pyramid"]["suggestions"])
        pyramid_html = (f'<div style="background:#f0f8ff;padding:12px 16px;border-top:1px solid #d6eaf8;">'
                        f'<div style="font-weight:bold;color:#2471a3;margin-bottom:6px;">🏗️ 金字塔建倉建議</div>'
                        f'<ul style="margin:0;padding-left:18px;">{sugg}</ul></div>')

    return (
        f'<div style="margin-bottom:28px;border:2px solid {result["border"]};'
        f'border-radius:10px;overflow:hidden;background:#fff;">'
        # 標題列
        f'<div style="background:{result["border"]};padding:12px 16px;'
        f'display:flex;justify-content:space-between;align-items:center;">'
        f'<span style="color:#fff;font-size:16px;font-weight:bold;">'
        f'{result["emoji"]} {name} ({ticker.replace(".TW","").replace(".tw","")})</span>'
        f'<span style="color:#fff;font-size:20px;font-weight:bold;">{result["close"]:.2f}</span>'
        f'</div>'
        # 個股備註
        f'{note_html}'
        # 實際交易建議
        f'{trade_html}'
        # 指標明細表格
        f'<table style="width:100%;border-collapse:collapse;">{rows}</table>'
        # 金字塔建議
        f'{pyramid_html}</div>'
    )


# ── 產生總覽表格 ─────────────────────────────────────────────
def summary_table(results: list) -> str:
    cards = ""
    for name, ticker, r in results:
        code = ticker.replace(".TW", "").replace(".tw", "")
        weekly = r.get("weekly", {})
        posture = weekly.get("posture", "觀察")
        posture_color = weekly.get("posture_color", r["border"])
        week_chg_pct = weekly.get("week_chg_pct")
        week_chg = pct_text(week_chg_pct)
        chg_color = UP_COLOR if (week_chg_pct or 0) >= 0 else DOWN_COLOR
        next_focus = html_lib.escape(_social_short_text(weekly.get("next_focus", ""), 110))
        ma_position = html_lib.escape(_social_short_text(weekly.get("ma_position", "-"), 48))
        inst_total = weekly.get("institutional_total")
        inst_text = format_market_value(inst_total / 1000) if inst_total is not None else "-"
        vol_ratio = weekly.get("volume_ratio")
        vol_text = f"{vol_ratio:.2f}x" if vol_ratio is not None else "-"
        high_low = f"{weekly.get('week_high', 0):.2f} / {weekly.get('week_low', 0):.2f}"
        cards += (
            f'<div style="border:1px solid #ddd;border-left:5px solid {posture_color};'
            f'border-radius:8px;padding:14px 16px;margin-bottom:14px;background:#fff;">'
            f'<div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start;">'
            f'<div style="min-width:0;">'
            f'<div style="font-size:16px;font-weight:bold;color:#2c3e50;line-height:1.4;">{name}</div>'
            f'<div style="font-size:12px;color:#888;margin-top:2px;">代號 {code}</div>'
            f'</div>'
            f'<div style="text-align:right;white-space:nowrap;">'
            f'<div style="font-size:11px;color:#888;">收盤 / 本週</div>'
            f'<div style="font-size:18px;font-weight:bold;color:#2c3e50;">{r["close"]:.2f}</div>'
            f'<div style="font-size:12px;font-weight:bold;color:{chg_color};">{week_chg}</div>'
            f'</div></div>'
            f'<div style="margin-top:8px;color:{posture_color};font-size:15px;font-weight:bold;">{posture}</div>'
            f'<div style="display:grid;grid-template-columns:1fr 1fr;gap:7px;margin-top:9px;">'
            f'<div style="background:#f7f9fb;border-radius:6px;padding:7px 8px;">'
            f'<div style="font-size:11px;color:#888;">本週高 / 低</div>'
            f'<div style="font-size:12px;font-weight:bold;color:#2c3e50;">{high_low}</div></div>'
            f'<div style="background:#f7f9fb;border-radius:6px;padding:7px 8px;">'
            f'<div style="font-size:11px;color:#888;">法人週合計</div>'
            f'<div style="font-size:12px;font-weight:bold;color:#2c3e50;">{inst_text}</div></div>'
            f'<div style="background:#f7f9fb;border-radius:6px;padding:7px 8px;">'
            f'<div style="font-size:11px;color:#888;">量能比</div>'
            f'<div style="font-size:12px;font-weight:bold;color:#2c3e50;">{vol_text}</div></div>'
            f'<div style="background:#f7f9fb;border-radius:6px;padding:7px 8px;">'
            f'<div style="font-size:11px;color:#888;">均線位置</div>'
            f'<div style="font-size:12px;font-weight:bold;color:#2c3e50;">{ma_position}</div></div>'
            f'</div>'
            f'<div style="margin-top:8px;color:#666;font-size:12px;line-height:1.55;">下週觀察：{next_focus}</div>'
            f'</div>'
        )
    return f'<div style="margin-bottom:28px;">{cards}</div>'


def market_context_html(macro: dict | None) -> str:
    if not macro:
        return ""

    fx = macro.get("fx")
    rates = macro.get("rates")
    fx_html = ""
    rates_html = ""

    if fx:
        fx_html = (
            f'<div style="padding:10px 12px;border-bottom:1px solid #eee;">'
            f'<strong>美元/台幣</strong>：{fx["value"]:.3f}｜'
            f'5日 {fx["chg_5d_pct"]:+.2f}%｜20日 {fx["chg_20d_pct"]:+.2f}%'
            f'<div style="color:#777;font-size:12px;margin-top:3px;">'
            f'數字變高代表台幣轉弱；短線通常提高外資撤出與台股修正風險，但出口股有部分匯兌抵銷。</div></div>'
        )

    if rates:
        rates_html = (
            f'<div style="padding:10px 12px;">'
            f'<strong>美國10年期公債殖利率</strong>：{rates["value"]:.2f}%｜'
            f'5日 {rates["chg_5d_bp"]:+.0f}bp｜20日 {rates["chg_20d_bp"]:+.0f}bp'
            f'<div style="color:#777;font-size:12px;margin-top:3px;">'
            f'殖利率上升通常壓抑科技股評價；殖利率下行則有利成長股估值修復。</div></div>'
        )

    if not fx_html and not rates_html:
        errors = "；".join(macro.get("errors", [])) or "未取得總體資料"
        return (f'<div style="background:#fff3cd;border:1px solid #ffeeba;'
                f'padding:10px 12px;border-radius:6px;margin-bottom:18px;'
                f'font-size:12px;color:#856404;">總體資料暫不可用：{errors}</div>')

    return (
        f'<h3 style="color:#2c3e50;border-bottom:2px solid #2c3e50;padding-bottom:6px;">總體環境</h3>'
        f'<div style="border:1px solid #ddd;border-radius:8px;overflow:hidden;margin-bottom:28px;">'
        f'{fx_html}{rates_html}</div>'
    )


def _classify_news_item(title: str) -> tuple:
    text = title.lower()
    high_keywords = ["戰爭", "開戰", "伊朗", "美伊", "霍爾木茲", "關稅", "晶片管制", "fomc", "fed", "川習", "習近平", "trump", "xi"]
    mid_keywords = ["原油", "油價", "利率", "殖利率", "匯率", "台積電", "tsmc", "nvidia", "ai", "半導體", "外資", "營收", "法說"]

    if any(k in text for k in high_keywords):
        impact = "高"
    elif any(k in text for k in mid_keywords):
        impact = "中高"
    else:
        impact = "中"

    if any(k in text for k in ["原油", "油價", "中東", "伊朗", "美伊", "霍爾木茲"]):
        note = "能源與地緣風險會影響通膨、利率預期與科技股評價；油價急漲通常壓抑風險偏好。"
        scope = "油價、通膨、全球股市、台股風險偏好"
    elif any(k in text for k in ["fed", "fomc", "利率", "殖利率"]):
        note = "利率預期會直接影響成長股估值；偏鷹訊息通常壓抑半導體與高本益比族群。"
        scope = "全球股市、美元、科技股、外資資金流"
    elif any(k in text for k in ["川習", "美中", "關稅", "晶片管制", "trump", "xi"]):
        note = "美中談判與晶片政策會影響半導體供應鏈、外資風險偏好與台股權值股評價。"
        scope = "台股、半導體、匯率、外資風險偏好"
    elif any(k in text for k in ["台積電", "tsmc", "nvidia", "ai", "半導體", "營收", "法說"]):
        note = "AI與半導體需求變化會影響台積電、聯發科與加權指數權值股表現。"
        scope = "台積電、聯發科、半導體供應鏈"
    else:
        note = "屬於市場風險偏好觀察項，需搭配價格、籌碼與總體環境判斷。"
        scope = "台股與全球風險偏好"
    return impact, scope, note


def _is_market_relevant_news(title: str) -> bool:
    text = title.lower()
    keywords = [
        "台股", "加權", "櫃買", "外資", "匯率", "台幣", "半導體", "晶片", "關稅",
        "美中", "川習", "習近平", "trump", "xi", "fed", "fomc", "利率", "殖利率",
        "原油", "油價", "中東", "伊朗", "美伊", "霍爾木茲", "台積電", "tsmc",
        "聯發科", "台達電", "鴻海", "廣達", "緯創", "緯穎", "nvidia", "ai",
        "ai伺服器", "cnyes", "鉅亨",
    ]
    return any(keyword in text for keyword in keywords)


def fetch_auto_news(cfg: dict) -> list:
    news_cfg = cfg.get("auto_news", {})
    if not news_cfg.get("enabled", False):
        return []

    queries = news_cfg.get("queries", [])
    lookback_days = int(news_cfg.get("lookback_days", 3))
    max_items = int(news_cfg.get("max_items", 8))
    max_items_per_query = int(news_cfg.get("max_items_per_query", 3))
    now = datetime.now(TAIPEI_TZ)
    min_date = now - timedelta(days=lookback_days)
    headers = {"User-Agent": "Mozilla/5.0"}
    items = []
    seen = set()

    for query in queries:
        url = (
            "https://news.google.com/rss/search?q="
            f"{quote_plus(query + f' when:{lookback_days}d')}"
            "&hl=zh-TW&gl=TW&ceid=TW:zh-Hant"
        )
        try:
            resp = requests.get(url, headers=headers, timeout=12)
            resp.raise_for_status()
            root = ET.fromstring(resp.content)
        except Exception:
            continue

        query_count = 0
        for item in root.findall(".//item"):
            title = item.findtext("title", "").strip()
            link = item.findtext("link", "").strip()
            pub_text = item.findtext("pubDate", "").strip()
            source = item.findtext("source", "").strip() or "Google News"
            if not title:
                continue
            if not _is_market_relevant_news(title):
                continue
            try:
                pub_dt = parsedate_to_datetime(pub_text).astimezone(TAIPEI_TZ)
            except Exception:
                pub_dt = now
            if pub_dt < min_date:
                continue
            key = re.sub(r"\s+", "", f"{title}{source}".lower())
            if key in seen:
                continue
            impact, scope, note = _classify_news_item(title)
            seen.add(key)
            items.append({
                "date": pub_dt.strftime("%Y-%m-%d %H:%M"),
                "_published_at": pub_dt,
                "title": title,
                "impact": impact,
                "scope": scope,
                "note": note,
                "source": source,
                "link": link,
            })
            query_count += 1
            if query_count >= max_items_per_query:
                break

    impact_rank = {"高": 3, "中高": 2, "中": 1, "低": 0}
    items.sort(key=lambda x: (x["_published_at"], impact_rank.get(x["impact"], 0)), reverse=True)
    for item in items:
        item.pop("_published_at", None)
    return items[:max_items]


def market_events_html(cfg: dict, today: str, news_items: list | None = None) -> str:
    events = cfg.get("market_events", [])
    window_days = int(cfg.get("market_events_window_days", cfg.get("market_events_lookahead_days", 3)))
    today_date = datetime.strptime(today, "%Y-%m-%d").date()
    start = today_date - timedelta(days=window_days)
    end = today_date + timedelta(days=window_days)
    scheduled_rows = ""
    news_rows = ""
    impact_colors = {"高": "#c0392b", "中高": "#e67e22", "中": "#f39c12", "低": "#7f8c8d"}

    for event in events:
        try:
            event_date = datetime.strptime(event["date"], "%Y-%m-%d").date()
        except Exception:
            continue
        if not (start <= event_date <= end):
            continue
        color = impact_colors.get(event.get("impact", ""), "#7f8c8d")
        scheduled_rows += (
            f'<tr style="border-bottom:1px solid #eee;">'
            f'<td style="padding:9px 12px;white-space:nowrap;color:#555;">{event["date"]}</td>'
            f'<td style="padding:9px 12px;font-weight:bold;">{event["title"]}</td>'
            f'<td style="padding:9px 12px;">'
            f'<span style="background:{color};color:#fff;font-size:11px;padding:2px 7px;border-radius:4px;white-space:nowrap;display:inline-block;">'
            f'{event.get("impact", "未評估")}</span></td>'
            f'<td style="padding:9px 12px;color:#666;font-size:12px;line-height:1.6;">'
            f'{event.get("scope", "")}｜{event.get("note", "")}'
            f'<div style="color:#aaa;margin-top:3px;">來源：{event.get("source", "手動維護")}</div></td>'
            f'</tr>'
        )

    if not scheduled_rows:
        scheduled_rows = (f'<tr><td style="padding:10px 12px;color:#777;font-size:12px;" colspan="4">'
                f'前後 {window_days} 天內尚未設定重大事件。</td></tr>')

    for item in news_items or []:
        color = impact_colors.get(item.get("impact", ""), "#7f8c8d")
        title = html_lib.escape(item.get("title", ""))
        source = html_lib.escape(item.get("source", "Google News"))
        link = html_lib.escape(item.get("link", ""))
        linked_title = f'<a href="{link}" style="color:#2c3e50;text-decoration:none;">{title}</a>' if link else title
        news_rows += (
            f'<tr style="border-bottom:1px solid #eee;">'
            f'<td style="padding:9px 12px;white-space:nowrap;color:#555;">{item.get("date", "")}</td>'
            f'<td style="padding:9px 12px;font-weight:bold;">{linked_title}</td>'
            f'<td style="padding:9px 12px;">'
            f'<span style="background:{color};color:#fff;font-size:11px;padding:2px 7px;border-radius:4px;white-space:nowrap;display:inline-block;">'
            f'{item.get("impact", "未評估")}</span></td>'
            f'<td style="padding:9px 12px;color:#666;font-size:12px;line-height:1.6;">'
            f'{item.get("scope", "")}｜{item.get("note", "")}'
            f'<div style="color:#aaa;margin-top:3px;">來源：{source}</div></td>'
            f'</tr>'
        )

    if not news_rows:
        news_rows = (f'<tr><td style="padding:10px 12px;color:#777;font-size:12px;" colspan="4">'
                     f'近 {cfg.get("auto_news", {}).get("lookback_days", 7)} 天未抓到符合條件的高關聯新聞。</td></tr>')

    return (
        f'<h3 style="color:#2c3e50;border-bottom:2px solid #2c3e50;padding-bottom:6px;">消息面與重大行事曆</h3>'
        f'<div style="font-size:12px;color:#777;margin:-12px 0 10px;">'
        f'固定行事曆顯示今天前後 {window_days} 天事件；自動新聞掃描只抓近 '
        f'{cfg.get("auto_news", {}).get("lookback_days", 3)} 天高關聯消息，例如油價、戰爭、美中、Fed與半導體新聞。</div>'
        f'<div style="font-weight:bold;color:#2c3e50;margin:4px 0 6px;">固定重大行事曆</div>'
        f'<table style="width:100%;border-collapse:collapse;margin-bottom:28px;'
        f'border:1px solid #ddd;border-radius:8px;overflow:hidden;">'
        f'<thead><tr style="background:#34495e;color:#fff;">'
        f'<th style="padding:10px 12px;text-align:left;">日期</th>'
        f'<th style="padding:10px 12px;text-align:left;">事件</th>'
        f'<th style="padding:10px 12px;text-align:left;">影響</th>'
        f'<th style="padding:10px 12px;text-align:left;">可能影響</th>'
        f'</tr></thead><tbody>{scheduled_rows}</tbody></table>'
        f'<div style="font-weight:bold;color:#2c3e50;margin:4px 0 6px;">近期自動新聞掃描</div>'
        f'<table style="width:100%;border-collapse:collapse;margin-bottom:28px;'
        f'border:1px solid #ddd;border-radius:8px;overflow:hidden;">'
        f'<thead><tr style="background:#566573;color:#fff;">'
        f'<th style="padding:10px 12px;text-align:left;">日期</th>'
        f'<th style="padding:10px 12px;text-align:left;">新聞</th>'
        f'<th style="padding:10px 12px;text-align:left;">影響</th>'
        f'<th style="padding:10px 12px;text-align:left;">可能影響</th>'
        f'</tr></thead><tbody>{news_rows}</tbody></table>'
    )


def scoring_rules_html() -> str:
    weights = [
        ("趨勢方向", WEIGHTS["trend"], "市場主方向"),
        ("MACD動能", WEIGHTS["macd"], "漲跌動能"),
        ("三大法人", WEIGHTS["institutional"], "法人籌碼"),
        ("KD", WEIGHTS["kd"], "進出場時機"),
        ("OBV", WEIGHTS["obv"], "量價配合"),
        ("台幣匯率", WEIGHTS["fx"], "台幣強弱影響外資流向與出口股獲利"),
        ("美國利率", WEIGHTS["rates"], "利率升降影響科技股評價"),
        ("量能", WEIGHTS["vol"], "成交確認"),
    ]
    weight_rows = "".join(
        f'<tr style="border-bottom:1px solid #eee;">'
        f'<td style="padding:7px 9px;font-weight:bold;color:#2c3e50;">{name}</td>'
        f'<td style="padding:7px 9px;text-align:right;color:#c0392b;font-weight:bold;">{score}</td>'
        f'<td style="padding:7px 9px;color:#777;font-size:12px;">{meaning}</td>'
        f'</tr>'
        for name, score, meaning in weights
    )
    trade_rows = "".join(
        f'<tr style="border-bottom:1px solid #eee;">'
        f'<td style="padding:7px 9px;font-weight:bold;color:#2c3e50;">{level}</td>'
        f'<td style="padding:7px 9px;color:#777;font-size:12px;">{note}</td>'
        f'</tr>'
        for level, note in [
            ("提醒", "只提醒市場溫度變化，不作為實際交易依據"),
            ("弱訊號", "只做觀察或小幅試單，不能單獨當成重倉理由"),
            ("中訊號", "代表多項條件開始一致，可考慮分批建立或降低部位"),
            ("強訊號", "代表高權重條件共振，但仍需保留後續調整空間"),
        ]
    )
    return (
        f'<details style="background:#f7fbff;border:1px solid #cfe2f3;border-radius:8px;'
        f'padding:12px 14px;margin-bottom:22px;">'
        f'<summary style="cursor:pointer;font-weight:bold;color:#1f4e79;font-size:15px;">'
        f'評分標準</summary>'
        f'<div style="margin-top:12px;">'
        f'<div style="font-size:13px;color:#555;line-height:1.7;margin-bottom:12px;">'
        f'系統會分別計算買進與賣出分數，最後以「實際參考分」作為主要判斷。'
        f'若季線乖離過熱，買進分數會被歸零，只保留背景分數讓你知道原本有哪些條件偏多。</div>'
        f'<div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px;">'
        f'<span style="background:#eef5fb;border:1px solid #d6eaf8;border-radius:6px;padding:5px 8px;font-size:12px;white-space:nowrap;display:inline-block;">提醒 15-29</span>'
        f'<span style="background:#fef9e7;border:1px solid #f9e79f;border-radius:6px;padding:5px 8px;font-size:12px;white-space:nowrap;display:inline-block;">弱 30-49</span>'
        f'<span style="background:#fef5e7;border:1px solid #fad7a0;border-radius:6px;padding:5px 8px;font-size:12px;white-space:nowrap;display:inline-block;">中 50-69</span>'
        f'<span style="background:#fdecea;border:1px solid #f5b7b1;border-radius:6px;padding:5px 8px;font-size:12px;white-space:nowrap;display:inline-block;">強 70+</span>'
        f'</div>'
        f'<table style="width:100%;border-collapse:collapse;background:#fff;border:1px solid #e5eef7;'
        f'border-radius:6px;overflow:hidden;">'
        f'<thead><tr style="background:#eaf4fb;color:#1f4e79;">'
        f'<th style="padding:8px 9px;text-align:left;">指標</th>'
        f'<th style="padding:8px 9px;text-align:right;">分數</th>'
        f'<th style="padding:8px 9px;text-align:left;">用途</th>'
        f'</tr></thead><tbody>{weight_rows}</tbody></table>'
        f'<div style="font-size:12px;color:#777;line-height:1.6;margin-top:10px;">'
        f'BIAS60 用來判斷中期過熱或超跌，不直接加分；過熱時會鎖住買進，避免追高。</div>'
        f'<div style="font-weight:bold;color:#1f4e79;font-size:14px;margin:14px 0 8px;">交易訊號怎麼用</div>'
        f'<div style="font-size:13px;color:#555;line-height:1.7;margin-bottom:10px;">'
        f'這裡說明訊號等級的用途，不代表一定要完整照比例下單。'
        f'系統會再依市場狀態調整：大多頭少賣、空頭少買、盤整時才較適合分批操作。'
        f'同一等級訊號連續出現時，不建議每週重複操作。</div>'
        f'<table style="width:100%;border-collapse:collapse;background:#fff;border:1px solid #e5eef7;'
        f'border-radius:6px;overflow:hidden;">'
        f'<thead><tr style="background:#eaf4fb;color:#1f4e79;">'
        f'<th style="padding:8px 9px;text-align:left;">等級</th>'
        f'<th style="padding:8px 9px;text-align:left;">實際用途</th>'
        f'</tr></thead><tbody>{trade_rows}</tbody></table>'
        f'</div></details>'
    )



# ── 週報版呈現輔助 ───────────────────────────────────────────
def _plain_number(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "-"
    return f"{value:.{digits}f}"


def _plain_inst_text(value: float | None) -> str:
    if value is None:
        return "-"
    return format_market_value_text(value / 1000)


def _pct_color(value: float | None) -> str:
    return UP_COLOR if (value or 0) >= 0 else DOWN_COLOR


def _escape(value) -> str:
    return html_lib.escape(str(value or ""))


def _svg_polyline(points: list[tuple[float, float]], color: str, width: float = 3.0, dash: str = "") -> str:
    if len(points) < 2:
        return ""
    raw = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
    dash_attr = f" stroke-dasharray='{dash}'" if dash else ""
    return f"<polyline points='{raw}' fill='none' stroke='{color}' stroke-width='{width}' stroke-linecap='round' stroke-linejoin='round'{dash_attr}/>"


def render_sparkline(values: list, width: int = 120, height: int = 34, color: str | None = None) -> str:
    clean = []
    for value in values or []:
        try:
            clean.append(float(value))
        except Exception:
            pass
    if len(clean) < 2:
        return f"<div style='height:{height}px;color:#9a927e;font-size:11px;'>資料不足</div>"
    lo, hi = min(clean), max(clean)
    span = hi - lo or 1.0
    pts = []
    for i, value in enumerate(clean):
        x = width * i / max(len(clean) - 1, 1)
        y = height - 4 - ((value - lo) / span * (height - 8))
        pts.append((x, y))
    line_color = color or (_pct_color(clean[-1] - clean[0]))
    return f"<svg width='{width}' height='{height}' viewBox='0 0 {width} {height}' xmlns='http://www.w3.org/2000/svg'><polyline points='{' '.join(f'{x:.1f},{y:.1f}' for x,y in pts)}' fill='none' stroke='{line_color}' stroke-width='3' stroke-linecap='round' stroke-linejoin='round'/></svg>"


def render_price_chart(result: dict, width: int = 760, height: int = 300, compact: bool = False) -> str:
    weekly = result.get("weekly", {})
    points = weekly.get("chart_points", [])
    if len(points) < 2:
        return "<div style='color:#7a8178;font-size:13px;'>走勢資料不足，暫無線圖。</div>"

    pad_l, pad_r, pad_t, pad_b = 54, 18, 22, 38
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b
    values = []
    for pt in points:
        for key in ("close", "ma_s", "ma_m", "ma_l"):
            val = pt.get(key)
            if val is not None:
                values.append(float(val))
    lo, hi = min(values), max(values)
    span = hi - lo or 1.0

    def xy(i: int, value: float) -> tuple[float, float]:
        x = pad_l + (plot_w * i / max(len(points) - 1, 1))
        y = pad_t + plot_h - ((value - lo) / span * plot_h)
        return x, y

    def series(key: str, color: str, width_line: float, dash: str = "") -> str:
        pts = []
        for i, pt in enumerate(points):
            val = pt.get(key)
            if val is not None:
                pts.append(xy(i, float(val)))
        return _svg_polyline(pts, color, width_line, dash)

    week_start_idx = max(len(points) - 5, 0)
    week_start_x = pad_l + plot_w * week_start_idx / max(len(points) - 1, 1)
    week_points = [xy(i, float(pt["close"])) for i, pt in enumerate(points) if i >= week_start_idx and pt.get("close") is not None]
    close = points[-1]["close"]
    week_chg = weekly.get("week_chg_pct")
    week_color = _pct_color(week_chg)
    title_size = 18 if compact else 20
    svg = f"""
    <svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="近60日價格走勢">
      <rect x="0" y="0" width="{width}" height="{height}" rx="18" fill="#fffdf7"/>
      <rect x="{week_start_x:.1f}" y="{pad_t}" width="{width - pad_r - week_start_x:.1f}" height="{plot_h}" fill="#efe5bd" opacity="0.32"/>
      <line x1="{pad_l}" y1="{pad_t}" x2="{pad_l}" y2="{pad_t + plot_h}" stroke="#d8d1bd"/>
      <line x1="{pad_l}" y1="{pad_t + plot_h}" x2="{width - pad_r}" y2="{pad_t + plot_h}" stroke="#d8d1bd"/>
      {series('ma_l', '#8f9a91', 2.0, '6 5')}
      {series('ma_m', '#c9a227', 2.2)}
      {series('ma_s', '#6d8f7a', 2.0)}
      {series('close', '#aeb7ad', 2.4)}
      {_svg_polyline(week_points, week_color, 6.0)}
      <text x="{pad_l}" y="{height - 13}" fill="#6f776f" font-size="13">近60日收盤價與 MA10 / MA20 / MA60；粗線為本週5日走勢</text>
      <text x="{width - pad_r}" y="{pad_t + 4}" text-anchor="end" fill="#12322b" font-size="{title_size}" font-weight="800">{close:.2f} / {pct_text(week_chg)}</text>
      <text x="{pad_l}" y="18" fill="#6f776f" font-size="12">高 {hi:.2f}</text>
      <text x="{pad_l}" y="{pad_t + plot_h - 6}" fill="#6f776f" font-size="12">低 {lo:.2f}</text>
    </svg>"""
    return svg


def weekly_market_overview_html(results: list, macro: dict | None, compact: bool = False) -> str:
    if not results:
        return ""
    market = results[0][2]
    weekly = market.get("weekly", {})
    fx = macro.get("fx") if macro else None
    rates = macro.get("rates") if macro else None
    chart_w = 920 if compact else 650
    chart_h = 360 if compact else 280
    chart = render_price_chart(market, chart_w, chart_h, compact=compact)
    fx_metric = f"{fx['value']:.3f}" if fx else "-"
    rates_metric = f"{rates['value']:.2f}%" if rates else "-"
    inst_value_text = weekly.get("institutional_value_text") or format_twd_billion_short(weekly.get("institutional_value"))
    inst_series = weekly.get("institutional_daily_values", [])
    fx_series = fx.get("series", []) if fx else []
    rates_series = rates.get("series", []) if rates else []
    metric_style = "background:#f8f4e7;border:1px solid #e5dcc0;border-radius:10px;padding:10px 12px;"
    return (
        f'<div style="background:{WEEKLY_PANEL};border:1px solid #e0d7bd;border-radius:14px;padding:18px 20px;margin-bottom:24px;">'
        f'<div style="display:flex;justify-content:space-between;gap:18px;align-items:flex-start;">'
        f'<div style="min-width:0;flex:1;">'
        f'<div style="color:{WEEKLY_GOLD};font-size:12px;font-weight:bold;letter-spacing:.08em;">WEEKLY MARKET BRIEF</div>'
        f'<div style="color:{WEEKLY_DARK};font-size:24px;font-weight:800;margin-top:4px;">{_escape(weekly.get("posture", "觀察"))}</div>'
        f'<div style="color:#4f5a52;font-size:14px;line-height:1.7;margin-top:8px;">{_escape(weekly.get("trend_summary", ""))}<br>{_escape(weekly.get("next_focus", ""))}</div>'
        f'</div>'
        f'<div style="text-align:right;white-space:nowrap;">'
        f'<div style="color:#6f776f;font-size:12px;">加權指數收盤</div>'
        f'<div style="color:{WEEKLY_DARK};font-size:30px;font-weight:800;">{market.get("close", 0):.2f}</div>'
        f'<div style="color:{_pct_color(weekly.get("week_chg_pct"))};font-size:16px;font-weight:800;">{pct_text(weekly.get("week_chg_pct"))}</div>'
        f'</div></div>'
        f'<div style="margin-top:16px;">{chart}</div>'
        f'<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-top:14px;">'
        f'<div style="{metric_style}"><div style="font-size:11px;color:#7a8178;">本週高 / 低</div><div style="font-size:15px;font-weight:800;color:{WEEKLY_DARK};">{weekly.get("week_high", 0):.2f} / {weekly.get("week_low", 0):.2f}</div></div>'
        f'<div style="{metric_style}"><div style="font-size:11px;color:#7a8178;">法人週合計（金額）</div><div style="font-size:15px;font-weight:800;color:{_pct_color(weekly.get("institutional_value"))};">{inst_value_text}</div>{render_sparkline(inst_series, 118, 28)}</div>'
        f'<div style="{metric_style}"><div style="font-size:11px;color:#7a8178;">美元/台幣</div><div style="font-size:15px;font-weight:800;color:{WEEKLY_DARK};">{fx_metric}</div>{render_sparkline(fx_series, 118, 28)}</div>'
        f'<div style="{metric_style}"><div style="font-size:11px;color:#7a8178;">美10年債</div><div style="font-size:15px;font-weight:800;color:{WEEKLY_DARK};">{rates_metric}</div>{render_sparkline(rates_series, 118, 28)}</div>'
        f'</div></div>'
    )


def weekly_stock_scoreboard_html(results: list) -> str:
    stock_results = results[1:] if results and results[0][1] == "^TWII" else results
    sorted_results = sorted(stock_results, key=lambda item: item[2].get("weekly", {}).get("week_chg_pct") or 0, reverse=True)
    rows = ""
    max_abs = max([abs(item[2].get("weekly", {}).get("week_chg_pct") or 0) for item in sorted_results] + [1])
    for name, ticker, r in sorted_results:
        weekly = r.get("weekly", {})
        chg = weekly.get("week_chg_pct") or 0
        width = max(8, abs(chg) / max_abs * 100)
        color = UP_COLOR if chg >= 0 else DOWN_COLOR
        rows += (
            f'<div style="display:grid;grid-template-columns:92px 1fr 64px;gap:10px;align-items:center;margin:8px 0;">'
            f'<div style="font-size:13px;font-weight:800;color:{WEEKLY_DARK};">{_escape(name)}</div>'
            f'<div style="height:16px;background:#ebe6d6;border-radius:99px;overflow:hidden;"><div style="height:16px;width:{width:.1f}%;background:{color};border-radius:99px;"></div></div>'
            f'<div style="font-size:13px;font-weight:800;text-align:right;color:{color};">{pct_text(chg)}</div>'
            f'</div>'
        )
    return f'<div style="background:{WEEKLY_PANEL};border:1px solid #e0d7bd;border-radius:14px;padding:16px 18px;margin-bottom:24px;"><h3 style="margin:0 0 10px;color:{WEEKLY_DARK};font-size:18px;">權值股本週漲跌排名</h3>{rows}</div>'


def weekly_trend_matrix_html(results: list) -> str:
    rows = ""
    for name, ticker, r in results:
        weekly = r.get("weekly", {})
        inst = weekly.get("institutional_value_text") or format_twd_billion_short(weekly.get("institutional_value"))
        vol = weekly.get("volume_ratio")
        vol_text = f"{vol:.2f}x" if vol is not None else "-"
        vol_tip = volume_ratio_note(vol)
        rows += (
            f'<tr style="border-bottom:1px solid #e7dfc9;">'
            f'<td style="padding:9px 8px;font-weight:800;color:{WEEKLY_DARK};">{_escape(name)}</td>'
            f'<td style="padding:9px 8px;color:{weekly.get("posture_color", WEEKLY_DARK)};font-weight:800;">{_escape(weekly.get("posture", "觀察"))}</td>'
            f'<td style="padding:9px 8px;text-align:right;color:{_pct_color(weekly.get("week_chg_pct"))};font-weight:800;">{pct_text(weekly.get("week_chg_pct"))}</td>'
            f'<td style="padding:9px 8px;color:#4f5a52;line-height:1.5;white-space:normal;">{_escape(weekly.get("ma_position", "-"))}</td>'
            f'<td style="padding:9px 8px;text-align:right;color:{_pct_color(weekly.get("institutional_value"))};font-weight:800;">{_escape(inst)}</td>'
            f'<td style="padding:9px 8px;text-align:right;color:#4f5a52;" title="{_escape(vol_tip)}">{vol_text}</td>'
            f'</tr>'
        )
    return (
        f'<div style="background:{WEEKLY_PANEL};border:1px solid #e0d7bd;border-radius:14px;padding:16px 18px;margin-bottom:24px;">'
        f'<h3 style="margin:0 0 10px;color:{WEEKLY_DARK};font-size:18px;">趨勢矩陣</h3>'
        f'<table style="width:100%;border-collapse:collapse;font-size:12px;">'
        f'<thead><tr style="background:#efe7cf;color:{WEEKLY_DARK};"><th style="padding:8px;text-align:left;width:90px;">標的</th><th style="padding:8px;text-align:left;width:80px;">狀態</th><th style="padding:8px;text-align:right;width:64px;">本週</th><th style="padding:8px;text-align:left;">均線</th><th style="padding:8px;text-align:right;width:100px;">法人金額</th><th style="padding:8px;text-align:right;width:70px;">量能</th></tr></thead>'
        f'<tbody>{rows}</tbody></table></div>'
    )

# ── 組裝 HTML Email ──────────────────────────────────────────
def build_email_html(results: list, today: str, cfg: dict | None = None,
                     macro: dict | None = None, news_items: list | None = None) -> str:
    meta = get_report_meta(datetime.strptime(today, "%Y-%m-%d").replace(tzinfo=TAIPEI_TZ))
    market_brief = weekly_market_overview_html(results, macro)
    scoreboard = weekly_stock_scoreboard_html(results)
    matrix = weekly_trend_matrix_html(results)
    events_block = market_events_html(cfg or {}, today, news_items)
    rules_block = scoring_rules_html()
    details = "".join(
        stock_html_block(n, t, r, note=r.get("stock_note", ""))
        for n, t, r in results
    )
    return (
        f'<!DOCTYPE html><html><head><meta charset="utf-8"></head>'
        f'<body style="font-family:Arial,sans-serif;max-width:760px;margin:0 auto;padding:20px;background:{WEEKLY_BG};">'
        f'<div style="background:{WEEKLY_DARK};color:#fff;padding:24px 26px;border-radius:14px 14px 0 0;">'
        f'<div style="color:{WEEKLY_GOLD};font-size:12px;font-weight:bold;letter-spacing:.12em;">TAIWAN EQUITY WEEKLY</div>'
        f'<h2 style="margin:6px 0 0;font-size:28px;line-height:1.25;">每週台股趨勢報告</h2>'
        f'<p style="margin:8px 0 0;color:#d9d2bd;">{today}｜{meta["week_label"]}｜一週總結、方向整理、下週觀察</p></div>'
        f'<div style="background:#fffaf0;padding:24px;border-radius:0 0 14px 14px;box-shadow:0 8px 24px rgba(18,50,43,.10);">'
        f'{market_brief}'
        f'{scoreboard}'
        f'{matrix}'
        f'{events_block}'
        f'<h3 style="color:{WEEKLY_DARK};border-bottom:2px solid {WEEKLY_GOLD};padding-bottom:6px;">指標解讀規則</h3>'
        f'{rules_block}'
        f'<h3 style="color:{WEEKLY_DARK};border-bottom:2px solid {WEEKLY_GOLD};padding-bottom:6px;">各股指標明細</h3>'
        f'{details}'
        f'<p style="color:#9a927e;font-size:11px;text-align:center;border-top:1px solid #e6ddc7;padding-top:12px;margin-top:8px;">'
        f'本報告由自動化模型產生，僅供參考，不構成投資建議。</p>'
        f'</div></body></html>'
    )

def _social_item_detail(result: dict, label: str, default: str = "-") -> tuple[str, str, str]:
    for item_label, value, color, note in result.get("items", []):
        if item_label == label or item_label.startswith(label):
            return str(value), str(color), str(note)
    return default, NEUTRAL_COLOR, ""


def _social_item(result: dict, label: str, default: str = "-") -> str:
    value, _color, _note = _social_item_detail(result, label, default)
    return value


def _social_reason(result: dict, limit: int = 78) -> str:
    trade_plan = result.get("trade_plan", {})
    reason = trade_plan.get("reason") or result.get("advice", "")
    reason = re.sub(r"\s+", " ", str(reason)).strip()
    return html_lib.escape(reason[:limit] + ("..." if len(reason) > limit else ""))


def _social_short_text(value: str, limit: int = 34) -> str:
    value = re.sub(r"\s+", " ", str(value or "")).strip()
    return value[:limit] + ("..." if len(value) > limit else "")


def _social_events(cfg: dict | None, today: str, limit: int = 4) -> list[dict]:
    if not cfg:
        return []
    events = cfg.get("market_events", [])
    window_days = int(cfg.get("market_events_window_days", cfg.get("market_events_lookahead_days", 14)))
    today_date = datetime.strptime(today, "%Y-%m-%d").date()
    start = today_date - timedelta(days=window_days)
    end = today_date + timedelta(days=window_days)
    filtered = []
    for event in events:
        try:
            event_date = datetime.strptime(event["date"], "%Y-%m-%d").date()
        except Exception:
            continue
        if start <= event_date <= end:
            item = dict(event)
            item["_distance"] = abs((event_date - today_date).days)
            filtered.append(item)
    impact_rank = {"高": 4, "中高": 3, "中": 2, "低": 1}
    filtered.sort(key=lambda x: (x["_distance"], -impact_rank.get(x.get("impact", ""), 0), x.get("date", "")))
    return filtered[:limit]


def _social_indicator_tile(result: dict, label: str, title: str | None = None) -> str:
    value, color, note = _social_item_detail(result, label)
    title = title or label
    note_text = ""
    if note:
        note_text = _social_short_text(note.split("｜")[0], 24)
    return (
        f"<div class='ind'>"
        f"<div class='ind-title'>{html_lib.escape(title)}</div>"
        f"<div class='ind-value' style='color:{color}'>{html_lib.escape(_social_short_text(value, 16))}</div>"
        f"<div class='ind-note'>{html_lib.escape(note_text)}</div>"
        f"</div>"
    )


def _social_score_impact(note: str) -> tuple[float, float]:
    match = re.search(r"分數影響:買進\+([0-9.]+)\/賣出\+([0-9.]+)", note or "")
    if not match:
        return 0.0, 0.0
    return float(match.group(1)), float(match.group(2))


def _social_key_indicator_tiles(result: dict, limit: int = 2) -> str:
    priority = {
        "趨勢環境": 90,
        "MACD": 80,
        "三大法人": 70,
        "美元/台幣匯率": 62,
        "利率環境": 60,
        "KD": 55,
        "OBV": 50,
        "量能趨勢": 45,
        "均線交叉": 40,
        "價格行為": 10,
    }
    candidates = []
    for label, value, color, note in result.get("items", []):
        if label in ("BIAS60 Z-Score", "⚠️ 槓桿警示"):
            continue
        buy, sell = _social_score_impact(str(note))
        score = max(buy, sell)
        if score <= 0 and label not in ("趨勢環境", "MACD"):
            continue
        candidates.append((score, priority.get(label, 0), str(label), str(value), str(color), str(note)))

    candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
    picked = candidates[:limit]
    existing = {item[2] for item in picked}
    for fallback in ("趨勢環境", "MACD", "三大法人", "KD"):
        if len(picked) >= limit:
            break
        if fallback in existing:
            continue
        value, color, note = _social_item_detail(result, fallback)
        if value != "-":
            picked.append((0.0, priority.get(fallback, 0), fallback, value, color, note))
            existing.add(fallback)

    tiles = ""
    title_map = {"趨勢環境": "趨勢", "三大法人": "法人", "美元/台幣匯率": "匯率", "利率環境": "利率", "量能趨勢": "量能"}
    for _score, _priority, label, value, color, note in picked[:limit]:
        note_text = _social_short_text(str(note).split("｜")[0], 24) if note else ""
        tiles += (
            f"<div class='ind'>"
            f"<div class='ind-title'>{html_lib.escape(title_map.get(label, label))}</div>"
            f"<div class='ind-value' style='color:{color}'>{html_lib.escape(_social_short_text(value, 16))}</div>"
            f"<div class='ind-note'>{html_lib.escape(note_text)}</div>"
            f"</div>"
        )
    return tiles


def build_social_report_pages(results: list, today: str, cfg: dict | None = None,
                              macro: dict | None = None, news_items: list | None = None) -> list[str]:
    news_items = news_items or []
    date_text = today.replace("-", "/")
    meta = get_report_meta(datetime.strptime(today, "%Y-%m-%d").replace(tzinfo=TAIPEI_TZ))
    market = results[0][2] if results else {}
    market_weekly = market.get("weekly", {})
    fx = macro.get("fx") if macro else None
    rates = macro.get("rates") if macro else None
    fx_value = f"{fx['value']:.3f}" if fx else "-"
    rates_value = f"{rates['value']:.2f}%" if rates else "-"
    inst_value_text = market_weekly.get("institutional_value_text") or format_twd_billion_short(market_weekly.get("institutional_value"))
    chart = render_price_chart(market, 930, 420, compact=True)
    css = f"""
    <style>
      *{{box-sizing:border-box}} body{{margin:0;background:#e8e1d0;font-family:Arial,'Noto Sans TC',sans-serif;color:#26322d}}
      .page{{width:1080px;height:1920px;background:{WEEKLY_BG};padding:48px 58px;overflow:hidden}}
      .header{{background:{WEEKLY_DARK};color:#fff;border-radius:26px;padding:30px 34px;margin-bottom:24px;border-bottom:8px solid {WEEKLY_GOLD}}}
      .kicker{{color:{WEEKLY_GOLD};font-size:18px;font-weight:800;letter-spacing:.12em}}.title{{font-size:48px;font-weight:900;line-height:1.15;margin-top:8px}}.date{{font-size:23px;color:#d9d2bd;margin-top:8px}}
      .section{{background:{WEEKLY_PANEL};border:1px solid #ded4b8;border-radius:22px;padding:24px 28px;margin-bottom:20px;box-shadow:0 10px 24px rgba(18,50,43,.08)}}
      .section-title{{font-size:29px;font-weight:900;color:{WEEKLY_DARK};margin-bottom:16px}}.brief{{display:grid;grid-template-columns:1.4fr .9fr;gap:18px;align-items:stretch}}
      .summary{{font-size:34px;font-weight:900;color:{market_weekly.get('posture_color', WEEKLY_DARK)};line-height:1.15}}.summary-sub{{font-size:22px;line-height:1.55;color:#4f5a52;margin-top:12px}}
      .metric-grid{{display:grid;grid-template-columns:1fr 1fr;gap:12px}}.metric{{background:#f4edd9;border-radius:16px;padding:16px 18px}}.metric-label{{font-size:17px;color:#7a8178}}.metric-value{{font-size:27px;font-weight:900;color:{WEEKLY_DARK};margin-top:5px}}
      .event-grid{{display:grid;grid-template-columns:1fr 1fr;gap:14px}}.event{{border-left:8px solid var(--c);background:#fbf7eb;border-radius:14px;padding:14px 16px;min-height:112px}}.event-title{{font-size:20px;font-weight:900;line-height:1.32;color:{WEEKLY_DARK}}}.event-note{{font-size:17px;line-height:1.38;color:#536158;margin-top:7px}}
      .bars{{display:grid;gap:12px}}.bar-row{{display:grid;grid-template-columns:116px 1fr 74px;gap:12px;align-items:center}}.bar-name{{font-size:21px;font-weight:900;color:{WEEKLY_DARK}}}.bar-track{{height:22px;background:#e8dfc8;border-radius:999px;overflow:hidden}}.bar-fill{{height:22px;border-radius:999px;background:var(--c);width:var(--w)}}.bar-val{{font-size:20px;font-weight:900;text-align:right;color:var(--c)}}
      .matrix{{display:grid;grid-template-columns:1fr 1fr;gap:14px}}.stock-card{{background:#fffdf7;border:1px solid #ded4b8;border-left:10px solid var(--c);border-radius:16px;padding:15px 16px;min-height:156px}}.stock-head{{display:flex;justify-content:space-between;gap:12px;align-items:flex-start}}.stock-name{{font-size:25px;font-weight:900;color:{WEEKLY_DARK}}}.stock-price{{font-size:24px;font-weight:900;color:#24352f}}.stock-status{{display:inline-flex;align-items:center;min-height:34px;background:var(--c);color:#fff;border-radius:999px;padding:0 12px;font-size:17px;font-weight:900;margin-top:10px}}.stock-note{{font-size:16px;color:#536158;line-height:1.38;margin-top:9px;max-height:44px;overflow:hidden}}.tile-row{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-top:10px}}.tile{{background:#f4edd9;border-radius:10px;padding:8px}}.tile-label{{font-size:12px;color:#7a8178}}.tile-value{{font-size:15px;font-weight:900;color:{WEEKLY_DARK};margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
      .footer{{font-size:16px;color:#7a8178;text-align:center;margin-top:10px}}
    </style>
    """
    metric_html = f"""
      <div class='metric-grid'>
        <div class='metric'><div class='metric-label'>加權收盤</div><div class='metric-value'>{market.get('close', 0):.2f}</div></div>
        <div class='metric'><div class='metric-label'>本週漲跌</div><div class='metric-value' style='color:{_pct_color(market_weekly.get('week_chg_pct'))}'>{pct_text(market_weekly.get('week_chg_pct'))}</div></div>
        <div class='metric'><div class='metric-label'>週高 / 週低</div><div class='metric-value'>{market_weekly.get('week_high', 0):.0f}/{market_weekly.get('week_low', 0):.0f}</div></div>
        <div class='metric'><div class='metric-label'>法人週合計（金額）</div><div class='metric-value' style='color:{_pct_color(market_weekly.get('institutional_value'))}'>{inst_value_text}</div>{render_sparkline(market_weekly.get('institutional_daily_values', []), 132, 30)}</div>
        <div class='metric'><div class='metric-label'>美元/台幣</div><div class='metric-value'>{fx_value}</div>{render_sparkline(fx.get('series', []) if fx else [], 132, 30)}</div>
        <div class='metric'><div class='metric-label'>美10年債</div><div class='metric-value'>{rates_value}</div>{render_sparkline(rates.get('series', []) if rates else [], 132, 30)}</div>
      </div>"""
    impact_colors = {"高": UP_COLOR, "中高": WARN_COLOR, "中": "#b8871b", "低": NEUTRAL_COLOR}
    events = _social_events(cfg, today, 4)
    event_rows = "".join(
        f"<div class='event' style='--c:{impact_colors.get(e.get('impact',''), NEUTRAL_COLOR)}'><div class='event-title'>{html_lib.escape(e.get('title',''))}</div><div class='event-note'>{html_lib.escape(e.get('date',''))}｜{html_lib.escape(_social_short_text(e.get('note',''), 62))}</div></div>"
        for e in events
    ) or f"<div class='event' style='--c:{WARN_COLOR}'><div class='event-title'>市場週變化觀察</div><div class='event-note'>本週加權指數{pct_text(market_weekly.get('week_chg_pct'))}；若新聞掃描未取得資料，仍以價格、法人、匯率與利率變化作為週報判斷基礎。</div></div>"
    page1 = f"""<!DOCTYPE html><html><head><meta charset='utf-8'>{css}</head><body><div class='page'>
      <div class='header'><div class='kicker'>TAIWAN EQUITY WEEKLY</div><div class='title'>每週台股趨勢報告</div><div class='date'>{date_text}｜{meta['week_label']}｜一週總結與方向整理</div></div>
      <div class='section'><div class='brief'><div><div class='summary'>{html_lib.escape(market_weekly.get('posture','觀察'))}</div><div class='summary-sub'>{html_lib.escape(market_weekly.get('trend_summary',''))}<br>{html_lib.escape(market_weekly.get('next_focus',''))}</div></div>{metric_html}</div></div>
      <div class='section'><div class='section-title'>加權指數 60 日走勢</div>{chart}</div>
      <div class='section'><div class='section-title'>重大事件回顧</div><div class='event-grid'>{event_rows}</div></div>
      <div class='footer'>本圖由自動化模型產生，僅供參考，不構成投資建議。</div>
    </div></body></html>"""

    stock_results = results[1:] if results and results[0][1] == "^TWII" else results
    sorted_results = sorted(stock_results, key=lambda item: item[2].get('weekly', {}).get('week_chg_pct') or 0, reverse=True)
    max_abs = max([abs(item[2].get('weekly', {}).get('week_chg_pct') or 0) for item in sorted_results] + [1])
    bars = ""
    for name, ticker, r in sorted_results:
        weekly = r.get('weekly', {})
        chg = weekly.get('week_chg_pct') or 0
        width = max(8, abs(chg) / max_abs * 100)
        color = _pct_color(chg)
        bars += f"<div class='bar-row'><div class='bar-name'>{html_lib.escape(name)}</div><div class='bar-track'><div class='bar-fill' style='--c:{color};--w:{width:.1f}%'></div></div><div class='bar-val' style='--c:{color}'>{pct_text(chg)}</div></div>"
    cards = ""
    for name, ticker, r in sorted_results:
        weekly = r.get('weekly', {})
        inst_total = weekly.get('institutional_total')
        inst_text = weekly.get('institutional_value_text') or format_twd_billion_short(weekly.get('institutional_value'))
        vol = weekly.get('volume_ratio')
        vol_text = f"{vol:.2f}x" if vol is not None else "-"
        vol_tip = volume_ratio_note(vol)
        color = weekly.get('posture_color', r.get('border', NEUTRAL_COLOR))
        cards += (
            f"<div class='stock-card' style='--c:{color}'><div class='stock-head'><div><div class='stock-name'>{html_lib.escape(name)}</div><div style='font-size:15px;color:#7a8178'>{ticker.replace('.TW','').replace('.tw','')}</div></div><div class='stock-price'>{r.get('close',0):.2f}</div></div>"
            f"<div class='stock-status'>{html_lib.escape(weekly.get('posture','觀察'))}</div><div class='stock-note'>{html_lib.escape(_social_short_text(weekly.get('next_focus',''), 58))}</div>"
            f"<div class='tile-row'><div class='tile'><div class='tile-label'>本週</div><div class='tile-value' style='color:{_pct_color(weekly.get('week_chg_pct'))}'>{pct_text(weekly.get('week_chg_pct'))}</div></div><div class='tile'><div class='tile-label'>法人金額</div><div class='tile-value' style='color:{_pct_color(weekly.get('institutional_value'))}'>{html_lib.escape(inst_text)}</div></div><div class='tile'><div class='tile-label'>量能</div><div class='tile-value'>{vol_text}</div></div></div><div style='font-size:12px;color:#7a8178;margin-top:6px;line-height:1.25'>{html_lib.escape(vol_tip)}</div></div>"
        )
    page2 = f"""<!DOCTYPE html><html><head><meta charset='utf-8'>{css}</head><body><div class='page'>
      <div class='header'><div class='kicker'>LARGE CAP MAP</div><div class='title'>權值股週變化地圖</div><div class='date'>{date_text}｜漲跌排名、趨勢分層、下週觀察</div></div>
      <div class='section'><div class='section-title'>8 檔權值股本週漲跌排名</div><div class='bars'>{bars}</div></div>
      <div class='section'><div class='section-title'>趨勢矩陣</div><div class='matrix'>{cards}</div></div>
      <div class='footer'>完整指標與評分細節請以 Email 報告為準。</div>
    </div></body></html>"""
    return [page1, page2]

def save_social_report_pages(pages: list[str], today: str) -> list[Path]:
    paths = []
    date_key = today.replace("-", "")
    for idx, html in enumerate(pages, start=1):
        path = Path(__file__).parent / f"social_report_{idx:02d}.html"
        path.write_text(html, encoding="utf-8")
        paths.append(path)
    return paths

# ── 本機 HTML 預覽 ───────────────────────────────────────────
def save_email_preview(html: str) -> Path:
    preview_path = Path(__file__).parent / "email_preview.html"
    preview_path.write_text(html, encoding="utf-8")
    return preview_path


# ── 產生分享圖片與上傳雲端硬碟 ───────────────────────────────
def render_report_image(html_path: Path, today: str, cfg: dict, output_name: str | None = None, full_page: bool = True, height: int = 1200) -> Path | None:
    drive_cfg = cfg.get("drive_report", {})
    if not drive_cfg.get("enabled", False):
        return None

    image_path = Path(__file__).parent / (output_name or f"{today.replace('-', '')}.png")
    width = int(drive_cfg.get("image_width", 900))

    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        print(f"⚠️  未安裝 Playwright，跳過產生圖片：{exc}")
        return None

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(args=["--no-sandbox"])
            page = browser.new_page(
                viewport={"width": width, "height": height},
                device_scale_factor=2,
            )
            page.goto(html_path.resolve().as_uri(), wait_until="networkidle")
            page.screenshot(path=str(image_path), full_page=full_page)
            browser.close()
        return image_path
    except Exception as exc:
        print(f"⚠️  產生報告圖片失敗：{exc}")
        return None


def _load_google_service_account_info() -> dict | None:
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not raw:
        return None
    try:
        if raw.startswith("{"):
            return json.loads(raw)
        return json.loads(base64.b64decode(raw).decode("utf-8"))
    except Exception as exc:
        print(f"⚠️  GOOGLE_SERVICE_ACCOUNT_JSON 格式錯誤：{exc}")
        return None


def _build_google_drive_credentials():
    scopes = ["https://www.googleapis.com/auth/drive"]
    refresh_token = os.environ.get("GOOGLE_OAUTH_REFRESH_TOKEN", "").strip()
    client_id = os.environ.get("GOOGLE_OAUTH_CLIENT_ID", "").strip()
    client_secret = os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET", "").strip()
    if refresh_token and client_id and client_secret:
        try:
            from google.oauth2.credentials import Credentials
            from google.auth.transport.requests import Request
            credentials = Credentials(
                token=None,
                refresh_token=refresh_token,
                token_uri="https://oauth2.googleapis.com/token",
                client_id=client_id,
                client_secret=client_secret,
                scopes=scopes,
            )
            credentials.refresh(Request())
            return credentials, "OAuth"
        except Exception as exc:
            print(f"⚠️  Google OAuth 憑證失敗，改試 service account：{exc}")

    sa_info = _load_google_service_account_info()
    if sa_info:
        try:
            from google.oauth2 import service_account
            credentials = service_account.Credentials.from_service_account_info(
                sa_info,
                scopes=scopes,
            )
            return credentials, "service account"
        except Exception as exc:
            print(f"⚠️  Google service account 憑證失敗：{exc}")

    return None, ""


def build_google_drive_service():
    try:
        from googleapiclient.discovery import build
    except Exception as exc:
        print(f"⚠️  未安裝 Google Drive API 套件：{exc}")
        return None, ""

    credentials, auth_mode = _build_google_drive_credentials()
    if not credentials:
        return None, ""
    return build("drive", "v3", credentials=credentials, cache_discovery=False), auth_mode


def _drive_name_query(name: str) -> str:
    return name.replace("\\", "\\\\").replace("'", "\\'")


def get_drive_target_folder_id(service, cfg: dict, report_meta: dict, create: bool = False) -> str | None:
    drive_cfg = cfg.get("drive_report", {})
    folder_id = os.environ.get("GOOGLE_DRIVE_FOLDER_ID") or drive_cfg.get("folder_id")
    if not folder_id:
        return None

    for raw_name in drive_cfg.get("folder_path", []):
        name = str(raw_name).format(**report_meta)
        query = (
            f"'{folder_id}' in parents and "
            f"name = '{_drive_name_query(name)}' and "
            "mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        )
        existing = service.files().list(
            q=query,
            fields="files(id,name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute().get("files", [])
        if existing:
            folder_id = existing[0]["id"]
            continue
        if not create:
            return None
        folder = service.files().create(
            body={
                "name": name,
                "parents": [folder_id],
                "mimeType": "application/vnd.google-apps.folder",
            },
            fields="id,name",
            supportsAllDrives=True,
        ).execute()
        folder_id = folder["id"]
    return folder_id


def drive_file_exists(file_name: str, cfg: dict) -> bool:
    drive_cfg = cfg.get("drive_report", {})
    if not drive_cfg.get("enabled", False):
        return False

    service, _auth_mode = build_google_drive_service()
    if not service:
        print("⚠️  無法檢查 Google Drive 既有檔案，繼續執行避免漏寄")
        return False
    report_meta = get_report_meta(datetime.strptime(file_name[:8], "%Y%m%d").replace(tzinfo=TAIPEI_TZ))
    folder_id = get_drive_target_folder_id(service, cfg, report_meta, create=False)
    if not folder_id:
        return False

    try:
        query = (
            f"'{folder_id}' in parents and "
            f"name = '{_drive_name_query(file_name)}' and "
            "trashed = false"
        )
        existing = service.files().list(
            q=query,
            fields="files(id,name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute().get("files", [])
        if existing:
            print(f"Google Drive 已有 {file_name}，視為本週已完成，跳過備援重複寄送")
            return True
    except Exception as exc:
        print(f"⚠️  檢查 Google Drive 既有檔案失敗，繼續執行避免漏寄：{exc}")
    return False

def upload_report_image_to_drive(image_path: Path, today: str, cfg: dict) -> str | None:
    drive_cfg = cfg.get("drive_report", {})
    if not drive_cfg.get("enabled", False):
        return None

    try:
        from googleapiclient.http import MediaFileUpload
    except Exception as exc:
        print(f"⚠️  未安裝 Google Drive API 套件，跳過上傳：{exc}")
        return None

    service, auth_mode = build_google_drive_service()
    if not service:
        print("⚠️  未設定 Google OAuth 或 service account 憑證，已保留本機圖片但跳過上傳")
        return None
    report_meta = get_report_meta(datetime.strptime(today, "%Y-%m-%d").replace(tzinfo=TAIPEI_TZ))
    folder_id = get_drive_target_folder_id(service, cfg, report_meta, create=True)
    if not folder_id:
        print("⚠️  未設定 Google Drive folder_id，跳過上傳圖片")
        return None

    try:
        print(f"使用 Google Drive {auth_mode} 憑證上傳圖片")
        file_name = image_path.name
        media = MediaFileUpload(str(image_path), mimetype="image/png", resumable=False)
        query = (
            f"'{folder_id}' in parents and "
            f"name = '{_drive_name_query(file_name)}' and "
            "trashed = false"
        )
        existing = service.files().list(
            q=query,
            fields="files(id,name,webViewLink)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute().get("files", [])

        if existing:
            uploaded = service.files().update(
                fileId=existing[0]["id"],
                media_body=media,
                fields="id,name,webViewLink",
                supportsAllDrives=True,
            ).execute()
        else:
            uploaded = service.files().create(
                body={"name": file_name, "parents": [folder_id]},
                media_body=media,
                fields="id,name,webViewLink",
                supportsAllDrives=True,
            ).execute()

        return uploaded.get("webViewLink")
    except Exception as exc:
        print(f"⚠️  上傳 Google Drive 失敗：{exc}")
        return None


# ── 發送 Email ───────────────────────────────────────────────
def send_email(cfg: dict, html: str, today: str) -> bool:
    gmail_pass = os.environ.get("GMAIL_PASSWORD", "")
    if not gmail_pass:
        print("⚠️  未設定 GMAIL_PASSWORD（GitHub Secret），跳過發信")
        return False
    ec  = cfg["email"]
    meta = get_report_meta(datetime.strptime(today, "%Y-%m-%d").replace(tzinfo=TAIPEI_TZ))
    msg = MIMEMultipart("alternative")
    msg["Subject"] = ec["subject"].format(date=today, week=meta["week"], week_key=meta["week_key"])
    msg["From"]    = ec["from"]
    msg["To"]      = ec["to"]
    msg.attach(MIMEText(html, "html", "utf-8"))
    s = smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30)
    try:
        s.login(ec["from"], gmail_pass)
        s.sendmail(ec["from"], ec["to"], msg.as_string())
        s.quit()
    except Exception:
        s.close()
        raise
    return True


# ── 主流程 ───────────────────────────────────────────────────
def main():
    cfg   = load_config()
    now_tw = datetime.now(TAIPEI_TZ)
    report_meta = get_report_meta(now_tw)
    today = report_meta["date"]
    print(f"[{now_tw.strftime('%Y-%m-%d %H:%M')}] 開始每週趨勢分析，共 {len(cfg['watchlist'])} 檔")
    if drive_file_exists(f"{report_meta['date_key']}_week{report_meta['week']}_01.png", cfg):
        return

    macro = fetch_market_context()
    if macro.get("fx"):
        print(f"  總體環境：美元/台幣 {macro['fx']['value']:.3f}", end="")
    if macro.get("rates"):
        print(f"｜美10年債 {macro['rates']['value']:.2f}%", end="")
    if macro.get("fx") or macro.get("rates"):
        print()
    elif macro.get("errors"):
        print(f"  總體環境資料暫不可用：{'；'.join(macro['errors'])}")

    news_items = fetch_auto_news(cfg)
    print(f"  自動新聞掃描：取得 {len(news_items)} 則高關聯新聞")

    market_inst_value_week = fetch_market_institutional_value_week(now_tw)

    results = []
    for stock in cfg["watchlist"]:
        ticker = stock["ticker"]
        name   = stock["name"]
        note   = stock.get("note", "")
        print(f"  {name} ({ticker}) ...", end=" ")
        try:
            scfg = get_stock_cfg(stock, cfg)
            df   = fetch_data(ticker, cfg["lookback_days"])
            data_date = df.index[-1].strftime("%Y-%m-%d")
            data_dt = datetime.strptime(data_date, "%Y-%m-%d").replace(tzinfo=TAIPEI_TZ)
            report_meta = get_report_meta(data_dt)
            today = report_meta["date"]
            df   = calc_indicators(df, scfg)
            inst = fetch_institutional(ticker) if scfg.get("use_institutional", True) else None
            inst_week = fetch_weekly_institutional(ticker, data_dt) if scfg.get("use_institutional", True) else None
            r    = evaluate_weighted(df, scfg, inst, macro, inst_week)
            if ticker == "^TWII" and market_inst_value_week.get("success"):
                r["weekly"]["institutional_value"] = market_inst_value_week.get("total")
                r["weekly"]["institutional_value_text"] = format_twd_billion_short(market_inst_value_week.get("total"))
                r["weekly"]["institutional_daily_values"] = [x.get("total", 0) for x in market_inst_value_week.get("daily", [])]
                r["weekly"]["institutional_week_value"] = market_inst_value_week
            r["stock_note"] = note
            r["data_date"] = data_date
            results.append((name, ticker, r))
            print(
                f"{r['emoji']} {r['summary']} | "
                f"資料日={data_date} | "
                f"週數={report_meta['week']} | "
                f"有效買{r['effective_buy']:.0f}/賣{r['effective_sell']:.0f} "
                f"(原始買{r['buy_score']:.0f}/賣{r['sell_score']:.0f}) | "
                f"BIAS60={r['b60']['bias60']:.1f}%"
            )
        except Exception as e:
            print(f"❌ {e}")

    if not results:
        print("所有分析失敗，中止")
        return

    if drive_file_exists(f"{report_meta['date_key']}_week{report_meta['week']}_01.png", cfg):
        return

    html = build_email_html(results, today, cfg, macro, news_items)
    preview_path = save_email_preview(html)
    print(f"\n已產生 Email 預覽：{preview_path}")

    print(f"\n發送 Email 至 {cfg['email']['to']} ...")
    try:
        if send_email(cfg, html, today):
            print("✅ Email 發送成功")
    except Exception as e:
        print(f"❌ Email 失敗：{e}")

    social_pages = save_social_report_pages(
        build_social_report_pages(results, today, cfg, macro, news_items), today
    )
    for idx, social_page in enumerate(social_pages, start=1):
        image_name = f"{report_meta['date_key']}_week{report_meta['week']}_{idx:02d}.png"
        image_path = render_report_image(
            social_page, today, cfg, output_name=image_name, full_page=False, height=1920
        )
        if image_path:
            print(f"已產生社群分享圖片：{image_path}")
            drive_link = upload_report_image_to_drive(image_path, today, cfg)
            if drive_link:
                print(f"已上傳社群分享圖片至 Google Drive：{drive_link}")



if __name__ == "__main__":
    main()

